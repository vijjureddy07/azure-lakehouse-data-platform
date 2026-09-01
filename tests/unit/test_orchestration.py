"""
Unit Tests for Module 5: Lakeflow Jobs Orchestration + Reliability + Operational Monitoring.

Validates:
1. Lakeflow Jobs YAML specification parsing, task graph, parameters, timeouts, retries.
2. Cycle detection in DAG dependencies.
3. Strict enforcement of modern dynamic variable syntax (rejection of deprecated syntax).
4. Secret scanning across job artifacts (no credentials, personal emails, tokens).
5. Thread-safe TaskValueStore operations.
6. Failure classification taxonomy (Transient vs Data Quality vs Configuration).
7. Intelligent retry policies (transient retries, data quality non-retryable abort).
8. Delta operational audit table schema and persistence (successful and failed runs).
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.medallion.silver import ReconciliationError
from src.modeling.quality import WarehouseQualityGateError
from src.orchestration.audit import (
    format_run_summary,
    persist_job_run_audit,
)
from src.orchestration.models import (
    FailureClassification,
    JobRunAudit,
    TaskValueStore,
)
from src.orchestration.reliability import (
    RetryPolicy,
    classify_failure,
    execute_with_retry,
)
from src.orchestration.validation import (
    LakeflowJobValidationError,
    validate_lakeflow_job_yaml,
)

YAML_JOB_PATH = Path(__file__).resolve().parent.parent.parent / "databricks" / "jobs" / "retail_lakehouse_job.yml"


def test_lakeflow_job_yaml_structure_and_parameters():
    """Verify that the Lakeflow Jobs YAML definition is structurally valid and complete."""
    parsed = validate_lakeflow_job_yaml(YAML_JOB_PATH)
    assert parsed is not None
    assert parsed["job_name"] == "Retail Lakehouse Batch Pipeline"
    assert parsed["task_count"] == 7

    expected_tasks = {
        "validate_landing_batch",
        "bronze_ingestion",
        "silver_transformation",
        "gold_analytics",
        "dimensional_warehouse",
        "final_quality_gate",
        "publish_run_summary",
    }
    assert set(parsed["task_keys"]) == expected_tasks

    expected_params = {
        "environment",
        "ingestion_date",
        "adf_run_id",
        "storage_account_name",
        "container_name",
        "catalog_name",
    }
    assert expected_params.issubset(parsed["parameters"])


def test_lakeflow_job_yaml_dependencies_and_run_if():
    """Verify task dependencies and run_if properties."""
    parsed = validate_lakeflow_job_yaml(YAML_JOB_PATH)
    tasks_by_key = {t["task_key"]: t for t in parsed["tasks"]}

    # bronze depends on validate_landing
    assert tasks_by_key["bronze_ingestion"]["depends_on"][0]["task_key"] == "validate_landing_batch"
    # silver depends on bronze
    assert tasks_by_key["silver_transformation"]["depends_on"][0]["task_key"] == "bronze_ingestion"
    # gold and warehouse both depend on silver
    assert tasks_by_key["gold_analytics"]["depends_on"][0]["task_key"] == "silver_transformation"
    assert tasks_by_key["dimensional_warehouse"]["depends_on"][0]["task_key"] == "silver_transformation"
    # final quality gate depends on both gold and warehouse
    fg_deps = {d["task_key"] for d in tasks_by_key["final_quality_gate"]["depends_on"]}
    assert fg_deps == {"gold_analytics", "dimensional_warehouse"}
    # publish summary has run_if: ALL_DONE
    assert tasks_by_key["publish_run_summary"].get("run_if") == "ALL_DONE"


def test_lakeflow_job_yaml_rejects_deprecated_syntax(tmp_path):
    """Verify that obsolete dynamic variable tokens are rejected."""
    bad_yaml = tmp_path / "bad_job.yml"
    bad_yaml.write_text(
        """
resources:
  jobs:
    bad_job:
      name: "Bad Job"
      parameters:
        - name: environment
          default: "dev"
        - name: ingestion_date
          default: "{{start_date}}" # DEPRECATED!
        - name: adf_run_id
          default: "run-1"
        - name: storage_account_name
          default: "acc"
        - name: container_name
          default: "cont"
        - name: catalog_name
          default: "cat"
      tasks:
        - task_key: t1
          python_wheel_task:
            package_name: "pkg"
            entry_point: "ep"
"""
    )
    with pytest.raises(LakeflowJobValidationError) as exc:
        validate_lakeflow_job_yaml(bad_yaml)
    assert "Deprecated Syntax" in str(exc.value)


def test_lakeflow_job_yaml_cycle_detection(tmp_path):
    """Verify that circular dependencies in task graph are detected and rejected."""
    cycle_yaml = tmp_path / "cycle_job.yml"
    cycle_yaml.write_text(
        """
resources:
  jobs:
    cycle_job:
      name: "Cycle Job"
      parameters:
        - name: environment
          default: "dev"
        - name: ingestion_date
          default: "2026-08-31"
        - name: adf_run_id
          default: "run-1"
        - name: storage_account_name
          default: "acc"
        - name: container_name
          default: "cont"
        - name: catalog_name
          default: "cat"
      tasks:
        - task_key: task_a
          depends_on:
            - task_key: task_b
          python_wheel_task:
            package_name: "p"
            entry_point: "e"
        - task_key: task_b
          depends_on:
            - task_key: task_a
          python_wheel_task:
            package_name: "p"
            entry_point: "e"
"""
    )
    with pytest.raises(LakeflowJobValidationError) as exc:
        validate_lakeflow_job_yaml(cycle_yaml)
    assert "Circular dependency detected" in str(exc.value)


def test_lakeflow_job_yaml_secret_scanning():
    """Verify that no real credentials, tokens, or personal emails are committed in job definition."""
    parsed = validate_lakeflow_job_yaml(YAML_JOB_PATH)
    assert parsed is not None  # Successfully validated with 0 secrets found


def test_task_value_store_operations():
    """Test thread-safe cross-task communication via TaskValueStore."""
    store = TaskValueStore()
    store.set("validate_landing_batch", "batch_path", "/mnt/landing")
    store.set("validate_landing_batch", "discovered_dataset_count", 8)
    store.set("silver_transformation", "silver_quarantine_rows", 12)

    assert store.get("validate_landing_batch", "batch_path") == "/mnt/landing"
    assert store.get("validate_landing_batch", "discovered_dataset_count") == 8
    assert store.get("silver_transformation", "silver_quarantine_rows") == 12
    assert store.get("non_existent_task", "key", default=0) == 0


def test_failure_classification_mapping():
    """Test exception taxonomy mapping into operational categories."""
    assert classify_failure(WarehouseQualityGateError("Quality gate failed")) == FailureClassification.DATA_QUALITY
    assert classify_failure(ReconciliationError("Recon mismatch")) == FailureClassification.DATA_QUALITY
    assert classify_failure(FileNotFoundError("Missing blob")) == FailureClassification.TRANSIENT
    assert classify_failure(TimeoutError("Storage timeout")) == FailureClassification.TRANSIENT
    assert classify_failure(KeyError("missing_param")) == FailureClassification.CONFIGURATION
    assert classify_failure(RuntimeError("Unknown crash")) == FailureClassification.UNKNOWN


def test_retry_policy_execution_transient_vs_deterministic():
    """Test retry behavior: transient retries and succeeds; deterministic quality fails immediately."""
    # 1. Transient failure: succeeds on 2nd attempt
    attempts = 0

    def transient_func():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise IOError("Transient network glitch")
        return "SUCCESS"

    policy = RetryPolicy(max_retries=2, backoff_seconds=0.01)
    res, retries_used, _ = execute_with_retry(transient_func, "transient_task", policy)
    assert res == "SUCCESS"
    assert retries_used == 1

    # 2. Data Quality failure: must NOT retry, fails on 1st attempt
    dq_attempts = 0

    def dq_func():
        nonlocal dq_attempts
        dq_attempts += 1
        raise WarehouseQualityGateError("Broken arithmetic gate")

    with pytest.raises(WarehouseQualityGateError):
        execute_with_retry(dq_func, "dq_task", policy)
    # MUST be exactly 1 attempt (0 retries) because DATA_QUALITY is non-retryable
    assert dq_attempts == 1


def test_operational_audit_persistence_and_summary(spark, tmp_path):
    """Test persisting JobRunAudit to Delta table and generating formatted summary."""
    audit_path = tmp_path / "job_run_audit_test"
    now = datetime.now(timezone.utc)

    audit = JobRunAudit(
        orchestration_run_id="orch-001",
        databricks_job_id="retail_lakehouse_lakeflow_job",
        databricks_job_run_id="dbr-run-001",
        environment="dev",
        ingestion_date="2026-08-31",
        adf_run_id="adf-001",
        started_at=now,
        completed_at=now,
        final_status="SUCCESS",
        duration_seconds=42.5,
        landing_ready=True,
        discovered_dataset_count=8,
        bronze_rows_ingested=1250,
        silver_valid_rows=1200,
        silver_quarantine_rows=50,
        gold_tables_generated=6,
        fact_sales_rows=1150,
        quality_status="PASSED",
        quarantine_rate=0.04,
        quarantine_alert_triggered=False,
    )

    # 1. Persist to Delta
    persist_job_run_audit(spark, audit, audit_path)

    # 2. Verify Delta table
    audit_df = spark.read.format("delta").load(str(audit_path))
    assert audit_df.count() == 1
    row = audit_df.collect()[0]
    assert row["orchestration_run_id"] == "orch-001"
    assert row["final_status"] == "SUCCESS"
    assert row["silver_valid_rows"] == 1200
    assert row["quarantine_alert_triggered"] is False

    # 3. Verify formatted ASCII summary
    summary_text = format_run_summary(audit)
    assert "LAKEFLOW JOBS RUN SUMMARY: SUCCESS" in summary_text
    assert "orch-001" in summary_text


def test_failed_job_run_audit_with_nullable_downstream_metrics(spark, tmp_path):
    """Verify that an early failure produces an audit record with populated error fields and null downstream metrics."""
    audit_path = tmp_path / "job_run_audit_failed_test"
    now = datetime.now(timezone.utc)

    failed_audit = JobRunAudit(
        orchestration_run_id="orch-failed-002",
        databricks_job_id="retail_lakehouse_lakeflow_job",
        databricks_job_run_id="dbr-run-002",
        environment="dev",
        ingestion_date="2026-08-31",
        adf_run_id="adf-002",
        started_at=now,
        completed_at=now,
        final_status="FAILED",
        duration_seconds=5.2,
        landing_ready=True,
        discovered_dataset_count=8,
        bronze_rows_ingested=100,
        silver_valid_rows=None,       # Null because failed at Silver
        silver_quarantine_rows=None,
        gold_tables_generated=None,
        fact_sales_rows=None,
        quality_status="FAILED",
        failure_task="silver_transformation",
        failure_classification="DATA_QUALITY",
        error_message="SilverReconciliationError: Discrepancy detected in payments table",
    )

    persist_job_run_audit(spark, failed_audit, audit_path)

    audit_df = spark.read.format("delta").load(str(audit_path))
    assert audit_df.count() == 1
    row = audit_df.collect()[0]
    assert row["final_status"] == "FAILED"
    assert row["failure_task"] == "silver_transformation"
    assert row["failure_classification"] == "DATA_QUALITY"
    assert row["silver_valid_rows"] is None
    assert row["fact_sales_rows"] is None
