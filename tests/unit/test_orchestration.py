"""
Unit Tests for Module 5: Lakeflow Jobs Orchestration + Reliability + Operational Monitoring.

Validates:
1. Lakeflow Jobs YAML specification parsing, task graph, parameters, condition tasks, timeouts, retries.
2. Two-tier retry design: native max_retries: 0 for deterministic-quality tasks, in-process classified retries.
3. Cycle detection and outcome dependency validation in DAG.
4. Strict enforcement of modern dynamic variable syntax (rejection of deprecated syntax).
5. Secret scanning and legacy DBFS mount path elimination.
6. Thread-safe TaskValueStore operations.
7. Failure classification taxonomy (Transient vs Data Quality vs Configuration).
8. Intelligent retry policies (transient retries, data quality non-retryable abort).
9. Landing batch validation (8 required datasets, date/run filtering, LandingBatchIncompleteError).
10. String-safe storage URI composition (preserving abfss:// cloud URIs).
11. Exact batch Bronze ingestion and idempotency.
12. Operations Unity Catalog DDL generation.
13. Delta operational audit table schema, root failure resolution, and persistence.
14. Complete absence of local machine file:/// and /Users/ links in documentation.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.medallion.bronze import ingest_bronze_layer
from src.medallion.catalog import register_operations_tables
from src.medallion.silver import ReconciliationError
from src.modeling.quality import WarehouseQualityGateError
from src.orchestration.audit import (
    format_run_summary,
    persist_job_run_audit,
)
from src.orchestration.models import (
    FailureClassification,
    JobRunAudit,
    RunContext,
    TaskValueStore,
)
from src.orchestration.reliability import (
    RetryPolicy,
    classify_failure,
    execute_with_retry,
)
from src.orchestration.tasks.publish_run_summary import (
    resolve_failed_task_from_states,
)
from src.orchestration.tasks.validate_landing import (
    LandingBatchIncompleteError,
    execute_validate_landing_task,
)
from src.orchestration.utils import join_storage_uri
from src.orchestration.validation import (
    LakeflowJobValidationError,
    validate_lakeflow_job_yaml,
)

YAML_JOB_PATH = Path(__file__).resolve().parent.parent.parent / "databricks" / "jobs" / "retail_lakehouse_job.yml"
REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def test_lakeflow_job_yaml_structure_and_parameters():
    """Verify that the Lakeflow Jobs YAML definition is structurally valid and complete."""
    parsed = validate_lakeflow_job_yaml(YAML_JOB_PATH)
    assert parsed is not None
    assert parsed["job_name"] == "Retail Lakehouse Batch Pipeline"
    assert parsed["task_count"] == 9

    expected_tasks = {
        "validate_landing_batch",
        "bronze_ingestion",
        "silver_transformation",
        "check_quarantine_threshold",
        "quality_attention",
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


def test_deterministic_quality_tasks_have_native_max_retries_zero():
    """Verify that tasks susceptible to deterministic data-quality errors configure native max_retries: 0."""
    parsed = validate_lakeflow_job_yaml(YAML_JOB_PATH)
    tasks_by_key = {t["task_key"]: t for t in parsed["tasks"]}

    # Tasks with potential deterministic errors must have max_retries: 0 (or omit max_retries)
    deterministic_tasks = [
        "validate_landing_batch",
        "silver_transformation",
        "dimensional_warehouse",
        "final_quality_gate",
    ]
    for t_key in deterministic_tasks:
        max_retries = tasks_by_key[t_key].get("max_retries", 0)
        assert max_retries == 0, f"Task '{t_key}' must have max_retries: 0 to prevent blind Databricks retries on data errors."


def test_publish_run_summary_receives_job_start_and_task_states():
    """Verify that publish_run_summary is configured with run_if ALL_DONE and targets the summary notebook."""
    parsed = validate_lakeflow_job_yaml(YAML_JOB_PATH)
    tasks_by_key = {t["task_key"]: t for t in parsed["tasks"]}
    summary_task = tasks_by_key["publish_run_summary"]

    assert summary_task.get("run_if") == "ALL_DONE"
    assert summary_task.get("notebook_task", {}).get("notebook_path") == "databricks/tasks/publish_run_summary.py"


def test_lakeflow_job_yaml_dependencies_and_run_if():
    """Verify task dependencies, condition task, and run_if properties."""
    parsed = validate_lakeflow_job_yaml(YAML_JOB_PATH)
    tasks_by_key = {t["task_key"]: t for t in parsed["tasks"]}

    # bronze depends on validate_landing
    assert tasks_by_key["bronze_ingestion"]["depends_on"][0]["task_key"] == "validate_landing_batch"
    # silver depends on bronze
    assert tasks_by_key["silver_transformation"]["depends_on"][0]["task_key"] == "bronze_ingestion"
    # check_quarantine_threshold depends on silver
    assert tasks_by_key["check_quarantine_threshold"]["depends_on"][0]["task_key"] == "silver_transformation"
    # quality_attention depends on check_quarantine_threshold with outcome: 'true'
    qa_dep = tasks_by_key["quality_attention"]["depends_on"][0]
    assert qa_dep["task_key"] == "check_quarantine_threshold"
    assert qa_dep["outcome"] == "true"

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
          notebook_task:
            notebook_path: "databricks/tasks/t1.py"
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
          notebook_task:
            notebook_path: "databricks/tasks/a.py"
        - task_key: task_b
          depends_on:
            - task_key: task_a
          notebook_task:
            notebook_path: "databricks/tasks/b.py"
"""
    )
    with pytest.raises(LakeflowJobValidationError) as exc:
        validate_lakeflow_job_yaml(cycle_yaml)
    assert "Circular dependency detected" in str(exc.value)


def test_lakeflow_job_yaml_secret_scanning():
    """Verify that no real credentials, tokens, or personal emails are committed in job definition."""
    parsed = validate_lakeflow_job_yaml(YAML_JOB_PATH)
    assert parsed is not None  # Successfully validated with 0 secrets found


def test_no_legacy_dbfs_mount_paths_in_repository():
    """Verify that zero legacy /mnt/ DBFS mount paths exist in Databricks tasks/notebooks or src."""
    scan_dirs = [REPO_ROOT / "databricks", REPO_ROOT / "src"]
    offending_files = []

    for s_dir in scan_dirs:
        for p in s_dir.rglob("*"):
            if p.is_file() and p.suffix in (".py", ".sql", ".yml", ".yaml", ".json"):
                if p.name == "validation.py":
                    continue
                text = p.read_text(encoding="utf-8")
                if "/mnt/" in text:
                    offending_files.append(str(p.relative_to(REPO_ROOT)))

    assert offending_files == [], f"Found legacy '/mnt/' mount paths in: {offending_files}"


def test_no_local_machine_file_links_in_docs():
    """Verify that zero machine-specific file:/// or /Users/ links exist in documentation files."""
    scan_files = list((REPO_ROOT / "docs").glob("*.md")) + [REPO_ROOT / "README.md"]
    offending = []

    for f in scan_files:
        if f.is_file():
            text = f.read_text(encoding="utf-8")
            if "file:///Users" in text or "file:///" in text or "/Users/vijju" in text:
                offending.append(f.name)

    assert offending == [], f"Found machine-specific local links in docs: {offending}"


def test_join_storage_uri_preserves_abfss_scheme():
    """Verify string-safe storage URI composition without double-slash corruption."""
    cloud_uri = join_storage_uri("abfss://lakehouse@stdev.dfs.core.windows.net", "landing", "retail")
    assert cloud_uri == "abfss://lakehouse@stdev.dfs.core.windows.net/landing/retail"
    assert "abfss:/" in cloud_uri
    assert not cloud_uri.startswith("abfss:/lakehouse")  # Must be abfss://lakehouse

    cloud_with_slashes = join_storage_uri("abfss://lakehouse@stdev.dfs.core.windows.net/landing/", "/retail/customers/")
    assert cloud_with_slashes == "abfss://lakehouse@stdev.dfs.core.windows.net/landing/retail/customers"

    local_path = join_storage_uri("/tmp/lakehouse", "delta", "bronze")
    assert local_path == "/tmp/lakehouse/delta/bronze"


def test_task_value_store_operations():
    """Test thread-safe cross-task communication via TaskValueStore."""
    store = TaskValueStore()
    store.set("validate_landing_batch", "landing_root", "abfss://lakehouse@stdev/landing")
    store.set("validate_landing_batch", "discovered_dataset_count", 8)
    store.set("silver_transformation", "silver_quarantine_rows", 12)

    assert store.get("validate_landing_batch", "landing_root") == "abfss://lakehouse@stdev/landing"
    assert store.get("validate_landing_batch", "discovered_dataset_count") == 8
    assert store.get("silver_transformation", "silver_quarantine_rows") == 12
    assert store.get("non_existent_task", "key", default=0) == 0


def test_failure_classification_mapping():
    """Test exception taxonomy mapping into operational categories."""
    assert classify_failure(WarehouseQualityGateError("Quality gate failed")) == FailureClassification.DATA_QUALITY
    assert classify_failure(ReconciliationError("Recon mismatch")) == FailureClassification.DATA_QUALITY
    assert classify_failure(LandingBatchIncompleteError("Missing tables")) == FailureClassification.CONFIGURATION
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
    assert dq_attempts == 1


def test_resolve_failed_task_from_states_and_values():
    """Test determining root failed task from result states, error codes, and published task values."""
    task_order = ["validate_landing_batch", "bronze_ingestion", "silver_transformation", "dimensional_warehouse"]
    store = TaskValueStore()

    # Scenario 1: All SUCCESS
    states_ok = {k: "SUCCESS" for k in task_order}
    errors_none = {k: "" for k in task_order}
    f_task, f_class, f_msg = resolve_failed_task_from_states(task_order, states_ok, errors_none, store)
    assert f_task is None

    # Scenario A: silver_transformation failed, NO published classification, generic RunExecutionError
    # Expected: failure_classification = UNKNOWN (NOT DATA_QUALITY inferred from task name)
    store_a = TaskValueStore()
    states_a = {
        "validate_landing_batch": "SUCCESS",
        "bronze_ingestion": "SUCCESS",
        "silver_transformation": "FAILED",
        "dimensional_warehouse": "UPSTREAM_FAILED",
    }
    errors_a = {"silver_transformation": "RunExecutionError"}
    f_task_a, f_class_a, f_msg_a = resolve_failed_task_from_states(task_order, states_a, errors_a, store_a)
    assert f_task_a == "silver_transformation"
    assert f_class_a == "UNKNOWN"
    assert "RunExecutionError" in f_msg_a

    # Scenario B: dimensional_warehouse failed with published DATA_QUALITY classification
    store_b = TaskValueStore()
    store_b.set("dimensional_warehouse", "failure_classification", "DATA_QUALITY")
    store_b.set("dimensional_warehouse", "failure_message", "WarehouseQualityGateError: primary key broken")
    states_b = {
        "validate_landing_batch": "SUCCESS",
        "bronze_ingestion": "SUCCESS",
        "silver_transformation": "SUCCESS",
        "dimensional_warehouse": "FAILED",
    }
    f_task_b, f_class_b, f_msg_b = resolve_failed_task_from_states(task_order, states_b, errors_none, store_b)
    assert f_task_b == "dimensional_warehouse"
    assert f_class_b == "DATA_QUALITY"
    assert "WarehouseQualityGateError" in f_msg_b

    # Scenario C: task state = timedout with no project classification -> TRANSIENT
    store_c = TaskValueStore()
    states_c = {
        "validate_landing_batch": "TIMEDOUT",
        "bronze_ingestion": "UPSTREAM_FAILED",
        "silver_transformation": "UPSTREAM_FAILED",
        "dimensional_warehouse": "UPSTREAM_FAILED",
    }
    f_task_c, f_class_c, _ = resolve_failed_task_from_states(task_order, states_c, errors_none, store_c)
    assert f_task_c == "validate_landing_batch"
    assert f_class_c == "TRANSIENT"

    # Scenario D: earlier actual failed task remains root failure when downstream is UPSTREAM_FAILED
    store_d = TaskValueStore()
    store_d.set("bronze_ingestion", "failure_classification", "TRANSIENT")
    store_d.set("bronze_ingestion", "failure_message", "TimeoutError: Storage timeout")
    states_d = {
        "validate_landing_batch": "SUCCESS",
        "bronze_ingestion": "FAILED",
        "silver_transformation": "UPSTREAM_FAILED",
        "dimensional_warehouse": "UPSTREAM_FAILED",
    }
    f_task_d, f_class_d, f_msg_d = resolve_failed_task_from_states(task_order, states_d, errors_none, store_d)
    assert f_task_d == "bronze_ingestion"
    assert f_class_d == "TRANSIENT"
    assert "Storage timeout" in f_msg_d


def test_landing_batch_validation_complete_and_incomplete(spark, tmp_path):
    """Test landing validation with complete 8-dataset batch vs incomplete batch."""
    landing_root = tmp_path / "landing"
    date_str = "2026-09-01"
    run_id = "run-001"

    # 1. Create complete 8-dataset landing batch
    datasets = ["customers", "products", "stores", "employees", "orders", "order_items", "payments", "returns"]
    for ds in datasets:
        ds_dir = landing_root / "retail" / ds / f"ingestion_date={date_str}" / f"run_id={run_id}"
        ds_dir.mkdir(parents=True, exist_ok=True)
        ext = "json" if ds == "payments" else "csv"
        (ds_dir / f"{ds}.{ext}").write_text("id,val\n1,test\n" if ext == "csv" else '{"id": 1}\n')

    task_values = TaskValueStore()
    context = RunContext(
        ingestion_date=date_str,
        adf_run_id=run_id,
        landing_root=str(landing_root),
    )

    # Complete batch passes
    res = execute_validate_landing_task(spark, context, task_values)
    assert res["landing_ready"] is True
    assert res["discovered_dataset_count"] == 8
    assert res["missing_dataset_count"] == 0

    # 2. Incomplete batch (e.g. 7 datasets - missing returns)
    landing_incomplete = tmp_path / "landing_incomplete"
    for ds in datasets[:-1]:
        ds_dir = landing_incomplete / "retail" / ds / f"ingestion_date={date_str}" / f"run_id={run_id}"
        ds_dir.mkdir(parents=True, exist_ok=True)
        ext = "json" if ds == "payments" else "csv"
        (ds_dir / f"{ds}.{ext}").write_text("id,val\n1,test\n" if ext == "csv" else '{"id": 1}\n')

    ctx_incomplete = RunContext(
        ingestion_date=date_str,
        adf_run_id=run_id,
        landing_root=str(landing_incomplete),
    )
    with pytest.raises(LandingBatchIncompleteError) as exc_inc:
        execute_validate_landing_task(spark, ctx_incomplete, TaskValueStore())
    assert "Missing 1 required dataset(s)" in str(exc_inc.value)

    # 3. Wrong ingestion date fails
    ctx_wrong_date = RunContext(
        ingestion_date="2020-01-01",
        adf_run_id=run_id,
        landing_root=str(landing_root),
    )
    with pytest.raises(LandingBatchIncompleteError):
        execute_validate_landing_task(spark, ctx_wrong_date, TaskValueStore())

    # 4. Wrong ADF run ID fails
    ctx_wrong_run = RunContext(
        ingestion_date=date_str,
        adf_run_id="run-999-unknown",
        landing_root=str(landing_root),
    )
    with pytest.raises(LandingBatchIncompleteError):
        execute_validate_landing_task(spark, ctx_wrong_run, TaskValueStore())


def test_exact_bronze_batch_ingestion_and_idempotency(spark, tmp_path):
    """Verify that orchestrated Bronze ingests only the specified ADF batch and is rerun-idempotent."""
    landing_root = tmp_path / "landing"
    bronze_root = tmp_path / "delta" / "bronze"

    date_str = "2026-09-01"
    run_a = "run-A"
    run_b = "run-B"

    # Setup Run A (2 rows in customers)
    dir_a = landing_root / "retail" / "customers" / f"ingestion_date={date_str}" / f"run_id={run_a}"
    dir_a.mkdir(parents=True, exist_ok=True)
    (dir_a / "customers.csv").write_text("customer_id,first_name\n1,Alice\n2,Bob\n")

    # Setup Run B (2 rows in customers)
    dir_b = landing_root / "retail" / "customers" / f"ingestion_date={date_str}" / f"run_id={run_b}"
    dir_b.mkdir(parents=True, exist_ok=True)
    (dir_b / "customers.csv").write_text("customer_id,first_name\n3,Charlie\n4,David\n")

    # Ingest only Run A
    counts_a = ingest_bronze_layer(
        spark=spark,
        landing_root=landing_root,
        bronze_root=bronze_root,
        datasets=["customers"],
        ingestion_date=date_str,
        adf_run_id=run_a,
    )
    assert counts_a["customers"] == 2

    # Verify Bronze table contains only Run A
    cust_df = spark.read.format("delta").load(str(bronze_root / "customers"))
    assert cust_df.count() == 2
    assert cust_df.filter(cust_df._adf_run_id == run_a).count() == 2
    assert cust_df.filter(cust_df._adf_run_id == run_b).count() == 0

    # Rerun of Run A without new files should be idempotent (0 new rows ingested)
    counts_a_rerun = ingest_bronze_layer(
        spark=spark,
        landing_root=landing_root,
        bronze_root=bronze_root,
        datasets=["customers"],
        ingestion_date=date_str,
        adf_run_id=run_a,
    )
    assert counts_a_rerun["customers"] == 0


def test_operations_unity_catalog_registration(spark, tmp_path):
    """Test generating and executing Unity Catalog operations DDL."""
    delta_root = tmp_path / "delta"
    audit_table = delta_root / "operations" / "job_run_audit"
    audit_table.mkdir(parents=True, exist_ok=True)

    df = spark.createDataFrame([(1, "dev")], ["id", "env"])
    df.write.format("delta").mode("overwrite").save(str(audit_table))

    statements = register_operations_tables(
        spark=spark,
        catalog_name="retail_lakehouse",
        delta_root_uri=str(delta_root),
    )
    assert len(statements) == 3
    assert "CREATE CATALOG IF NOT EXISTS retail_lakehouse;" in statements[0]
    assert "CREATE SCHEMA IF NOT EXISTS retail_lakehouse.operations;" in statements[1]
    assert "CREATE TABLE IF NOT EXISTS retail_lakehouse.operations.job_run_audit" in statements[2]
    assert f"LOCATION '{str(delta_root)}/operations/job_run_audit'" in statements[2]


def test_operational_audit_persistence_and_summary(spark, tmp_path):
    """Test persisting JobRunAudit to Delta table and generating formatted summary."""
    audit_path = tmp_path / "job_run_audit_test"
    now = datetime(2026, 9, 1, 10, 0, 0, tzinfo=timezone.utc)

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

    persist_job_run_audit(spark, audit, audit_path)

    audit_df = spark.read.format("delta").load(str(audit_path))
    assert audit_df.count() == 1
    row = audit_df.collect()[0]
    assert row["orchestration_run_id"] == "orch-001"
    assert row["final_status"] == "SUCCESS"
    assert row["silver_valid_rows"] == 1200
    assert row["quarantine_alert_triggered"] is False

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
        silver_valid_rows=None,
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
