"""
Publish Run Summary & Operational Audit Task (Module 5 Lakeflow Jobs).

Executes with run_if: ALL_DONE semantics. Gathers all task metrics and failure details,
constructs a unified JobRunAudit record, persists it to delta/operations/job_run_audit,
and registers it in Unity Catalog operations schema.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from src.medallion.catalog import register_operations_tables
from src.orchestration.audit import format_run_summary, persist_job_run_audit
from src.orchestration.models import JobRunAudit, RunContext, TaskResult, TaskValueStore

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

FAILED_RESULT_STATES = {"failed", "timedout", "canceled", "evicted", "FAILED", "TIMEDOUT", "CANCELED", "EVICTED"}
UPSTREAM_RESULT_STATES = {"upstream_failed", "upstream_canceled", "excluded", "UPSTREAM_FAILED", "EXCLUDED"}


def resolve_failed_task_from_states(
    task_order: list[str],
    task_states: dict[str, str],
    task_errors: dict[str, str],
    task_values: TaskValueStore,
) -> tuple[str | None, str | None, str | None]:
    """
    Determine the earliest root failed task, its failure classification, and error message
    from Lakeflow task result states and published task values.

    Args:
        task_order: List of task keys in DAG execution order.
        task_states: Mapping of task key to Databricks result_state.
        task_errors: Mapping of task key to Databricks error_code.
        task_values: TaskValueStore holding task-published values.

    Returns:
        tuple: (failure_task, failure_classification, error_message) or (None, None, None).
    """
    for t_key in task_order:
        state = (task_states.get(t_key) or "").strip().lower()
        if state in FAILED_RESULT_STATES:
            err_code = task_errors.get(t_key)
            classification = task_values.get(t_key, "failure_classification")
            msg = task_values.get(t_key, "failure_message")

            if not classification:
                if state == "timedout":
                    classification = "TRANSIENT"
                elif any(k in t_key for k in ("quality", "silver", "warehouse")):
                    classification = "DATA_QUALITY"
                elif "landing" in t_key:
                    classification = "CONFIGURATION"
                else:
                    classification = "UNKNOWN"

            if not msg:
                if err_code:
                    msg = f"Databricks task error code: {err_code}"
                else:
                    msg = f"Task '{t_key}' terminated with Databricks result state '{state}'"

            return t_key, classification, msg

    return None, None, None


def execute_publish_run_summary_task(
    spark: SparkSession,
    context: RunContext,
    task_values: TaskValueStore,
    task_results: dict[str, TaskResult] | None = None,
    overall_status: str = "SUCCESS",
    start_time: datetime | None = None,
    failure_task: str | None = None,
    failure_classification: str | None = None,
    error_message: str | None = None,
) -> JobRunAudit:
    """
    Compile and persist operational job run audit record.

    Args:
        spark: Active SparkSession.
        context: RunContext for the job.
        task_values: TaskValueStore populated by upstream tasks.
        task_results: Optional map of task names to TaskResult instances.
        overall_status: SUCCESS or FAILED.
        start_time: Job start timestamp.
        failure_task: Name of the failed task (if any).
        failure_classification: Classification enum string (if failed).
        error_message: Detailed error string (if failed).

    Returns:
        JobRunAudit: The created and persisted operational audit record.
    """
    completed_time = datetime.now(timezone.utc)
    job_started = start_time or completed_time
    duration_secs = max(0.0, (completed_time - job_started).total_seconds())

    audit = JobRunAudit(
        orchestration_run_id=context.orchestration_run_id,
        databricks_job_id=context.databricks_job_id,
        databricks_job_run_id=context.databricks_job_run_id,
        environment=context.environment,
        ingestion_date=context.ingestion_date,
        adf_run_id=context.adf_run_id,
        started_at=job_started,
        completed_at=completed_time,
        final_status=overall_status,
        duration_seconds=duration_secs,
        landing_ready=task_values.get("validate_landing_batch", "landing_ready"),
        discovered_dataset_count=task_values.get("validate_landing_batch", "discovered_dataset_count"),
        bronze_rows_ingested=task_values.get("bronze_ingestion", "bronze_rows_ingested"),
        silver_valid_rows=task_values.get("silver_transformation", "silver_valid_rows"),
        silver_quarantine_rows=task_values.get("silver_transformation", "silver_quarantine_rows"),
        gold_tables_generated=task_values.get("gold_analytics", "gold_tables_generated"),
        fact_sales_rows=task_values.get("dimensional_warehouse", "fact_sales_rows"),
        quality_status=task_values.get("final_quality_gate", "overall_quality_status") or ("FAILED" if overall_status == "FAILED" else "PASSED"),
        quarantine_rate=task_values.get("silver_transformation", "quarantine_rate"),
        quarantine_alert_triggered=task_values.get("silver_transformation", "quarantine_alert_triggered"),
        failure_task=failure_task,
        failure_classification=failure_classification,
        error_message=error_message,
    )

    # 1. Persist to delta/operations/job_run_audit
    delta_root = str(context.delta_root).rstrip("/")
    audit_path = f"{delta_root}/operations/job_run_audit"
    persist_job_run_audit(spark, audit, audit_path)

    # 2. Register table in Unity Catalog operations schema
    try:
        register_operations_tables(spark, context.catalog_name, delta_root)
    except Exception as e:
        logger.warning("Could not register operations table in Unity Catalog: %s", e)

    # 3. Print and log structured ASCII summary
    summary_text = format_run_summary(audit)
    print("\n" + summary_text)
    logger.info("Published operational run summary for run: %s [Status: %s]", context.orchestration_run_id, overall_status)

    return audit
