"""
Operational Run Audit Framework & Persistence (Module 5).

Persists Lakeflow Job execution summaries to the delta/operations/job_run_audit Delta table.
Grain: Exactly ONE row per Lakeflow Job execution.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.orchestration.models import JobRunAudit

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

OPERATIONAL_AUDIT_SCHEMA = StructType([
    StructField("orchestration_run_id", StringType(), False),
    StructField("databricks_job_id", StringType(), False),
    StructField("databricks_job_run_id", StringType(), False),
    StructField("environment", StringType(), False),
    StructField("ingestion_date", StringType(), False),
    StructField("adf_run_id", StringType(), False),
    StructField("started_at", TimestampType(), False),
    StructField("completed_at", TimestampType(), False),
    StructField("final_status", StringType(), False),
    StructField("duration_seconds", DoubleType(), False),
    StructField("landing_ready", BooleanType(), True),
    StructField("discovered_dataset_count", IntegerType(), True),
    StructField("bronze_rows_ingested", IntegerType(), True),
    StructField("silver_valid_rows", IntegerType(), True),
    StructField("silver_quarantine_rows", IntegerType(), True),
    StructField("gold_tables_generated", IntegerType(), True),
    StructField("fact_sales_rows", IntegerType(), True),
    StructField("quality_status", StringType(), True),
    StructField("quarantine_rate", DoubleType(), True),
    StructField("quarantine_alert_triggered", BooleanType(), True),
    StructField("failure_task", StringType(), True),
    StructField("failure_classification", StringType(), True),
    StructField("error_message", StringType(), True),
])


def persist_job_run_audit(
    spark: SparkSession,
    audit: JobRunAudit,
    audit_path: Path | str,
) -> None:
    """
    Append an operational job run audit record to the Delta audit table.

    Args:
        spark: Active SparkSession.
        audit: JobRunAudit instance.
        audit_path: Path to delta/operations/job_run_audit.
    """
    path_str = str(audit_path)
    record = [(
        audit.orchestration_run_id,
        audit.databricks_job_id,
        audit.databricks_job_run_id,
        audit.environment,
        audit.ingestion_date,
        audit.adf_run_id,
        audit.started_at,
        audit.completed_at,
        audit.final_status,
        float(audit.duration_seconds),
        audit.landing_ready,
        audit.discovered_dataset_count,
        audit.bronze_rows_ingested,
        audit.silver_valid_rows,
        audit.silver_quarantine_rows,
        audit.gold_tables_generated,
        audit.fact_sales_rows,
        audit.quality_status,
        float(audit.quarantine_rate) if audit.quarantine_rate is not None else None,
        audit.quarantine_alert_triggered,
        audit.failure_task,
        audit.failure_classification,
        audit.error_message,
    )]

    df = spark.createDataFrame(record, schema=OPERATIONAL_AUDIT_SCHEMA)
    df.write.format("delta").mode("append").save(path_str)
    logger.info("Persisted operational audit record for run '%s' to %s", audit.orchestration_run_id, path_str)


def format_run_summary(audit: JobRunAudit) -> str:
    """Format structured ASCII run summary for console and logs."""
    status_icon = "🟢" if audit.final_status == "SUCCESS" else "🔴"
    lines = [
        "=" * 80,
        f"{status_icon} LAKEFLOW JOBS RUN SUMMARY: {audit.final_status}",
        "=" * 80,
        f"  Orchestration Run ID   : {audit.orchestration_run_id}",
        f"  Databricks Job Run ID  : {audit.databricks_job_run_id}",
        f"  Environment / Date     : {audit.environment.upper()} / {audit.ingestion_date}",
        f"  ADF Batch Run ID       : {audit.adf_run_id}",
        f"  Started / Completed    : {audit.started_at.isoformat()} ➔ {audit.completed_at.isoformat()}",
        f"  Duration (Seconds)     : {audit.duration_seconds:.2f}s",
        "-" * 80,
        "  METRICS & THROUGHPUT:",
        f"    Landing Batch Ready  : {audit.landing_ready} ({audit.discovered_dataset_count or 0} datasets)",
        f"    Bronze Ingested Rows : {audit.bronze_rows_ingested or 0:,}",
        f"    Silver Valid Rows    : {audit.silver_valid_rows or 0:,}",
        f"    Silver Quarantine    : {audit.silver_quarantine_rows or 0:,} (Rate: {(audit.quarantine_rate or 0.0)*100:.2f}%)",
        f"    Gold Tables Generated: {audit.gold_tables_generated or 0}",
        f"    Warehouse Fact Rows  : {audit.fact_sales_rows or 0:,}",
        f"    Quality Gate Status  : {audit.quality_status or 'N/A'}",
    ]

    if audit.quarantine_alert_triggered:
        lines.append("    ⚠️  QUARANTINE ALERT   : Triggered (Quarantine rate exceeded threshold)")

    if audit.final_status == "FAILED":
        lines.extend([
            "-" * 80,
            "  FAILURE DETAILS:",
            f"    Failed Task          : {audit.failure_task}",
            f"    Classification       : {audit.failure_classification}",
            f"    Error Message        : {audit.error_message}",
        ])

    lines.append("=" * 80)
    return "\n".join(lines)
