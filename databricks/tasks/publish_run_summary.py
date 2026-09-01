# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Task: Publish Operational Run Summary
# MAGIC **Task Key:** `publish_run_summary`
# MAGIC Executed under `run_if: ALL_DONE` to compile and persist the JobRunAudit record.

# COMMAND ----------

from datetime import datetime, timezone

from src.orchestration.models import RunContext, TaskValueStore
from src.orchestration.tasks.publish_run_summary import (
    execute_publish_run_summary_task,
    resolve_failed_task_from_states,
)
from src.utils.spark import get_spark_session

# COMMAND ----------

# Lakeflow widgets parameters
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("ingestion_date", "", "Ingestion Date")
dbutils.widgets.text("adf_run_id", "", "ADF Run ID")
dbutils.widgets.text("storage_account_name", "stlakehousedev", "Storage Account Name")
dbutils.widgets.text("container_name", "lakehouse", "Container Name")
dbutils.widgets.text("catalog_name", "retail_lakehouse", "Catalog Name")
dbutils.widgets.text("delta_root", "", "Delta Root URI")
dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("job_run_id", "", "Job Run ID")
dbutils.widgets.text("job_start_time", "", "Job Start Time (ISO)")

# Upstream task result states
dbutils.widgets.text("validate_landing_state", "", "Validate Landing State")
dbutils.widgets.text("validate_landing_error", "", "Validate Landing Error")
dbutils.widgets.text("bronze_state", "", "Bronze State")
dbutils.widgets.text("bronze_error", "", "Bronze Error")
dbutils.widgets.text("silver_state", "", "Silver State")
dbutils.widgets.text("silver_error", "", "Silver Error")
dbutils.widgets.text("gold_state", "", "Gold State")
dbutils.widgets.text("gold_error", "", "Gold Error")
dbutils.widgets.text("warehouse_state", "", "Warehouse State")
dbutils.widgets.text("warehouse_error", "", "Warehouse Error")
dbutils.widgets.text("final_quality_state", "", "Final Quality State")
dbutils.widgets.text("final_quality_error", "", "Final Quality Error")

env = dbutils.widgets.get("environment")
ingestion_date = dbutils.widgets.get("ingestion_date")
adf_run_id = dbutils.widgets.get("adf_run_id")
account = dbutils.widgets.get("storage_account_name")
container = dbutils.widgets.get("container_name")
catalog = dbutils.widgets.get("catalog_name")
delta_root_param = dbutils.widgets.get("delta_root")
job_id = dbutils.widgets.get("job_id")
job_run_id = dbutils.widgets.get("job_run_id")
job_start_str = dbutils.widgets.get("job_start_time")

storage_base = f"abfss://{container}@{account}.dfs.core.windows.net"
delta_root = delta_root_param if delta_root_param else f"{storage_base}/delta"

# Parse job_start_time
start_time = None
if job_start_str:
    try:
        # Handle ISO strings like 2026-09-01T15:30:00Z or +00:00
        clean_iso = job_start_str.replace("Z", "+00:00")
        start_time = datetime.fromisoformat(clean_iso)
    except Exception:
        start_time = None

if start_time is None:
    start_time = datetime.now(timezone.utc)

context = RunContext(
    databricks_job_id=job_id if job_id else "retail_lakehouse_lakeflow_job",
    databricks_job_run_id=job_run_id if job_run_id else "dbr-run-manual",
    environment=env,
    ingestion_date=ingestion_date,
    adf_run_id=adf_run_id,
    storage_account_name=account,
    container_name=container,
    catalog_name=catalog,
    delta_root=delta_root,
)

spark = get_spark_session()
task_values = TaskValueStore()

# COMMAND ----------

# Fetch task values set by upstream tasks in the active Lakeflow Job run if available
task_keys = [
    "validate_landing_batch",
    "bronze_ingestion",
    "silver_transformation",
    "gold_analytics",
    "dimensional_warehouse",
    "final_quality_gate",
]

for t_key in task_keys:
    try:
        for k in ("landing_ready", "discovered_dataset_count", "bronze_rows_ingested",
                  "silver_valid_rows", "silver_quarantine_rows", "quarantine_rate",
                  "quarantine_alert_triggered", "gold_tables_generated", "fact_sales_rows",
                  "overall_quality_status", "failure_classification", "failure_message"):
            val = dbutils.jobs.taskValues.get(taskKey=t_key, key=k, default=None)
            if val is not None:
                task_values.set(t_key, k, val)
    except Exception:
        pass

# Collect task states and error codes
task_states = {
    "validate_landing_batch": dbutils.widgets.get("validate_landing_state"),
    "bronze_ingestion": dbutils.widgets.get("bronze_state"),
    "silver_transformation": dbutils.widgets.get("silver_state"),
    "gold_analytics": dbutils.widgets.get("gold_state"),
    "dimensional_warehouse": dbutils.widgets.get("warehouse_state"),
    "final_quality_gate": dbutils.widgets.get("final_quality_state"),
}

task_errors = {
    "validate_landing_batch": dbutils.widgets.get("validate_landing_error"),
    "bronze_ingestion": dbutils.widgets.get("bronze_error"),
    "silver_transformation": dbutils.widgets.get("silver_error"),
    "gold_analytics": dbutils.widgets.get("gold_error"),
    "dimensional_warehouse": dbutils.widgets.get("warehouse_error"),
    "final_quality_gate": dbutils.widgets.get("final_quality_error"),
}

# Determine root failed task
fail_task, fail_class, fail_msg = resolve_failed_task_from_states(
    task_order=task_keys,
    task_states=task_states,
    task_errors=task_errors,
    task_values=task_values,
)

overall_status = "FAILED" if fail_task else "SUCCESS"

audit = execute_publish_run_summary_task(
    spark=spark,
    context=context,
    task_values=task_values,
    overall_status=overall_status,
    start_time=start_time,
    failure_task=fail_task,
    failure_classification=fail_class,
    error_message=fail_msg,
)

print(f"Operational Run Audit Recorded: ID={audit.orchestration_run_id}, Status={audit.final_status}, Duration={audit.duration_seconds:.2f}s")
