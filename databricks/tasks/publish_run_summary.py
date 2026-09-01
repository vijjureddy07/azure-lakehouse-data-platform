# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Task: Publish Operational Run Summary
# MAGIC **Task Key:** `publish_run_summary`
# MAGIC Executed under `run_if: ALL_DONE` to compile and persist the JobRunAudit record.

# COMMAND ----------

from datetime import datetime, timezone

from src.orchestration.models import RunContext, TaskValueStore
from src.orchestration.tasks.publish_run_summary import (
    PRIMARY_DAG_TASK_ORDER,
    execute_publish_run_summary_task,
    resolve_failed_task_from_task_values,
)
from src.utils.spark import get_spark_session

# COMMAND ----------

# Lakeflow widgets parameters (Injected via Job-Level Parameter Pushdown)
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

# Fetch task values set by upstream primary tasks in the active Lakeflow Job run
for t_key in PRIMARY_DAG_TASK_ORDER:
    try:
        for k in (
            "terminal_state",
            "failure_classification",
            "failure_message",
            "landing_ready",
            "discovered_dataset_count",
            "bronze_rows_ingested",
            "silver_valid_rows",
            "silver_quarantine_rows",
            "quarantine_rate",
            "quarantine_alert_triggered",
            "gold_tables_generated",
            "fact_sales_rows",
            "overall_quality_status",
        ):
            val = dbutils.jobs.taskValues.get(taskKey=t_key, key=k, default=None)
            if val is not None:
                task_values.set(t_key, k, val)
    except Exception:
        pass

# Determine root failed task from published task values and terminal states
fail_task, fail_class, fail_msg = resolve_failed_task_from_task_values(
    primary_tasks=PRIMARY_DAG_TASK_ORDER,
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
