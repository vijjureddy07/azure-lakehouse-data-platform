# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Task: Publish Operational Run Summary
# MAGIC **Task Key:** `publish_run_summary`
# MAGIC Executed under `run_if: ALL_DONE` to compile and persist the JobRunAudit record.

# COMMAND ----------

from datetime import datetime, timezone

from src.orchestration.models import RunContext, TaskValueStore
from src.orchestration.tasks.publish_run_summary import execute_publish_run_summary_task
from src.utils.spark import get_spark_session

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("ingestion_date", "", "Ingestion Date")
dbutils.widgets.text("adf_run_id", "", "ADF Run ID")
dbutils.widgets.text("storage_account_name", "stlakehousedev", "Storage Account Name")
dbutils.widgets.text("container_name", "lakehouse", "Container Name")
dbutils.widgets.text("catalog_name", "retail_lakehouse", "Catalog Name")
dbutils.widgets.text("delta_root", "", "Delta Root URI")
dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("job_run_id", "", "Job Run ID")

env = dbutils.widgets.get("environment")
ingestion_date = dbutils.widgets.get("ingestion_date")
adf_run_id = dbutils.widgets.get("adf_run_id")
account = dbutils.widgets.get("storage_account_name")
container = dbutils.widgets.get("container_name")
catalog = dbutils.widgets.get("catalog_name")
delta_root_param = dbutils.widgets.get("delta_root")
job_id = dbutils.widgets.get("job_id")
job_run_id = dbutils.widgets.get("job_run_id")

storage_base = f"abfss://{container}@{account}.dfs.core.windows.net"
delta_root = delta_root_param if delta_root_param else f"{storage_base}/delta"

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
task_results = {}
start_time = datetime.now(timezone.utc)

# COMMAND ----------

# Fetch task values set by upstream tasks in the active Lakeflow Job run if available
try:
    landing_ready = dbutils.jobs.taskValues.get(taskKey="validate_landing_batch", key="landing_ready", default=None)
    discovered_count = dbutils.jobs.taskValues.get(taskKey="validate_landing_batch", key="discovered_dataset_count", default=None)
    bronze_rows = dbutils.jobs.taskValues.get(taskKey="bronze_ingestion", key="bronze_rows_ingested", default=None)
    silver_valid = dbutils.jobs.taskValues.get(taskKey="silver_transformation", key="silver_valid_rows", default=None)
    silver_quarantine = dbutils.jobs.taskValues.get(taskKey="silver_transformation", key="silver_quarantine_rows", default=None)
    quarantine_rate = dbutils.jobs.taskValues.get(taskKey="silver_transformation", key="quarantine_rate", default=None)
    quarantine_alert = dbutils.jobs.taskValues.get(taskKey="silver_transformation", key="quarantine_alert_triggered", default=None)
    fact_sales = dbutils.jobs.taskValues.get(taskKey="dimensional_warehouse", key="fact_sales_rows", default=None)
    quality_status = dbutils.jobs.taskValues.get(taskKey="final_quality_gate", key="overall_quality_status", default=None)

    task_values.set("validate_landing_batch", "landing_ready", landing_ready)
    task_values.set("validate_landing_batch", "discovered_dataset_count", discovered_count)
    task_values.set("bronze_ingestion", "bronze_rows_ingested", bronze_rows)
    task_values.set("silver_transformation", "silver_valid_rows", silver_valid)
    task_values.set("silver_transformation", "silver_quarantine_rows", silver_quarantine)
    task_values.set("silver_transformation", "quarantine_rate", quarantine_rate)
    task_values.set("silver_transformation", "quarantine_alert_triggered", quarantine_alert)
    task_values.set("dimensional_warehouse", "fact_sales_rows", fact_sales)
    task_values.set("final_quality_gate", "overall_quality_status", quality_status)
except Exception as e:
    print(f"Reading task values from dbutils context (fallback to local defaults): {e}")

overall_status = "SUCCESS" if task_values.get("final_quality_gate", "overall_quality_status") == "PASSED" else "FAILED"

audit = execute_publish_run_summary_task(
    spark=spark,
    context=context,
    task_values=task_values,
    task_results=task_results,
    overall_status=overall_status,
    start_time=start_time,
)

print(f"Operational Run Audit Recorded: ID={audit.orchestration_run_id}, Status={audit.final_status}")
