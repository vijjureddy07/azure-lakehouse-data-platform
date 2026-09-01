# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Task: Silver Conformance & Quality Quarantine
# MAGIC **Task Key:** `silver_transformation`
# MAGIC Conforms Bronze tables, routes defects to quarantine, and enforces mathematical reconciliation.

# COMMAND ----------

from src.orchestration.models import RunContext, TaskValueStore
from src.orchestration.tasks.run_silver import execute_silver_task
from src.utils.spark import get_spark_session

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("ingestion_date", "", "Ingestion Date")
dbutils.widgets.text("adf_run_id", "", "ADF Run ID")
dbutils.widgets.text("storage_account_name", "stlakehousedev", "Storage Account Name")
dbutils.widgets.text("container_name", "lakehouse", "Container Name")
dbutils.widgets.text("catalog_name", "retail_lakehouse", "Catalog Name")
dbutils.widgets.text("delta_root", "", "Delta Root URI")
dbutils.widgets.text("quarantine_threshold_rate", "0.20", "Quarantine Threshold Rate")

env = dbutils.widgets.get("environment")
ingestion_date = dbutils.widgets.get("ingestion_date")
adf_run_id = dbutils.widgets.get("adf_run_id")
account = dbutils.widgets.get("storage_account_name")
container = dbutils.widgets.get("container_name")
catalog = dbutils.widgets.get("catalog_name")
delta_root_param = dbutils.widgets.get("delta_root")
threshold = float(dbutils.widgets.get("quarantine_threshold_rate"))

storage_base = f"abfss://{container}@{account}.dfs.core.windows.net"
delta_root = delta_root_param if delta_root_param else f"{storage_base}/delta"

context = RunContext(
    environment=env,
    ingestion_date=ingestion_date,
    adf_run_id=adf_run_id,
    storage_account_name=account,
    container_name=container,
    catalog_name=catalog,
    delta_root=delta_root,
    quarantine_threshold_rate=threshold,
)

spark = get_spark_session()
task_values = TaskValueStore()

# COMMAND ----------

result = execute_silver_task(spark, context, task_values)

try:
    for k, v in task_values.get_all("silver_transformation").items():
        dbutils.jobs.taskValues.set(key=k, value=v)
except Exception as e:
    print(f"Task values set locally: {e}")

print(f"Silver Conformance Complete: {result}")
