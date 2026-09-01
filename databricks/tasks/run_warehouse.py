# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Task: Kimball Dimensional Warehouse Modeling
# MAGIC **Task Key:** `dimensional_warehouse`
# MAGIC Builds SCD Type 1 & 2 dimensions, PIT facts, and executes enterprise quality gates.

# COMMAND ----------

from src.orchestration.models import RunContext, TaskValueStore
from src.orchestration.tasks.run_warehouse import execute_warehouse_task
from src.utils.spark import get_spark_session

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("storage_account_name", "stlakehousedev", "Storage Account Name")
dbutils.widgets.text("container_name", "lakehouse", "Container Name")
dbutils.widgets.text("catalog_name", "retail_lakehouse", "Catalog Name")
dbutils.widgets.text("delta_root", "", "Delta Root URI")

env = dbutils.widgets.get("environment")
account = dbutils.widgets.get("storage_account_name")
container = dbutils.widgets.get("container_name")
catalog = dbutils.widgets.get("catalog_name")
delta_root_param = dbutils.widgets.get("delta_root")

storage_base = f"abfss://{container}@{account}.dfs.core.windows.net"
delta_root = delta_root_param if delta_root_param else f"{storage_base}/delta"

context = RunContext(
    environment=env,
    storage_account_name=account,
    container_name=container,
    catalog_name=catalog,
    delta_root=delta_root,
)

spark = get_spark_session()
task_values = TaskValueStore()

# COMMAND ----------

result = execute_warehouse_task(spark, context, task_values)

try:
    for k, v in task_values.get_all("dimensional_warehouse").items():
        dbutils.jobs.taskValues.set(key=k, value=v)
except Exception as e:
    print(f"Task values set locally: {e}")

print(f"Dimensional Warehouse Complete: {result}")
