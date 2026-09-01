# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Task: Gold Layer Analytical Aggregations
# MAGIC **Task Key:** `gold_analytics`
# MAGIC Builds 6 business KPI Delta aggregate tables from conformed Silver data.

# COMMAND ----------

from src.orchestration.models import FailureClassification, RunContext, TaskValueStore
from src.orchestration.reliability import RetryPolicy, classify_failure, execute_with_retry
from src.orchestration.tasks.run_gold import execute_gold_task
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
policy = RetryPolicy(max_retries=1, retryable_classifications={FailureClassification.TRANSIENT})

# COMMAND ----------

try:
    result, _, _ = execute_with_retry(
        lambda: execute_gold_task(spark, context, task_values),
        "gold_analytics",
        policy,
    )

    for k, v in task_values.get_all("gold_analytics").items():
        try:
            dbutils.jobs.taskValues.set(key=k, value=v)
        except Exception:
            pass

    try:
        dbutils.jobs.taskValues.set(key="terminal_state", value="SUCCESS")
    except Exception:
        pass

    print(f"Gold Aggregations Complete: {result}")
except Exception as exc:
    classification = classify_failure(exc)
    try:
        dbutils.jobs.taskValues.set(key="terminal_state", value="FAILED")
        dbutils.jobs.taskValues.set(key="failure_classification", value=classification.value)
        dbutils.jobs.taskValues.set(key="failure_message", value=str(exc)[:500])
    except Exception:
        pass
    raise
