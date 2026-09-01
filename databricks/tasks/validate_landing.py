# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Task: Validate Landing Batch
# MAGIC **Task Key:** `validate_landing_batch`
# MAGIC Verifies that the requested ADF landing batch exists and contains all 8 required datasets.

# COMMAND ----------

from src.orchestration.models import FailureClassification, RunContext, TaskValueStore
from src.orchestration.reliability import RetryPolicy, classify_failure, execute_with_retry
from src.orchestration.tasks.validate_landing import execute_validate_landing_task
from src.utils.spark import get_spark_session

# COMMAND ----------

# Retrieve Parameters from Lakeflow Widgets
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("ingestion_date", "", "Ingestion Date")
dbutils.widgets.text("adf_run_id", "", "ADF Run ID")
dbutils.widgets.text("storage_account_name", "stlakehousedev", "Storage Account Name")
dbutils.widgets.text("container_name", "lakehouse", "Container Name")
dbutils.widgets.text("catalog_name", "retail_lakehouse", "Catalog Name")
dbutils.widgets.text("landing_root", "", "Landing Root URI")
dbutils.widgets.text("delta_root", "", "Delta Root URI")

env = dbutils.widgets.get("environment")
ingestion_date = dbutils.widgets.get("ingestion_date")
adf_run_id = dbutils.widgets.get("adf_run_id")
account = dbutils.widgets.get("storage_account_name")
container = dbutils.widgets.get("container_name")
catalog = dbutils.widgets.get("catalog_name")
landing_root_param = dbutils.widgets.get("landing_root")
delta_root_param = dbutils.widgets.get("delta_root")

storage_base = f"abfss://{container}@{account}.dfs.core.windows.net"
landing_root = landing_root_param if landing_root_param else f"{storage_base}/landing"
delta_root = delta_root_param if delta_root_param else f"{storage_base}/delta"

context = RunContext(
    environment=env,
    ingestion_date=ingestion_date,
    adf_run_id=adf_run_id,
    storage_account_name=account,
    container_name=container,
    catalog_name=catalog,
    landing_root=landing_root,
    delta_root=delta_root,
)

spark = get_spark_session()
task_values = TaskValueStore()
policy = RetryPolicy(max_retries=1, retryable_classifications={FailureClassification.TRANSIENT})

# COMMAND ----------

try:
    result, _, _ = execute_with_retry(
        lambda: execute_validate_landing_task(spark, context, task_values),
        "validate_landing_batch",
        policy,
    )

    # Publish Lakeflow Task Values
    for k, v in task_values.get_all("validate_landing_batch").items():
        try:
            dbutils.jobs.taskValues.set(key=k, value=v)
        except Exception:
            pass

    print(f"Validate Landing Batch Complete: {result}")
except Exception as exc:
    classification = classify_failure(exc)
    try:
        dbutils.jobs.taskValues.set(key="failure_classification", value=classification.value)
        dbutils.jobs.taskValues.set(key="failure_message", value=str(exc)[:500])
    except Exception:
        pass
    raise
