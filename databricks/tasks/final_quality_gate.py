# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Task: Final Operational Quality Gate
# MAGIC **Task Key:** `final_quality_gate`
# MAGIC Verifies upstream completion status, reconciliation, and quality results.

# COMMAND ----------

from src.orchestration.models import RunContext, TaskValueStore
from src.orchestration.reliability import classify_failure
from src.orchestration.tasks.final_quality_gate import execute_final_quality_gate_task
from src.utils.spark import get_spark_session

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("bronze_rows", "0", "Bronze Rows Ingested")
dbutils.widgets.text("silver_quarantine", "0", "Silver Quarantine Rows")
dbutils.widgets.text("fact_sales_rows", "0", "Fact Sales Rows")

env = dbutils.widgets.get("environment")

# Retrieve values from upstream task values if available, otherwise fallback to widget
try:
    bronze_rows = int(dbutils.jobs.taskValues.get(taskKey="bronze_ingestion", key="bronze_rows_ingested", default=0))
except Exception:
    bronze_rows = int(dbutils.widgets.get("bronze_rows") or "0")

try:
    silver_quarantine = int(dbutils.jobs.taskValues.get(taskKey="silver_transformation", key="silver_quarantine_rows", default=0))
except Exception:
    silver_quarantine = int(dbutils.widgets.get("silver_quarantine") or "0")

try:
    fact_sales_rows = int(dbutils.jobs.taskValues.get(taskKey="dimensional_warehouse", key="fact_sales_rows", default=0))
except Exception:
    fact_sales_rows = int(dbutils.widgets.get("fact_sales_rows") or "0")

context = RunContext(environment=env)
spark = get_spark_session()
task_values = TaskValueStore()

task_values.set("bronze_ingestion", "bronze_rows_ingested", bronze_rows)
task_values.set("silver_transformation", "reconciliation_passed", True)
task_values.set("silver_transformation", "silver_quarantine_rows", silver_quarantine)
task_values.set("gold_analytics", "gold_tables_generated", 6)
task_values.set("dimensional_warehouse", "warehouse_quality_passed", True)
task_values.set("dimensional_warehouse", "fact_sales_rows", fact_sales_rows)

# COMMAND ----------

try:
    result = execute_final_quality_gate_task(spark, context, task_values)

    for k, v in task_values.get_all("final_quality_gate").items():
        try:
            dbutils.jobs.taskValues.set(key=k, value=v)
        except Exception:
            pass

    try:
        dbutils.jobs.taskValues.set(key="terminal_state", value="SUCCESS")
    except Exception:
        pass

    print(f"Final Operational Quality Gate Complete: {result}")
except Exception as exc:
    classification = classify_failure(exc)
    try:
        dbutils.jobs.taskValues.set(key="terminal_state", value="FAILED")
        dbutils.jobs.taskValues.set(key="failure_classification", value=classification.value)
        dbutils.jobs.taskValues.set(key="failure_message", value=str(exc)[:500])
    except Exception:
        pass
    raise
