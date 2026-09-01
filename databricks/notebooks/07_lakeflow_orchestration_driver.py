# Databricks notebook source
# MAGIC %md
# MAGIC # 07. Lakeflow Jobs Orchestration & Operational Monitoring Driver
# MAGIC
# MAGIC **Module 5:** Operationalize the Lakehouse & Warehouse Workload with **Lakeflow Jobs**
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Lakeflow Jobs Parameter retrieval (`dbutils.widgets`).
# MAGIC 2. Task Values cross-task communication (`dbutils.jobs.taskValues.set/get`).
# MAGIC 3. Operational audit table queries (`retail_lakehouse.operations.job_run_audit`).
# MAGIC 4. Failure handling and Databricks Repair-Run verification.

# COMMAND ----------

# DBTITLE 1,Retrieve Job & Task Parameters
# In Lakeflow Jobs, widgets receive job-level parameters or task-level parameters
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("ingestion_date", "2026-08-31", "Ingestion Date")
dbutils.widgets.text("adf_run_id", "manual_demo_run", "ADF Run ID")
dbutils.widgets.text("catalog_name", "retail_lakehouse", "Catalog Name")
dbutils.widgets.text("storage_account_name", "stlakehousedev", "Storage Account Name")
dbutils.widgets.text("container_name", "lakehouse", "Container Name")

env = dbutils.widgets.get("environment")
ingestion_date = dbutils.widgets.get("ingestion_date")
adf_run_id = dbutils.widgets.get("adf_run_id")
catalog_name = dbutils.widgets.get("catalog_name")
storage_account = dbutils.widgets.get("storage_account_name")
container = dbutils.widgets.get("container_name")

storage_base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
delta_root = f"{storage_base}/delta"

print("Lakeflow Job Context:")
print(f"  Environment    : {env}")
print(f"  Ingestion Date : {ingestion_date}")
print(f"  ADF Run ID     : {adf_run_id}")
print(f"  Catalog        : {catalog_name}")
print(f"  Delta Root URI : {delta_root}")

# COMMAND ----------

# DBTITLE 1,Cross-Task Values Example (dbutils.jobs.taskValues)
# Simulate setting task values for downstream consumption
try:
    # Set operational metadata for downstream tasks in the Lakeflow Jobs DAG
    dbutils.jobs.taskValues.set(key="discovered_dataset_count", value=8)
    dbutils.jobs.taskValues.set(key="landing_batch_path", value=f"{storage_base}/landing/retail")
    print("Successfully published task values to Lakeflow Jobs Context.")
except Exception as e:
    print(f"Running outside active Lakeflow Job runner (Local/Interactive mode): {e}")

# COMMAND ----------

# DBTITLE 1,Inspect Operational Run Audit History
# MAGIC %sql
# MAGIC SELECT
# MAGIC     orchestration_run_id,
# MAGIC     databricks_job_run_id,
# MAGIC     environment,
# MAGIC     ingestion_date,
# MAGIC     started_at,
# MAGIC     completed_at,
# MAGIC     final_status,
# MAGIC     duration_seconds,
# MAGIC     bronze_rows_ingested,
# MAGIC     silver_valid_rows,
# MAGIC     silver_quarantine_rows,
# MAGIC     fact_sales_rows,
# MAGIC     quality_status,
# MAGIC     failure_task,
# MAGIC     failure_classification
# MAGIC FROM delta.`/mnt/lakehouse/delta/operations/job_run_audit`
# MAGIC ORDER BY started_at DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Quarantine Trend Analysis
# MAGIC %sql
# MAGIC SELECT
# MAGIC     ingestion_date,
# MAGIC     SUM(silver_valid_rows) AS total_valid_rows,
# MAGIC     SUM(silver_quarantine_rows) AS total_quarantine_rows,
# MAGIC     ROUND(SUM(silver_quarantine_rows) * 100.0 / NULLIF(SUM(silver_valid_rows + silver_quarantine_rows), 0), 2) AS quarantine_rate_pct,
# MAGIC     MAX(CASE WHEN quarantine_alert_triggered THEN 'ALERT TRIGGERED' ELSE 'NORMAL' END) AS alert_status
# MAGIC FROM delta.`/mnt/lakehouse/delta/operations/job_run_audit`
# MAGIC GROUP BY ingestion_date
# MAGIC ORDER BY ingestion_date DESC;
