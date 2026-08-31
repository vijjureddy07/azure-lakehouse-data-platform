# Databricks notebook source
# MAGIC %md
# MAGIC # 02. Bronze Medallion Ingestion
# MAGIC
# MAGIC **Module 3: Azure Databricks + Delta Lake + Medallion Lakehouse**
# MAGIC
# MAGIC Ingests newly arrived raw landing files from Azure Data Lake Storage Gen2 (ADLS Gen2)
# MAGIC into **Bronze Delta Tables** within Unity Catalog:
# MAGIC
# MAGIC - Discovers dynamic ADF path pattern: `landing/retail/<dataset>/ingestion_date=<yyyy-MM-dd>/run_id=<run_id>/<file>`
# MAGIC - Attaches metadata audit columns (`_source_file`, `_source_path`, `_ingestion_date`, `_adf_run_id`, `_ingested_timestamp`)
# MAGIC - Maintains an immutable Delta audit log (`bronze._ingestion_audit`) preventing duplicate ingestion on reruns
# MAGIC - Ingests all 8 datasets: `customers`, `products`, `stores`, `employees`, `orders`, `order_items`, `payments`, `returns`

# COMMAND ----------

# DBTITLE 1,Widget Parameters
dbutils.widgets.text("catalog_name", "retail_lakehouse", "Catalog Name")
dbutils.widgets.text("landing_base_path", "/mnt/lakehouse/landing/retail", "Landing Base Path")
dbutils.widgets.dropdown("force_all", "false", ["true", "false"], "Force All Ingestion")

catalog_name = dbutils.widgets.get("catalog_name")
landing_base_path = dbutils.widgets.get("landing_base_path")
force_all = dbutils.widgets.get("force_all").lower() == "true"

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql("USE SCHEMA bronze")

# COMMAND ----------

# DBTITLE 1,Import Medallion Ingestion Logic
from pathlib import Path

from src.medallion.bronze import ingest_bronze_layer

# In Databricks workspace with Repo / Wheel installed:
# Bronze ingestion handles incremental file discovery and audit logging
bronze_root = Path("/mnt/lakehouse/delta/bronze")
landing_root = Path(landing_base_path)

counts = ingest_bronze_layer(
    spark=spark,
    landing_root=landing_root,
    bronze_root=bronze_root,
    force_all=force_all,
)

for dataset_name, row_count in counts.items():
    print(f"Ingested {dataset_name}: +{row_count:,} rows")

# COMMAND ----------

# DBTITLE 1,Verify Bronze Tables in Unity Catalog
# MAGIC %sql
# MAGIC SHOW TABLES IN retail_lakehouse.bronze;
