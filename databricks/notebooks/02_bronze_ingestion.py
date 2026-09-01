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
# MAGIC - Registers external Delta tables in Unity Catalog under `<catalog>.bronze.<dataset>`

# COMMAND ----------

# DBTITLE 1,Widget Parameters
dbutils.widgets.text("catalog_name", "retail_lakehouse", "Catalog Name")
dbutils.widgets.text("storage_account_name", "stlakehousedev", "ADLS Gen2 Storage Account")
dbutils.widgets.text("container_name", "lakehouse", "Container Name")
dbutils.widgets.dropdown("force_all", "false", ["true", "false"], "Force All Ingestion")

catalog_name = dbutils.widgets.get("catalog_name")
storage_account = dbutils.widgets.get("storage_account_name")
container = dbutils.widgets.get("container_name")
force_all = dbutils.widgets.get("force_all").lower() == "true"

storage_base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
landing_root = f"{storage_base}/landing"
bronze_root = f"{storage_base}/delta/bronze"

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("USE SCHEMA bronze")

# COMMAND ----------

# DBTITLE 1,Import Medallion Ingestion Logic & Ingest Bronze Layer
from src.medallion.bronze import ingest_bronze_layer
from src.medallion.catalog import register_bronze_tables

counts = ingest_bronze_layer(
    spark=spark,
    landing_root=landing_root,
    bronze_root=bronze_root,
    force_all=force_all,
)

for dataset_name, row_count in counts.items():
    print(f"Ingested {dataset_name}: +{row_count:,} rows")

# Register ONLY Bronze external Delta tables into Unity Catalog
register_bronze_tables(spark, catalog_name, f"{storage_base}/delta")

# COMMAND ----------

# DBTITLE 1,Verify Bronze Tables in Unity Catalog
# MAGIC %sql
# MAGIC SHOW TABLES IN retail_lakehouse.bronze;
