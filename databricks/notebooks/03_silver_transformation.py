# Databricks notebook source
# MAGIC %md
# MAGIC # 03. Silver Medallion Transformation & Conformance
# MAGIC
# MAGIC **Module 3: Azure Databricks + Delta Lake + Medallion Lakehouse**
# MAGIC
# MAGIC Transforms raw Bronze Delta tables into conformed, typed, and deduplicated
# MAGIC **Silver Delta Tables** with Silver Quarantine isolation and quality reconciliation.
# MAGIC
# MAGIC ### Engineering Rules:
# MAGIC - **Strong Typing:** Cast strings to `TimestampType`, `DateType`, `IntegerType`, `DecimalType(10,2)`.
# MAGIC - **Deterministic Deduplication:** Window `row_number()` over primary keys ordered by `_ingested_timestamp DESC, _row_hash ASC`.
# MAGIC - **Financial Accuracy:** `discount_amount = quantity * unit_price * discount_percent` (no extra / 100).
# MAGIC - **Referential Integrity:** Anti-join validation against parent dimensions (customers, stores, products, orders).
# MAGIC - **Quarantine Routing:** Non-conforming rows written to `silver_quarantine_<dataset>` with detailed reason codes.
# MAGIC - **Runtime Reconciliation:** Strict verification that `bronze_count == silver_valid_count + quarantine_count`.
# MAGIC - **Delta MERGE:** Idempotent upsert capability for customer, product, and order dimensions.

# COMMAND ----------

# DBTITLE 1,Widget Parameters
dbutils.widgets.text("catalog_name", "retail_lakehouse", "Catalog Name")
dbutils.widgets.text("storage_account_name", "stlakehousedev", "ADLS Gen2 Storage Account")
dbutils.widgets.text("container_name", "lakehouse", "Container Name")

catalog_name = dbutils.widgets.get("catalog_name")
storage_account = dbutils.widgets.get("storage_account_name")
container = dbutils.widgets.get("container_name")

storage_base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
bronze_root = f"{storage_base}/delta/bronze"
silver_root = f"{storage_base}/delta/silver"
quarantine_root = f"{storage_base}/delta/silver/quarantine"

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

# DBTITLE 1,Execute Silver Transformations
from src.medallion.catalog import register_medallion_tables_in_catalog
from src.medallion.silver import process_silver_layer

metrics = process_silver_layer(
    spark=spark,
    bronze_root=bronze_root,
    silver_root=silver_root,
    quarantine_root=quarantine_root,
)

print("=" * 60)
print(f"{'DATASET':<15} | {'BRONZE':<10} | {'SILVER VALID':<12} | {'QUARANTINE':<10}")
print("=" * 60)
for ds, m in metrics.items():
    print(f"{ds:<15} | {m['bronze']:<10} | {m['silver_valid']:<12} | {m['quarantine']:<10}")
print("=" * 60)

# Register external Delta tables into Unity Catalog
register_medallion_tables_in_catalog(spark, catalog_name, f"{storage_base}/delta")

# COMMAND ----------

# DBTITLE 1,Demonstrate Delta MERGE / Upsert on Actual Silver Customer Schema
from pyspark.sql.functions import lit

from src.medallion.merge import upsert_customers

# Read an existing Silver customer, preserving full schema
existing_customer = spark.read.format("delta").load(f"{silver_root}/customers").limit(1)
initial_count = spark.read.format("delta").load(f"{silver_root}/customers").count()

# Modify loyalty_tier
customer_update_df = existing_customer.withColumn("loyalty_tier", lit("PLATINUM"))

# 1. Execute Upsert
upsert_customers(
    spark=spark,
    silver_customers_path=f"{silver_root}/customers",
    incoming_customers_df=customer_update_df,
)

# 2. Verify Rerun Idempotency (re-run exact same MERGE)
upsert_customers(
    spark=spark,
    silver_customers_path=f"{silver_root}/customers",
    incoming_customers_df=customer_update_df,
)

post_merge_count = spark.read.format("delta").load(f"{silver_root}/customers").count()
assert initial_count == post_merge_count, "Rerun MERGE must not produce duplicate records"

print(f"Idempotent Delta MERGE successfully verified. Total Silver Customers count maintained at: {post_merge_count}")
