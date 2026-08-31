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
# MAGIC - **Deduplication:** Window `row_number()` over primary keys ordered by ingestion timestamp.
# MAGIC - **Referential Integrity:** Anti-join validation against parent dimensions (customers, stores, products, orders).
# MAGIC - **Quarantine Routing:** Non-conforming rows written to `silver_quarantine_<dataset>` with detailed reason codes.
# MAGIC - **Delta MERGE:** Idempotent upsert capability for customer, product, and order dimensions.

# COMMAND ----------

# DBTITLE 1,Widget Parameters
dbutils.widgets.text("catalog_name", "retail_lakehouse", "Catalog Name")
catalog_name = dbutils.widgets.get("catalog_name")

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

# DBTITLE 1,Execute Silver Transformations
from pathlib import Path

from src.medallion.silver import process_silver_layer

bronze_root = Path("/mnt/lakehouse/delta/bronze")
silver_root = Path("/mnt/lakehouse/delta/silver")
quarantine_root = Path("/mnt/lakehouse/delta/silver/quarantine")

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

# COMMAND ----------

# DBTITLE 1,Demonstrate Delta MERGE / Upsert
from src.medallion.merge import upsert_customers

# Upsert demonstration
sample_customer_update = spark.createDataFrame(
    [(1, "John", "Doe", "john.doe.updated@example.com", "555-0100", "123 Main St", "Dallas", "TX", "75001", "USA", "2026-01-01 10:00:00", "ACTIVE", "PLATINUM")],
    ["customer_id", "first_name", "last_name", "email", "phone", "street_address", "city", "state", "zip_code", "country", "account_created_at", "status", "loyalty_tier"]
)

upsert_customers(
    spark=spark,
    silver_customers_path=silver_root / "customers",
    incoming_customers_df=sample_customer_update,
)

print("Idempotent Delta MERGE successfully applied to Silver Customers.")
