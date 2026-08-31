# Databricks notebook source
# MAGIC %md
# MAGIC # 04. Gold Medallion Business Analytics
# MAGIC
# MAGIC **Module 3: Azure Databricks + Delta Lake + Medallion Lakehouse**
# MAGIC
# MAGIC Derives business-ready, analytical **Gold Delta Tables** strictly from
# MAGIC conformed Silver Delta tables (never directly from raw landing files).
# MAGIC
# MAGIC ### Gold Tables Created:
# MAGIC 1. `gold_daily_sales_performance`: Daily revenue, orders, discounts, gross profit, and refunds.
# MAGIC 2. `gold_monthly_revenue`: Monthly revenue trends and order volumes.
# MAGIC 3. `gold_revenue_by_store_region`: Store and regional revenue analysis and average order value.
# MAGIC 4. `gold_category_revenue_performance`: Product category revenue, units sold, and return rates.
# MAGIC 5. `gold_customer_spending_summary`: Customer lifetime spend, order frequency, and recency.
# MAGIC 6. `gold_return_refund_performance`: Return reason distribution and financial impact.

# COMMAND ----------

# DBTITLE 1,Widget Parameters
dbutils.widgets.text("catalog_name", "retail_lakehouse", "Catalog Name")
catalog_name = dbutils.widgets.get("catalog_name")

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

# DBTITLE 1,Build Gold Analytical Tables
from pathlib import Path

from src.medallion.gold import process_gold_layer

silver_root = Path("/mnt/lakehouse/delta/silver")
gold_root = Path("/mnt/lakehouse/delta/gold")

gold_counts = process_gold_layer(
    spark=spark,
    silver_root=silver_root,
    gold_root=gold_root,
)

for table_name, count in gold_counts.items():
    print(f"Generated Gold Table: {table_name:<35} | {count:,} rows")

# COMMAND ----------

# DBTITLE 1,Query Gold Daily Sales Performance
# MAGIC %sql
# MAGIC SELECT * FROM retail_lakehouse.gold.gold_daily_sales_performance
# MAGIC ORDER BY order_date DESC
# MAGIC LIMIT 10;
