# Databricks notebook source
# MAGIC %md
# MAGIC # 06. Enterprise Dimensional Modeling, SCD & Quality Gates
# MAGIC
# MAGIC **Module 4: Advanced PySpark + Dimensional Modeling + SCD + Enterprise Quality**
# MAGIC
# MAGIC Transforms the conformed Silver Delta Lake layer into a Kimball Star Schema
# MAGIC dimensional warehouse with:
# MAGIC
# MAGIC 1. **Date Dimension (`dim_date`):** Deterministic temporal calendar with integer `date_key`.
# MAGIC 2. **SCD Type 1 (`dim_product`, `dim_store`, `dim_employee`):** In-place attribute overwrites with Delta MERGE.
# MAGIC 3. **SCD Type 2 (`dim_customer`):** Full history preservation with half-open intervals `[effective_from, effective_to)` and SHA-256 attribute hashing.
# MAGIC 4. **Point-in-Time Fact Resolution (`fact_sales`):** Dynamic interval join resolving historical customer dimension keys valid at the transaction timestamp.
# MAGIC 5. **Enterprise Quality Gates (`quality_audit`):** Automated checks for completeness, uniqueness, referential integrity, SCD2 invariants, and Decimal measure arithmetic.
# MAGIC 6. **Warehouse Reconciliation:** 100% row-count and Decimal monetary reconciliation against Silver sources.

# COMMAND ----------

# DBTITLE 1,Widget Parameters
dbutils.widgets.text("catalog_name", "retail_lakehouse", "Catalog Name")
dbutils.widgets.text("storage_account_name", "stlakehousedev", "ADLS Gen2 Storage Account")
dbutils.widgets.text("container_name", "lakehouse", "Container Name")

catalog_name = dbutils.widgets.get("catalog_name")
storage_account = dbutils.widgets.get("storage_account_name")
container = dbutils.widgets.get("container_name")

storage_base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
silver_root = f"{storage_base}/delta/silver"
warehouse_root = f"{storage_base}/delta/warehouse"

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql("CREATE SCHEMA IF NOT EXISTS warehouse COMMENT 'Kimball Star Schema Dimensional Warehouse'")
spark.sql("USE SCHEMA warehouse")

# COMMAND ----------

# DBTITLE 1,Execute Dimensional Modeling & SCD Processing
from src.modeling.catalog import register_warehouse_tables
from src.modeling.dimensions import process_dim_date, process_dim_employee, process_dim_store
from src.modeling.facts import process_fact_returns, process_fact_sales
from src.modeling.quality import run_warehouse_quality_suite
from src.modeling.reconciliation import reconcile_warehouse_sales
from src.modeling.scd_type1 import process_dim_product_scd1
from src.modeling.scd_type2 import process_dim_customer_scd2

# 1. Load Silver Conformed Sources
silver_cust = spark.read.format("delta").load(f"{silver_root}/customers")
silver_prod = spark.read.format("delta").load(f"{silver_root}/products")
silver_stor = spark.read.format("delta").load(f"{silver_root}/stores")
silver_empl = spark.read.format("delta").load(f"{silver_root}/employees")
silver_ord = spark.read.format("delta").load(f"{silver_root}/orders")
silver_items = spark.read.format("delta").load(f"{silver_root}/order_items")
silver_ret = spark.read.format("delta").load(f"{silver_root}/returns")

# 2. Build Dimensions
dim_date_df = process_dim_date(spark, f"{warehouse_root}/dim_date")
dim_prod_df = process_dim_product_scd1(spark, silver_prod, f"{warehouse_root}/dim_product")
dim_stor_df = process_dim_store(spark, silver_stor, f"{warehouse_root}/dim_store")
min_order_ts = silver_ord.select(F.min("order_timestamp")).collect()[0][0]
warehouse_history_start = min_order_ts

dim_empl_df = process_dim_employee(spark, silver_empl, dim_stor_df, f"{warehouse_root}/dim_employee")
dim_cust_df = process_dim_customer_scd2(
    spark,
    silver_cust,
    f"{warehouse_root}/dim_customer",
    initial_effective_from=warehouse_history_start,
)

# 3. Build Facts with Point-in-Time SCD2 Lookups
fact_sales_df = process_fact_sales(
    spark=spark,
    silver_order_items_df=silver_items,
    silver_orders_df=silver_ord,
    dim_customer_df=dim_cust_df,
    dim_product_df=dim_prod_df,
    dim_store_df=dim_stor_df,
    dim_date_df=dim_date_df,
    fact_sales_path=f"{warehouse_root}/fact_sales",
)
fact_returns_df = process_fact_returns(
    spark=spark,
    silver_returns_df=silver_ret,
    fact_sales_df=fact_sales_df,
    dim_date_df=dim_date_df,
    fact_returns_path=f"{warehouse_root}/fact_returns",
)

# 4. Enterprise Quality Gates
quality_results = run_warehouse_quality_suite(
    spark=spark,
    dim_customer_df=dim_cust_df,
    dim_product_df=dim_prod_df,
    dim_store_df=dim_stor_df,
    dim_date_df=dim_date_df,
    fact_sales_df=fact_sales_df,
    fact_returns_df=fact_returns_df,
    quality_audit_path=f"{warehouse_root}/quality_audit",
    raise_on_failure=True,
)

# 5. Exact Warehouse Reconciliation
recon_result = reconcile_warehouse_sales(
    silver_order_items_df=silver_items,
    fact_sales_df=fact_sales_df,
    raise_on_failure=True,
)

# 6. Register Warehouse Tables in Unity Catalog
register_warehouse_tables(spark, catalog_name, f"{storage_base}/delta")

print("Dimensional Warehouse pipeline executed and registered in Unity Catalog successfully.")

# COMMAND ----------

# DBTITLE 1,Verify Registered Warehouse Tables
# MAGIC %sql
# MAGIC SHOW TABLES IN retail_lakehouse.warehouse;

# COMMAND ----------

# DBTITLE 1,Star Schema Analytical Query: Sales by Loyalty Tier (Point-in-Time)
# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.loyalty_tier,
# MAGIC     d.year,
# MAGIC     d.quarter_name,
# MAGIC     COUNT(DISTINCT s.order_id) AS total_orders,
# MAGIC     SUM(s.quantity) AS total_units_sold,
# MAGIC     SUM(s.gross_amount) AS total_gross_sales,
# MAGIC     SUM(s.discount_amount) AS total_discounts,
# MAGIC     SUM(s.net_amount) AS total_net_sales,
# MAGIC     SUM(s.profit_amount) AS total_profit
# MAGIC FROM retail_lakehouse.warehouse.fact_sales s
# MAGIC JOIN retail_lakehouse.warehouse.dim_customer c ON s.customer_key = c.customer_key
# MAGIC JOIN retail_lakehouse.warehouse.dim_date d ON s.order_date_key = d.date_key
# MAGIC GROUP BY c.loyalty_tier, d.year, d.quarter_name
# MAGIC ORDER BY d.year DESC, d.quarter_name DESC, total_net_sales DESC;

# COMMAND ----------

# DBTITLE 1,Inspect Quality Gate Audit Log
# MAGIC %sql
# MAGIC SELECT check_name, table_name, check_type, severity, passed, observed_value, expected_value, execution_timestamp
# MAGIC FROM retail_lakehouse.warehouse.quality_audit
# MAGIC ORDER BY execution_timestamp DESC, passed ASC;
