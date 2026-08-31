# Databricks notebook source
# MAGIC %md
# MAGIC # 05. Delta Lake Deep Dive: ACID, Time Travel & Schema Evolution
# MAGIC
# MAGIC **Module 3: Azure Databricks + Delta Lake + Medallion Lakehouse**
# MAGIC
# MAGIC Demonstrates the core technical differentiators of Delta Lake:
# MAGIC 1. **Delta Transaction Log (`_delta_log`):** Atomicity, ordered JSON commits, and metadata pointers.
# MAGIC 2. **Table History (`DESCRIBE HISTORY`):** Complete provenance audit trail of operations.
# MAGIC 3. **Time Travel (`VERSION AS OF` / `TIMESTAMP AS OF`):** Querying previous immutable snapshots.
# MAGIC 4. **Schema Enforcement:** Preventing unexpected data drift and silent column mismatches.
# MAGIC 5. **Controlled Schema Evolution (`mergeSchema`):** Explicitly updating schema when authorized.

# COMMAND ----------

# DBTITLE 1,Widget Parameters
dbutils.widgets.text("catalog_name", "retail_lakehouse", "Catalog Name")
catalog_name = dbutils.widgets.get("catalog_name")

spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

# DBTITLE 1,Inspect Table History
# MAGIC %sql
# MAGIC DESCRIBE HISTORY retail_lakehouse.silver.customers;

# COMMAND ----------

# DBTITLE 1,Time Travel Query by Version
# MAGIC %sql
# MAGIC -- Query state of Silver Customers at initial ingestion (Version 0)
# MAGIC SELECT COUNT(*) AS initial_version_count
# MAGIC FROM retail_lakehouse.silver.customers VERSION AS OF 0;

# COMMAND ----------

# DBTITLE 1,Demonstrate Schema Enforcement
# MAGIC %python
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# MAGIC
# MAGIC test_table_path = "/tmp/delta_schema_test"
# MAGIC
# MAGIC # 1. Create Base Table
# MAGIC df_v0 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
# MAGIC df_v0.write.format("delta").mode("overwrite").save(test_table_path)
# MAGIC
# MAGIC # 2. Attempt Appending Unexpected Column
# MAGIC df_invalid = spark.createDataFrame([(3, "Charlie", "US")], ["id", "name", "country"])
# MAGIC try:
# MAGIC     df_invalid.write.format("delta").mode("append").save(test_table_path)
# MAGIC     print("FAILURE: Schema enforcement did not catch mismatch.")
# MAGIC except Exception as e:
# MAGIC     print(f"SUCCESS: Schema enforcement blocked write:\n{str(e)[:150]}...")

# COMMAND ----------

# DBTITLE 1,Demonstrate Controlled Schema Evolution
# MAGIC %python
# MAGIC # 3. Safely Evolve Schema using mergeSchema option
# MAGIC df_invalid.write.format("delta").mode("append").option("mergeSchema", "true").save(test_table_path)
# MAGIC
# MAGIC evolved_df = spark.read.format("delta").load(test_table_path)
# MAGIC print("Evolved Delta Table Schema:")
# MAGIC evolved_df.printSchema()
# MAGIC display(evolved_df)
