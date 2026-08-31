# Databricks notebook source
# MAGIC %md
# MAGIC # 01. Unity Catalog & Storage Access Setup
# MAGIC
# MAGIC **Module 3: Azure Databricks + Delta Lake + Medallion Lakehouse**
# MAGIC
# MAGIC This notebook initializes the Unity Catalog 3-level namespace (`<catalog>.<schema>.<table>`),
# MAGIC configures the external storage location mapped to Azure Data Lake Storage Gen2 (ADLS Gen2)
# MAGIC via the Azure Databricks Access Connector (Managed Identity), and prepares the
# MAGIC `bronze`, `silver`, and `gold` schemas.
# MAGIC
# MAGIC ### Security Principles:
# MAGIC - **Zero Stored Secrets:** No storage account access keys or SAS tokens are stored in notebooks.
# MAGIC - **Azure RBAC & Managed Identity:** Databricks Access Connector authenticates to ADLS Gen2 using Microsoft Entra ID.

# COMMAND ----------

# DBTITLE 1,Define Catalog & Storage Parameters
dbutils.widgets.text("catalog_name", "retail_lakehouse", "Catalog Name")
dbutils.widgets.text("storage_account_name", "stlakehousedev", "ADLS Gen2 Storage Account")
dbutils.widgets.text("container_name", "lakehouse", "Container Name")
dbutils.widgets.text("access_connector_resource_id", "/subscriptions/<SUB_ID>/resourceGroups/<RG>/providers/Microsoft.Databricks/accessConnectors/dbx-access-connector", "Access Connector Resource ID")

catalog_name = dbutils.widgets.get("catalog_name")
storage_account = dbutils.widgets.get("storage_account_name")
container = dbutils.widgets.get("container_name")
connector_id = dbutils.widgets.get("access_connector_resource_id")

storage_root = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

print(f"Catalog: {catalog_name}")
print(f"Storage Root: {storage_root}")
print(f"Access Connector ID: {connector_id}")

# COMMAND ----------

# DBTITLE 1,Create Storage Credential & External Location (Cloud Execution)
# MAGIC %sql
# MAGIC -- 1. Create Unity Catalog Storage Credential referencing the Databricks Access Connector Managed Identity
# MAGIC -- CREATE STORAGE CREDENTIAL IF NOT EXISTS cred_adls_lakehouse
# MAGIC -- WITH (
# MAGIC --   AZURE_MANAGED_IDENTITY = (
# MAGIC --     RESOURCE_ID = '<access_connector_resource_id>'
# MAGIC --   )
# MAGIC -- );
# MAGIC
# MAGIC -- 2. Create External Location referencing ADLS Gen2 Lakehouse container
# MAGIC -- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_lakehouse
# MAGIC -- URL 'abfss://<container>@<storage_account>.dfs.core.windows.net/'
# MAGIC -- WITH (STORAGE CREDENTIAL cred_adls_lakehouse);

# COMMAND ----------

# DBTITLE 1,Create Unity Catalog & Medallion Schemas
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"USE CATALOG {catalog_name}")

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw source tables with lineage metadata'")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Conformed, typed, validated, and deduplicated tables'")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold COMMENT 'Business-ready aggregated KPI tables'")

print(f"Unity Catalog '{catalog_name}' and Medallion schemas (bronze, silver, gold) ready.")
