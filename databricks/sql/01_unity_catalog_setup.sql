-- ==============================================================================
-- 01_unity_catalog_setup.sql
-- Module 3: Azure Databricks + Delta Lake + Medallion Lakehouse
--
-- Unity Catalog 3-Level Namespace Architecture:
-- <catalog>.<schema>.<table>
-- ==============================================================================

-- 1. Create Catalog
CREATE CATALOG IF NOT EXISTS retail_lakehouse
COMMENT 'Enterprise retail data platform medallion catalog';

USE CATALOG retail_lakehouse;

-- 2. Create Medallion Schemas
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw source Delta tables with full lineage and audit metadata';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleaned, typed, validated, and deduplicated conformed Delta tables';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Business-ready aggregated KPI Delta tables for reporting and analytics';

-- 3. Unity Catalog Storage Credentials & External Locations (Template)
-- Note: Replace placeholders with Azure subscription and storage details.
-- CREATE STORAGE CREDENTIAL IF NOT EXISTS cred_adls_lakehouse
-- WITH (
--   AZURE_MANAGED_IDENTITY = (
--     RESOURCE_ID = '/subscriptions/<SUBSCRIPTION_ID>/resourceGroups/<RESOURCE_GROUP>/providers/Microsoft.Databricks/accessConnectors/dbx-access-connector'
--   )
-- );

-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_lakehouse
-- URL 'abfss://lakehouse@<storage_account>.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL cred_adls_lakehouse);
