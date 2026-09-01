# 06. Azure Databricks + Delta Lake Medallion Architecture (Module 3 Study Guide)

> **Build Status:** 🟢 COMPLETE & LOCAL-VERIFIED  
> **Cloud Verification Status:** ⏳ LIVE DATABRICKS CLOUD VERIFICATION PENDING (Azure Databricks credentials required)  
> **Learning Status:** ⏳ NOT STUDIED / PENDING  

---

## 1. Azure Databricks Architecture: Control Plane vs Compute Plane

### ARCHITECTURAL OVERVIEW
Azure Databricks is an enterprise-grade analytics platform built on Apache Spark and Delta Lake. It divides operations into two distinct planes:

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 AZURE DATABRICKS CONTROL PLANE                                   │
│  - Hosted in Microsoft-managed Databricks infrastructure                                         │
│  - Manages Workspace UI, Notebooks, Job Scheduler, Access Control (RBAC), and REST APIs          │
│  - Houses Unity Catalog Central Metastore (Catalog metadata, permissions, table definitions)     │
└────────────────────────────────────────────────┬─────────────────────────────────────────────────┘
                                                 │ Secure RPC / Cluster Management
                                                 ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                           AZURE DATABRICKS COMPUTE (DATA) PLANE                                  │
│  - Deployed in customer's Azure subscription (or managed VNet)                                   │
│  - Driver & Worker Nodes (Azure VMs / Spark Cluster) executing query plans                       │
│  - Direct High-Speed Storage Access to ADLS Gen2 over ABFSS protocol via Managed Identity        │
│  - Zero customer data is persisted in the Control Plane                                          │
└────────────────────────────────────────────────┬─────────────────────────────────────────────────┘
                                                 │ ABFSS (Azure Blob File System)
                                                 ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                        AZURE DATA LAKE STORAGE GEN2 (ADLS Gen2)                                  │
│  - Landing Zone: landing/retail/<dataset>/ingestion_date=<yyyy-MM-dd>/run_id=<run_id>/<file>     │
│  - Delta Lake Storage: delta/ (bronze/, silver/, silver/quarantine/, gold/)                      │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Delta Lake Core Architecture & Transaction Log Internals

### PARQUET VS DELTA LAKE
| Dimension | Standard Apache Parquet | Delta Lake Table |
| :--- | :--- | :--- |
| **Storage Format** | Immutable columnar file storage | Columnar Parquet data files + `_delta_log/` transaction journal |
| **Transactions** | None (Non-atomic writes leave orphan files on failure) | Full **ACID Transactions** with atomic commit guarantees |
| **Concurrency** | Blind overwrites cause race conditions and corrupt reads | **Optimistic Concurrency Control (OCC)** |
| **Mutation** | Rewrite entire dataset/partition to update a single row | Granular **MERGE (Upsert)**, UPDATE, and DELETE operations |
| **Auditability** | None | Comprehensive `DESCRIBE HISTORY` and **Time Travel** |
| **Schema Governance**| Schema drift causes silent column truncation or errors | Strict **Schema Enforcement** and controlled **Schema Evolution** |

### THE DELTA TRANSACTION LOG (`_delta_log`)
Every Delta table maintains a `_delta_log/` directory containing ordered, zero-padded JSON commit logs (`00000000000000000000.json`, `00000000000000000001.json`, etc.):
- **Single Source of Truth:** A Parquet file physically present in the folder is NOT part of the table until an atomic commit is recorded in `_delta_log`.
- **Atomic Commits:** Readers parse the transaction log sequentially to reconstruct the current active snapshot of valid Parquet file paths.
- **Checkpoints:** Every 10 commits, Delta automatically generates a compacted Parquet checkpoint file (`00000000000000000010.checkpoint.parquet`) to eliminate the overhead of reading thousands of JSON files.

---

## 3. The Medallion Architecture (Landing ➔ Bronze ➔ Silver ➔ Gold)

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                          THE MEDALLION LIFECYCLE                                           │
│                                                                                                            │
│  [ ADLS Gen2 Landing Zone ]                                                                                │
│  - Immutable external raw CSV / JSON Lines files as landed by Azure Data Factory                           │
│  - Organized by: landing/retail/<dataset>/ingestion_date=<yyyy-MM-dd>/run_id=<run_id>/<file>               │
│               │                                                                                            │
│               ▼ (Incremental Discovery & Audit Log: bronze._ingestion_audit)                               │
│  [ Bronze Layer: retail_lakehouse.bronze ]                                                                 │
│  - Raw Delta tables preserving exact source fidelity (Standard JSON Lines reading)                         │
│  - System lineage metadata attached: _source_file, _source_path, _ingestion_date, _adf_run_id, _ingested_ts │
│  - File SHA-256 computed locally; cloud ingestion identity uses immutable source path / ADF run path       │
│  - Zero data loss: Defective records are NOT dropped prematurely                                           │
│               │                                                                                            │
│               ▼ (Strong Typing, Deterministic Deduplication, Decimal Precision, Defect Routing)            │
│  [ Silver Layer: retail_lakehouse.silver ]                                                                 │
│  - Strongly-typed schemas (Decimal financial precision, DateType, TimestampType)                           │
│  - Deterministic tie-breaking: Window ROW_NUMBER ordered by _ingested_timestamp DESC, _row_hash ASC        │
│  - Financial precision: discount_amount = quantity * unit_price * discount_percent                         │
│  - Quality quarantine routing: Non-conforming rows written to silver_quarantine_<dataset>                  │
│  - Runtime Reconciliation: Strictly enforces bronze_count == silver_valid_count + quarantine_count         │
│  - Delta MERGE: Idempotent upsert by business primary key                                                  │
│               │                                                                                            │
│               ▼ (Business KPI Aggregation & Metrics)                                                       │
│  [ Gold Layer: retail_lakehouse.gold ]                                                                     │
│  - High-performance, business-ready Delta analytical aggregates                                           │
│  - Derived strictly from Silver Delta tables (never directly from landing)                                 │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Delta Lake Key Features in This Implementation

### 1. DELTA MERGE (UPSERT)
Delta Lake `MERGE` executes atomic, idempotent upserts:
```sql
MERGE INTO silver.customers AS target
USING incoming_updates AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;
```
- **Idempotency Guarantee:** Rerunning an identical batch produces zero duplicate rows.
- **Granular Updates:** Matched rows are updated, new rows are inserted, and unaffected records remain untouched.

### 2. TABLE HISTORY & TIME TRAVEL
Delta Lake automatically tracks every transaction:
```sql
-- Audit table operations
DESCRIBE HISTORY retail_lakehouse.silver.customers;

-- Query earlier table version (Time Travel)
SELECT * FROM retail_lakehouse.silver.customers VERSION AS OF 0;

-- Query table state at a specific timestamp
SELECT * FROM retail_lakehouse.silver.customers TIMESTAMP AS OF '2026-08-31T10:00:00Z';
```

### 3. SCHEMA ENFORCEMENT VS SCHEMA EVOLUTION
- **Schema Enforcement:** Prevents accidental data corruption by throwing `AnalysisException` if an append operation contains unexpected or incompatible columns.
- **Controlled Schema Evolution:** Safely merges newly introduced columns when explicitly authorized:
  ```python
  df_evolved.write.format("delta").mode("append").option("mergeSchema", "true").save(table_path)
  ```

---

## 5. Unity Catalog Governance & Zero-Secret Security

### THREE-LEVEL NAMESPACE
Unity Catalog provides unified data governance across all Databricks workspaces:
```
<catalog_name>.<schema_name>.<table_name>
(e.g., retail_lakehouse.silver.customers)
```

### ZERO-STORED-CREDENTIAL SECURITY MODEL
- **No Hard-Coded Keys:** Never store Azure storage access keys, SAS tokens, or secrets in notebooks.
- **No DBFS Mounts:** Direct ABFSS URI access backed by Unity Catalog External Locations.
- **Azure Databricks Access Connector:** A dedicated Azure Managed Identity (`Microsoft.Databricks/accessConnectors`) assigned the `Storage Blob Data Contributor` RBAC role on ADLS Gen2.
- **Unity Catalog Storage Credential & External Location:**
  ```sql
  CREATE STORAGE CREDENTIAL cred_adls_lakehouse
  WITH (AZURE_MANAGED_IDENTITY = (RESOURCE_ID = '/subscriptions/.../accessConnectors/dbx-access-connector-dev'));

  CREATE EXTERNAL LOCATION ext_loc_lakehouse
  URL 'abfss://lakehouse@stlakehousedev.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL cred_adls_lakehouse);
  ```

### LAYER-ISOLATED EXTERNAL TABLE REGISTRATION
Delta tables written to external storage paths are registered into Unity Catalog in layer-specific order to ensure external table locations exist before registration:

- **Notebook 02 (Bronze):** Writes Bronze Delta tables $\rightarrow$ registers `retail_lakehouse.bronze.*` (8 tables).
- **Notebook 03 (Silver):** Writes Silver & Quarantine Delta tables $\rightarrow$ registers `retail_lakehouse.silver.*` (8 conformed + 8 quarantine tables).
- **Notebook 04 (Gold):** Writes Gold analytical Delta tables $\rightarrow$ registers `retail_lakehouse.gold.*` (6 KPI tables).

---

## 6. Interview Questions & Expected Answers

### INTERVIEW QUESTION 1
> "How does your raw landing data from Azure Data Factory become a Delta Lake medallion architecture?"

### EXPECTED ANSWER
> "In our architecture, Azure Data Factory lands external raw files into ADLS Gen2 under a dynamic immutable directory pattern partitioned by date and ADF RunId. In Databricks, an incremental discovery scanner detects newly landed files by checking against a Delta ingestion audit log (`bronze._ingestion_audit`), preventing duplicate ingestion on pipeline reruns.
> 1. **Bronze Layer:** Files are ingested as raw strings (using standard JSON Lines parsing for payments) into Delta tables with system lineage metadata (`_source_file`, `_source_path`, `_ingestion_date`, `_adf_run_id`, `_ingested_timestamp`), preserving raw source fidelity without premature drops. File hash is computed locally; cloud ingestion identity uses immutable source paths.
> 2. **Silver Layer:** Bronze tables are transformed into conformed Delta tables with explicit typed schemas (using Decimal precision for financials), deterministic window-ranked deduplication (`_ingested_timestamp DESC, _row_hash ASC`), exact discount arithmetic, and referential anti-joins against validated dimension tables. Defective rows are isolated in `silver_quarantine_<dataset>` tables, strictly verified at runtime by a reconciliation validator (`bronze == valid + quarantine`).
> 3. **Gold Layer:** Business-ready aggregate Delta tables (such as daily sales performance, customer lifetime spend, and store regional revenue) are computed strictly from Silver tables for reporting and analytics. Tables are registered layer-by-layer in Unity Catalog only after their Delta storage paths are committed."

---

### INTERVIEW QUESTION 2
> "How does Delta Lake's transaction log guarantee ACID transactions and enable time travel?"

### EXPECTED ANSWER
> "Delta Lake maintains an ordered, immutable transaction journal in the `_delta_log/` directory. When a write, merge, or delete operation executes, Delta writes the new data files to Parquet and commits a JSON entry containing file actions (`add` or `remove`). Readers determine the valid state of the table by scanning the transaction log from the latest checkpoint. Because writes commit atomically via log creation, failed operations never leave orphan records in the table view (Atomicity & Isolation). Time travel is achieved by reading the transaction log up to a specific version number (`VERSION AS OF N`) or timestamp, allowing Spark to construct the exact snapshot of active files at that historical point."

---

### INTERVIEW QUESTION 3
> "Why is Managed Identity with an Azure Databricks Access Connector preferred over storage account keys or DBFS mounts in Unity Catalog?"

### EXPECTED ANSWER
> "Storage account keys grant full superuser root bypass over the entire storage account and cannot be scoped or audited by identity. Legacy DBFS mounts store credentials cluster-wide, exposing data to any user with compute access. In contrast, Unity Catalog leverages the Azure Databricks Access Connector—a native Microsoft Entra Managed Identity. The Access Connector is granted the least-privilege role 'Storage Blob Data Contributor' over ADLS Gen2. Unity Catalog manages table-level and column-level SQL permissions centrally without ever exposing storage credentials to end users or storing secrets in notebooks."
