# Project Progress & Status Tracker

> **Workflow Rule:** BUILD FIRST → DOCUMENT EVERYTHING → LEARN LATER  
> Learning status remains **NOT STUDIED / PENDING** until all portfolio builds are complete and dedicated study begins.

---

## 📊 Summary Status Board

| Milestone | Build Status | Test Status | Learning Status |
| :--- | :--- | :--- | :--- |
| **Module 1: Local PySpark & Quality Framework** | 🟢 **COMPLETE** | 🟢 **PASSED (15 Tests)** | ⏳ **NOT STUDIED / PENDING** |
| **Module 2: ADF + ADLS Gen2 Cloud Ingestion** | 🟢 **COMPLETE (Deployment-Ready)**<br>*(Cloud Verification Pending)* | 🟢 **PASSED (8 Tests)** | ⏳ **NOT STUDIED / PENDING** |
| **Module 3: Databricks + Delta Lake + Medallion** | ⏹️ NOT STARTED | ⏹️ NOT STARTED | ⏳ **NOT STUDIED / PENDING** |
| **Module 4: Advanced PySpark + Data Quality + Modeling** | ⏹️ NOT STARTED | ⏹️ NOT STARTED | ⏳ **NOT STUDIED / PENDING** |
| **Module 5: Orchestration + Databricks Jobs + Alerts** | ⏹️ NOT STARTED | ⏹️ NOT STARTED | ⏳ **NOT STUDIED / PENDING** |
| **Module 6: CI/CD + Serving Architecture** | ⏹️ NOT STARTED | ⏹️ NOT STARTED | ⏳ **NOT STUDIED / PENDING** |

---

## 🎯 Module 1 Detailed Objectives Checklist

- [x] **Project Structure:** Clean, modular Python package structure with `src/`, `tests/`, `sql/`, `data/`, `output/`, and `docs/`.
- [x] **Deterministic Data Generation:** Synthetic omnichannel retail data generator with configurable scales (`small` vs `standard`) and fixed random seed (`seed=42`).
- [x] **Injected Real-World Defects:** Documented defects (nulls, duplicates, invalid emails, malformed dates, non-positive quantities/prices, orphan foreign keys, unreconciled payments).
- [x] **Explicit StructType Schemas:** Strict schema contracts for all 8 source datasets using `DecimalType` for all financial columns.
- [x] **Local PySpark Setup:** Reusable `SparkSession` factory with multi-threaded local master (`local[*]`), UTC timezone, and 4 shuffle partitions.
- [x] **Multi-Format Ingestion:** Schema-enforced ingestion of both CSV files and JSON lines (`payments.json`).
- [x] **Transformations & Cleaning:** Whitespace trimming, email lowercase/regex validation, state/country normalization, and date parsing.
- [x] **Deduplication:** Primary key deduplication via `ROW_NUMBER()` window function ranking.
- [x] **Referential Integrity Checks:** Foreign key orphan detection across all parent-child relationships via anti-joins and left joins.
- [x] **Quarantine Architecture:** Routing rejected records to standardized `QUARANTINE_SCHEMA` Parquet sink with audit trail.
- [x] **Analytical Sales Curation:** Enriched business sales dataset joining orders, items, products, customers, and stores.
- [x] **Financial Calculations:** Decimal precision derivations for gross sales, discount amount, net sales, and profit margin.
- [x] **Window Functions:** Implemented `ROW_NUMBER` (order sequence), `SUM OVER` (running cumulative spend), `LAG` (days between orders), and `DENSE_RANK` (category product ranking).
- [x] **Spark SQL Analytics:** Modular ANSI SQL queries executing against in-memory temporary views (`v_curated_sales`, `v_returns`, etc.).
- [x] **Columnar Parquet Output:** Cleaned, quarantine, curated, and metrics datasets written to Parquet, with temporal partitioning (`order_year`, `order_month`).
- [x] **Pipeline Idempotency:** Overwrite semantics ensuring repeatable, deterministic execution.
- [x] **Data Quality Reconciliation:** Metrics table validating that `source_count = valid_count + quarantine_count`.
- [x] **Automated Test Suite:** Comprehensive unit and integration test suite using Pytest and session-scoped Spark fixture.
- [x] **Documentation & Learning Guides:** Comprehensive guides created for data engineering foundations, PySpark internals, quality architecture, window functions, and interview Q&A.

---

## 🎯 Module 2 Detailed Objectives Checklist

- [x] **ADLS Gen2 Storage Architecture:** Storage account with Hierarchical Namespace (HNS) enabled (`isHnsEnabled: true`) and primary `lakehouse` container.
- [x] **Managed Identity Authentication:** System-Assigned Managed Identity for Azure Data Factory with zero hardcoded keys in version control.
- [x] **Azure RBAC Authorization:** Role assignment granting ADF Managed Identity the `Storage Blob Data Contributor` role on the storage account scope.
- [x] **Parameterized ADF Linked Services:** `ls_adls_gen2.json` (parameterized for storage account) and `ls_http_source.json` (parameterized for source URL).
- [x] **Parameterized ADF Datasets:** `ds_http_raw_file.json` and `ds_adls_landing_file.json` with dynamic paths.
- [x] **Child Ingestion Pipeline:** `pl_ingest_single_file.json` containing parameterized Binary Copy Activity from HTTP to ADLS Gen2 landing zone.
- [x] **Master Orchestration Pipeline:** `pl_master_retail_ingestion.json` with `ForEach` activity iterating across metadata array for all 8 retail datasets (`customers`, `products`, `stores`, `employees`, `orders`, `order_items`, `payments`, `returns`).
- [x] **Dynamic Landing Paths:** Dynamic expression `landing/retail/<dataset_name>/ingestion_date=<yyyy-MM-dd>/run_id=<run_id>/<file_name>` ensuring isolated immutable raw snapshots.
- [x] **Raw Fidelity Preservation:** Copy activity preserves exact source formats (CSV as CSV, JSON as JSON) without alteration.
- [x] **Infrastructure as Code:** Bicep template (`infra/bicep/main.bicep`) and ARM template (`infra/arm_template.json`).
- [x] **Deployment Automation:** Shell provisioning script (`scripts/deploy_azure_resources.sh`).
- [x] **Verification Tool:** Python verification script (`scripts/verify_azure_deployment.py`) supporting local validation and live cloud audit.
- [x] **Unit Testing & Secret Scanning:** Automated Pytest test suite (`tests/unit/test_adf_artifacts.py`) verifying JSON syntax, contracts, parameter schemas, and scanning for exposed credentials.
- [x] **Module 2 Study Guide:** Comprehensive study guide (`docs/05_ADF_ADLS_CLOUD_INGESTION.md`) and interview Q&A.
