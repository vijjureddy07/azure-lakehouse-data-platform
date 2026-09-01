# Azure Lakehouse Data Platform

> **Portfolio Project:** Enterprise Lakehouse Architecture & Production Data Engineering Platform  
> **Target Roles:** Lead Data Engineer | Senior Azure Data Engineer | Data Platform Architect | Databricks / Lakehouse Specialist | Analytics Engineer  
> **Key Technologies:** Azure Databricks, Delta Lake, Apache Spark / PySpark 3.5, Azure Data Factory (ADF), ADLS Gen2, Kimball Dimensional Modeling (SCD1 / SCD2), Lakeflow Jobs Orchestration, Declarative Automation Bundles, GitHub Actions CI/CD (OIDC), Serverless SQL Warehouse, Unity Catalog

---

## 📌 Executive Summary

This repository implements an end-to-end, enterprise-grade cloud data platform for an omnichannel retail enterprise. It demonstrates modern lakehouse engineering across six production modules:

1. **Module 1 (Local PySpark & Quality Framework):** Explicit StructType schemas, Decimal precision, defect quarantine routing, referential anti-joins, window functions, and partitioned Parquet storage.
2. **Module 2 (Azure Cloud Ingestion Platform):** Metadata-driven Azure Data Factory (ADF) master-child pipelines, ADLS Gen2 with Hierarchical Namespace (HNS), Entra System-Assigned Managed Identity, Azure RBAC, and immutable landing paths.
3. **Module 3 (Databricks Delta Lake Medallion Architecture):** Delta transaction log ACID transactions, time travel, schema enforcement/evolution, Medallion Bronze/Silver/Gold layers, idempotent Delta MERGE, and Unity Catalog 3-level namespace.
4. **Module 4 (Kimball Dimensional Modeling & SCD):** Enterprise star schema (`dim_customer` SCD2, `dim_product` SCD1, `dim_store`, `dim_employee`, `dim_date`, `fact_sales`, `fact_returns`), deterministic surrogate keys, point-in-time fact resolution, and enterprise data quality gates.
5. **Module 5 (Lakeflow Jobs Orchestration & Operational Auditing):** Multi-task DAG, Task Values cross-task telemetry, condition tasks, two-tier retry taxonomy (in-process transient vs fail-fast data quality), idempotent repair runs, and durable `JobRunAudit` Delta ledger.
6. **Module 6 (Production CI/CD, Bundles & Governed SQL Serving):** Python wheel packaging (`.whl`), Declarative Automation Bundles (IaC for `dev`/`prod`), GitHub Actions CI (Ruff, 102 Pytests, wheel smoke tests), zero-secret GitHub OIDC deployment, Serverless Databricks SQL Warehouse, and 8 governed Unity Catalog serving views.

---

## 🏛️ End-to-End Enterprise Architecture

```
                                      DATA PLATFORM ARCHITECTURE
                                      
  [ HTTP Raw Source Files ]
             │
             ▼ (Managed Identity + Azure RBAC)
  [ Azure Data Factory: adf-lakehouse-dev ] (Master-Child Metadata Orchestration)
             │
             ▼ (Binary Copy)
  [ ADLS Gen2 Storage Account: stlakehousedev ] (Hierarchical Namespace Enabled)
             └── Container: lakehouse/landing/retail/<dataset>/ingestion_date=<yyyy-MM-dd>/run_id=<run_id>/
                         │
                         ▼
  ====================================================================================================
                                      AZURE DATABRICKS LAKEHOUSE
  ====================================================================================================
  [ Lakeflow Jobs Orchestrator ] (Multi-Task DAG with In-Process Transient Retries & Task Values)
             │
             ├── 1. validate_landing_batch ────► Verifies 8 required datasets for batch (native retries: 0)
             │
             ├── 2. bronze_ingestion ──────────► retail_lakehouse.bronze.* (Raw strings + Lineage metadata)
             │
             ├── 3. silver_transformation ─────► retail_lakehouse.silver.* (Conformed Delta Tables)
             │                                   ├── Valid Records ──► Conformed tables
             │                                   └── Bad Records  ───► silver_quarantine_<dataset>
             │                                   └── Invariant    ───► Bronze == Valid + Quarantine
             │
             ├── 4. check_quarantine_threshold ► (Lakeflow condition_task) ──► quality_attention branch
             │
             ├── 5A. gold_analytics ───────────► retail_lakehouse.gold.* (6 Business KPI Aggregations)
             │
             ├── 5B. dimensional_warehouse ────► retail_lakehouse.warehouse.* (Kimball Star Schema)
             │                                   ├── Dimensions: dim_customer (SCD2), dim_product (SCD1),
             │                                   │               dim_store, dim_employee, dim_date
             │                                   ├── Facts:      fact_sales (PIT customer_key), fact_returns
             │                                   └── Quality:    Enterprise Data Quality Suite (EDQ)
             │
             ├── 6. final_quality_gate ────────► High-level operational health verification
             │
             └── 7. publish_run_summary ───────► delta/operations/job_run_audit (run_if: ALL_DONE)
                         │
                         ▼
  [ Unity Catalog Metastore: retail_lakehouse ] (3-Level Namespace Governance)
             │
             ▼ (Zero Data Duplication / Pure SQL Views)
  [ Governed Serving Views: retail_lakehouse.serving.* ] (8 Analytical Views)
             ├── 6 Views over Gold KPIs (daily_sales, monthly_revenue, store_region, category, customer, returns)
             └── 2 Enriched Views over Kimball Facts (sales_detail with PIT SCD2 customer_key, returns_detail)
                         │
                         ▼
  [ Serverless Databricks SQL Warehouse ] (PRO SKU, 2X-Small, Auto-Stop: 10 mins)
             │
             ▼
  [ BI Dashboards / SQL Analysts / Future Power BI ]

  ====================================================================================================
                                      RELEASE AUTOMATION (CI / CD)
  ====================================================================================================
  [ Developer PR / Push ] ──► GitHub Actions CI (.github/workflows/ci.yml)
                                 ├── 1. Ruff Static Analysis (0 errors)
                                 ├── 2. Full Pytest Suite (102/102 tests passed)
                                 ├── 3. Build & Smoke Test Python Wheel (.whl)
                                 └── 4. Bundle & Serving Contract Validation
                                 
  [ Operator Release ] ────► GitHub Actions CD (.github/workflows/deploy_databricks.yml)
                                 ├── 1. Workload Identity Federation (GitHub OIDC Token Exchange)
                                 ├── 2. Authenticate Databricks Service Principal (BUNDLE_VAR_deployment_sp_id)
                                 ├── 3. databricks version (Pinned to 1.10.0)
                                 ├── 4. databricks bundle validate --target prod
                                 └── 5. databricks bundle deploy --target prod
```

---

## 🛒 Retail Domain Data Model

The platform models an omnichannel retail enterprise with 8 relational entities:

```
customers (customer_id PK)
    ▲
    │ (FK: customer_id)
orders (order_id PK) ──(FK: store_id)──► stores (store_id PK)
    │           │                                ▲
    │           └──(FK: employee_id)──► employees (employee_id PK, store_id FK)
    │
    ├──(FK: order_id)──► payments (payment_id PK, JSON Lines)
    │
    ▼ (FK: order_id)
order_items (order_item_id PK) ──(FK: product_id)──► products (product_id PK)
    ▲
    │ (FK: order_item_id)
returns (return_id PK)
```

---

## 💎 Medallion & Dimensional Modeling Highlights

### 1. Medallion Invariant Enforcement
- **Bronze:** Ingests raw files preserving source fidelity with cloud lineage (`_source_file`, `_source_path`, `_ingestion_date`, `_adf_run_id`, `_ingested_timestamp`).
- **Silver:** Enforces strongly typed schemas, Decimal currency precision, and regex validation. Defective records (null mandatory keys, malformed emails, orphan foreign keys) route to a parallel `silver_quarantine_<dataset>` sink.
- **Mathematical Invariant:** `bronze_count == silver_valid_count + quarantine_count` is verified before downstream processing.

### 2. Kimball Star Schema & SCD Processing
- **Deterministic Surrogate Keys:** Allocated via distributed window ranking `max_existing_key + ROW_NUMBER() OVER (ORDER BY natural_key)` in `src/modeling/surrogate_keys.py`.
- **SCD Type 1 (`dim_product`):** Null-safe Delta MERGE `<=>` updating attributes while preserving stable `product_key`.
- **SCD Type 2 (`dim_customer`):** Full historical versioning with SHA-256 attribute hash (`attribute_hash`), half-open validity intervals `[effective_from, effective_to)` (`effective_to = NULL` for current records), `is_current`, and out-of-order temporal validation.
- **Point-in-Time Fact Resolution:** Joins order items to `dim_customer` on `order_timestamp >= effective_from AND (order_timestamp < effective_to OR effective_to IS NULL)` to stamp `fact_sales.customer_key`.

---

## ⚙️ Declarative Automation Bundles & Serving Layer

- **Root Bundle Config (`databricks.yml`):** Unified IaC declaration with `dev` and `prod` targets.
- **Python Wheel Packaging:** Packages core business logic (`src/`) into `retail_lakehouse_data_platform-0.1.0-py3-none-any.whl`.
- **Serverless SQL Warehouse (`databricks/resources/sql_serving.yml`):** Minimal `2X-Small` PRO compute with 10-minute auto-stop.
- **Governed SQL Serving (`databricks/sql/04_serving_views.sql`):** 8 analytical views in `retail_lakehouse.serving.*`. The `sales_detail` view joins `fact_sales` to `dim_customer` on surrogate `customer_key`, guaranteeing historical loyalty tier accuracy for BI queries without `dim_employee` fanout.

---

## 🚀 Local Setup & Build Instructions

### Prerequisites
- **Python:** 3.11+
- **Java Runtime:** OpenJDK 17 (or Java 11) for local PySpark execution:
  ```bash
  brew install openjdk@17
  ```

### 1. Environment Setup
```bash
git clone https://github.com/vijjureddy07/azure-lakehouse-data-platform.git
cd azure-lakehouse-data-platform

python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

### 2. Run Quality Checks & Test Suite (102 Tests)
```bash
# Static Code Quality
ruff check .

# Automated Pytest Suite across Modules 1–6 (102 tests)
pytest -v
```

### 3. Build & Smoke-Test Python Release Wheel
```bash
# Build wheel artifact
python -m build --wheel

# Smoke test installation
python -m venv /tmp/smoke_env
/tmp/smoke_env/bin/pip install dist/*.whl
/tmp/smoke_env/bin/python -c "import src.orchestration.models; import src.modeling.dimensions; import src.medallion.silver; print('Wheel Installed Cleanly!')"
rm -rf /tmp/smoke_env
```

### 4. Execute Local Batch Pipelines
```bash
# Module 1: Local Parquet pipeline
python -m src.pipelines.local_batch_pipeline --scale small

# Module 3: Delta Medallion pipeline (Bronze -> Silver -> Gold)
python -m src.pipelines.delta_medallion_pipeline --scale small

# Module 4: Kimball Dimensional Warehouse pipeline (SCD1, SCD2, PIT Facts, EDQ)
python -m src.pipelines.dimensional_warehouse_pipeline --scale small
```

---

## 🧪 Automated Testing & Quality Matrix

| Test Suite | Scope & Coverage | Test Count | Status |
| :--- | :--- | :--- | :--- |
| `test_schemas.py` & `test_quality_rules.py` | Explicit StructTypes, Decimal precision, anti-join quarantine routing | 15 | 🟢 PASSED |
| `test_adf_artifacts.py` | ADF JSON payloads, Managed Identity, secret scanning, verifier exit codes | 14 | 🟢 PASSED |
| `test_delta_medallion.py` | Delta transaction log, time travel, schema evolution, MERGE upserts, Gold KPIs | 18 | 🟢 PASSED |
| `test_dimensional_modeling.py` | Surrogate keys, `dim_date`, SCD1 MERGE, SCD2 intervals, PIT joins, EDQ gates | 19 | 🟢 PASSED |
| `test_orchestration.py` & `test_orchestration_workflow.py` | Lakeflow DAG, condition tasks, retry taxonomy, repair idempotency, run audit | 22 | 🟢 PASSED |
| `test_module6_cicd_bundle_serving.py` | Bundle config, SQL warehouse, serving views, fact grain, catalog parameterization, OIDC | 16 | 🟢 PASSED |
| **Total Test Suite** | **Comprehensive Full Platform Coverage** | **104 / 104** | 🟢 **100% PASS** |

---

## 📚 Project Documentation Directory

| Document | Purpose & Focus Areas |
| :--- | :--- |
| [01_DATA_ENGINEERING_FOUNDATIONS.md](docs/01_DATA_ENGINEERING_FOUNDATIONS.md) | Core DE paradigms, ETL vs ELT, batch processing, idempotency |
| [02_SPARK_PYSPARK_FOUNDATIONS.md](docs/02_SPARK_PYSPARK_FOUNDATIONS.md) | Apache Spark internals, Lazy Evaluation, Narrow vs Wide transformations, Shuffling |
| [03_DATA_QUALITY.md](docs/03_DATA_QUALITY.md) | Injected defect catalog, referential integrity anti-joins, quarantine architecture |
| [04_SPARK_SQL_WINDOWS.md](docs/04_SPARK_SQL_WINDOWS.md) | Spark SQL temporary views and PySpark window functions (`ROW_NUMBER`, `DENSE_RANK`, `LAG`) |
| [05_ADF_ADLS_CLOUD_INGESTION.md](docs/05_ADF_ADLS_CLOUD_INGESTION.md) | Azure Data Factory, ADLS Gen2 Hierarchical Namespace, Managed Identity, Azure RBAC |
| [06_DATABRICKS_DELTA_MEDALLION.md](docs/06_DATABRICKS_DELTA_MEDALLION.md) | Delta Lake ACID transaction log, Medallion Bronze/Silver/Gold, Time Travel, Unity Catalog |
| [07_DIMENSIONAL_MODELING_SCD.md](docs/07_DIMENSIONAL_MODELING_SCD.md) | Kimball star schema, SCD Type 1 & Type 2, deterministic surrogate keys, PIT joins, EDQ |
| [08_LAKEFLOW_JOBS_ORCHESTRATION.md](docs/08_LAKEFLOW_JOBS_ORCHESTRATION.md) | Lakeflow Jobs multi-task DAG, Task Values, two-tier retry taxonomy, operational run auditing |
| [09_CICD_BUNDLES_SERVING.md](docs/09_CICD_BUNDLES_SERVING.md) | Wheel packaging, Declarative Automation Bundles, GitHub Actions OIDC CD, SQL Serving |
| [IMPLEMENTATION_MAP.md](docs/IMPLEMENTATION_MAP.md) | Skill-to-code traceability matrix connecting resume skills to concrete source code |
| [INTERVIEW_QA.md](docs/INTERVIEW_QA.md) | 48 comprehensive Data Engineering interview questions and architectural answers |
| [PROGRESS.md](docs/PROGRESS.md) | Milestone status board and detailed objective verification checklist |

---

## 🔒 Cloud Verification & Learning Status

- **Local Implementation & Automated Testing:** 🟢 **100% COMPLETE & VERIFIED** (102/102 tests passing, 0 Ruff errors)
- **Static Resource Contract Tests:** 🟢 **VERIFIED**
- **Authenticated Databricks Bundle Validation:** ⏳ **PENDING** (Requires live Databricks CLI authentication and workspace connection)
- **Cloud Deployment Verification:** ⏳ **PENDING** (Live deployment requires active Azure subscription and Databricks workspace credentials)
- **Learning Status:** ⏳ **NOT STUDIED / PENDING** (Maintained per workflow rule: `BUILD FIRST -> DOCUMENT EVERYTHING -> LEARN LATER`)
