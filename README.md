# Azure Lakehouse Data Platform

> **Portfolio Project:** Scalable Lakehouse Architecture & Data Engineering Pipelines  
> **Target Roles:** Data Engineer | Azure Data Engineer | Data Engineering Analyst | Data Platform Engineer | Databricks Engineer | ETL Engineer

---

## 📌 Project Overview

This repository demonstrates the engineering, modeling, data quality enforcement, and analytical curation of an omnichannel retail enterprise data platform. 

The project is structured into progressive modules. **Module 1 (Implemented Now)** establishes genuine local Data Engineering and PySpark foundations—including explicit schema enforcement, defect quarantine routing, referential integrity validation, financial Decimal precision, window functions, Spark SQL analytics, and partitioned Parquet storage—prior to introducing cloud orchestration and cloud infrastructure in subsequent modules.

---

## 🏛️ Architecture & Scope Breakdown

```
+----------------------------------------------------------------------------------------------------+
|                                    MODULE 1: CURRENT IMPLEMENTATION                                 |
|                                                                                                    |
|  [ Synthetic Raw Data ] (CSV & JSON Lines with Injected Real-World Defects)                        |
|             │                                                                                      |
|             ▼                                                                                      |
|  [ Local Apache Spark / PySpark ]                                                                  |
|             ├── 1. Schema Enforcement (Explicit StructType with DecimalType monetary precision)    |
|             ├── 2. Data Quality & Cleaning (Trimming, email normalization, regex, date parsing)    |
|             ├── 3. Duplicate Elimination & Window Ranking (ROW_NUMBER over partition)              |
|             ├── 4. Referential Integrity (Anti-joins & Left joins detecting orphan foreign keys)   |
|             └── 5. Standardized Quarantine Routing (Serializing rejected rows to JSON audit trail) |
|             │                                                                                      |
|             ├── Cleaned Data Sinks ──────────────► [ output/cleaned/*.parquet ]                    |
|             ├── Quarantine Data Sinks ───────────► [ output/quarantine/*.parquet ]                 |
|             │                                                                                      |
|             ▼                                                                                      |
|  [ Analytical Curation & Window Functions ]                                                        |
|             ├── Line-level financials (gross, discount, net, profit)                               |
|             ├── Customer order sequencing (ROW_NUMBER)                                             |
|             ├── Customer running cumulative spend (SUM over sliding frame)                         |
|             ├── Purchase interval elapsed days (LAG)                                               |
|             └── Product category revenue leaderboards (DENSE_RANK)                                 |
|             │                                                                                      |
|             ▼                                                                                      |
|  [ Partitioned Storage Sink ] ───────────────────► [ output/curated/curated_sales/ ]               |
|                                                    (Partitioned by order_year, order_month)        |
|             │                                                                                      |
|             ▼                                                                                      |
|  [ Spark SQL Analytics Views ] ──────────────────► [ In-Memory Temporary Views & KPI Reports ]     |
|  [ Quality Audit Reconciliation ] ───────────────► [ output/metrics/quality_summary/ ]             |
+----------------------------------------------------------------------------------------------------+
```

### 🗺️ Planned Cloud Roadmap (Upcoming Modules)
- **Module 2:** Azure Ingestion — Azure Data Factory (ADF) pipelines & ADLS Gen2 Hierarchical Storage.
- **Module 3:** Azure Databricks, Delta Lake ACID Transactions, and Medallion Architecture (Bronze -> Silver -> Gold).
- **Module 4:** Advanced PySpark, Dimensional Modeling (Star Schema / Kimball), and Slowly Changing Dimensions (SCD Type 1 & 2).
- **Module 5:** Pipeline Orchestration with Databricks Workflows, Azure Monitor, and Logging.
- **Module 6:** CI/CD Automation with GitHub Actions, Automated PySpark Testing, and Serving Layers.

---

## 🛒 Domain & Datasets

The platform models an omnichannel retail enterprise with 8 interconnected datasets:

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

## 🛡️ Data Quality & Quarantine Framework

To reflect real-world operational challenges, the raw data generator intentionally injects defects at controlled rates using a fixed random seed (`seed=42`). 

The pipeline never silently drops data. Instead, it classifies each record, routes valid data forward, and isolates rejected records into a standardized `QUARANTINE_SCHEMA` with full audit fields (`record_id`, `source_dataset`, `rejection_reason`, `raw_record`, `ingested_at`).

### Handled Defect Types:
1. **Null Mandatory Fields:** Empty or missing primary/business keys.
2. **Duplicate Keys:** Multiple records sharing the same primary key (deduplicated via `ROW_NUMBER()`).
3. **Invalid Email Formats:** Malformed email strings failing email format validation using a practical regex.
4. **Malformed Dates / Timestamps:** Unparseable date strings (e.g. `'2023-99-99'`).
5. **Non-Positive Prices & Quantities:** Items with quantity $\le 0$ or price $< 0$.
6. **Referential Integrity Violations:** Orphan orders, items, payments, employees, and returns referencing non-existent primary keys.
7. **Unreconciled Payments:** Discrepancies between captured payment amounts and order totals.
8. **Invalid Status Enums:** Status values outside allowed operational enumerations.

---

## 💻 Tech Stack (Module 1)

- **Language:** Python 3.11
- **Compute Engine:** Apache Spark / PySpark 3.5.x
- **Storage Format:** Apache Parquet (Snappy-compressed columnar storage)
- **Data Modeling:** StructType schemas, DecimalType(10, 2) / DecimalType(12, 2) financial precision
- **SQL Engine:** Spark SQL ANSI queries against in-memory temporary views
- **Testing:** Pytest & Pytest-Cov with session-scoped Spark fixtures
- **Linting & Code Quality:** Ruff

---

## 🚀 Setup & Local Execution

### Prerequisites
- **Python:** 3.11+
- **Java Runtime:** OpenJDK 17 (or Java 11). Spark requires a compatible JVM.
  ```bash
  # On macOS via Homebrew:
  brew install openjdk@17
  ```

### 1. Environment Setup
```bash
# Clone repository
git clone https://github.com/vijjureddy07/azure-lakehouse-data-platform.git
cd azure-lakehouse-data-platform

# Create and activate virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements-dev.txt
```

### 2. Generate Synthetic Raw Data
Generate deterministic raw data with injected defects (available in `small` or `standard` scales):
```bash
# Small scale (fast development/testing: ~2k orders, ~5k items)
python -m src.data_generation.generate_retail_data --scale small

# Standard scale (realistic demonstration: 50k customers, 200k orders, 500k+ items)
python -m src.data_generation.generate_retail_data --scale standard
```

### 3. Run the End-to-End Batch Pipeline
```bash
# Run batch pipeline on small scale
python -m src.pipelines.local_batch_pipeline --scale small

# Run batch pipeline on standard scale
python -m src.pipelines.local_batch_pipeline --scale standard
```

### 4. Run Automated Test Suite
```bash
pytest -v
```

---

## 📊 Example Pipeline Execution Output

Below is an actual audit summary generated during a `--scale small` pipeline execution:

```
======================================================================
STARTING AZURE LAKEHOUSE LOCAL BATCH PIPELINE (MODULE 1)
Scale: small | Data Dir: data/raw | Output Dir: output
======================================================================

--- STAGE 1: SYNTHETIC DATA GENERATION ---
Generated source records: {'stores': 10, 'employees': 50, 'customers': 517, 'products': 51, 'orders': 2023, 'order_items': 5055, 'payments': 2023, 'returns': 408}

--- STAGE 2: INITIALIZING LOCAL SPARK SESSION ---
SparkSession active: Spark 3.5.9 (Master: local[*])

--- STAGE 3: SCHEMA-ENFORCED RAW INGESTION ---
Ingested 8 raw source datasets successfully.

--- STAGE 4: CLEANING, TRANSFORMATIONS & DATA QUALITY ENFORCEMENT ---
Customers: 470 valid, 47 quarantined.
Products: 49 valid, 2 quarantined.
Orders: 1879 valid, 144 quarantined.
Order Items: 4738 valid, 317 quarantined.
Payments: 1882 valid, 141 quarantined.
Returns: 388 valid, 20 quarantined.

--- STAGE 5: PERSISTING CLEANED & QUARANTINE PARQUET DATASETS ---
Persisted clean datasets -> output/cleaned/
Persisted quarantine datasets -> output/quarantine/

--- STAGE 6 & 7: ANALYTICAL CURATION & PARTITIONED PARQUET EXPORT ---
Writing curated_sales to Parquet (Partitioned by order_year, order_month)...

--- STAGE 8: REGISTERING SPARK SQL VIEWS & EXECUTING ANALYTICS ---
>>> Executing Spark SQL: daily_monthly_revenue.sql
>>> Executing Spark SQL: top_products_by_revenue.sql
>>> Executing Spark SQL: revenue_by_region.sql
>>> Executing Spark SQL: category_returns_profitability.sql

--- STAGE 9: DATA QUALITY AUDIT RECONCILIATION ---
+-------------+----------------+---------------+--------------------+---------------+---------------------+-------------------------+
|dataset_name |source_row_count|valid_row_count|quarantine_row_count|duplicate_count|null_mandatory_count |referential_orphan_count |
+-------------+----------------+---------------+--------------------+---------------+---------------------+-------------------------+
|stores       |10              |10             |0                   |0              |0                    |0                        |
|employees    |50              |49             |1                   |0              |0                    |1                        |
|customers    |517             |470            |47                  |13             |11                   |0                        |
|products     |51              |49             |2                   |1              |1                    |0                        |
|orders       |2023            |1879           |144                 |20             |0                    |78                       |
|order_items  |5055            |4738           |317                 |0              |0                    |204                      |
|payments     |2023            |1882           |141                 |20             |0                    |42                       |
|returns      |408             |388            |20                  |0              |0                    |20                       |
+-------------+----------------+---------------+--------------------+---------------+---------------------+-------------------------+

======================================================================
PIPELINE EXECUTION COMPLETED SUCCESSFULLY
======================================================================
```

---

## 📖 Learning Documentation

Comprehensive Data Engineering study guides and interview preparation materials are documented in the `docs/` directory:

- [docs/00_LEARNING_INDEX.md](docs/00_LEARNING_INDEX.md): Curriculum roadmap and module status.
- [docs/01_DATA_ENGINEERING_FOUNDATIONS.md](docs/01_DATA_ENGINEERING_FOUNDATIONS.md): Core data engineering concepts (ETL/ELT, idempotency, batch processing, schemas).
- [docs/02_SPARK_PYSPARK_FOUNDATIONS.md](docs/02_SPARK_PYSPARK_FOUNDATIONS.md): Apache Spark architecture, lazy evaluation, DAGs, narrow vs wide transformations, shuffling, and Parquet.
- [docs/03_DATA_QUALITY.md](docs/03_DATA_QUALITY.md): Quality rules, defect injection catalog, and quarantine architecture.
- [docs/04_SPARK_SQL_WINDOWS.md](docs/04_SPARK_SQL_WINDOWS.md): Spark SQL temporary views and PySpark window functions (`ROW_NUMBER`, `DENSE_RANK`, `LAG`, `SUM`).
- [docs/IMPLEMENTATION_MAP.md](docs/IMPLEMENTATION_MAP.md): Skill-to-code traceability matrix.
- [docs/INTERVIEW_QA.md](docs/INTERVIEW_QA.md): Real-world interview questions and concise technical answers.
- [docs/PROGRESS.md](docs/PROGRESS.md): Detailed progress tracker.
