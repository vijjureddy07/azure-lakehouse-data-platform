# Project Progress & Status Tracker

> **Workflow Rule:** BUILD FIRST → DOCUMENT EVERYTHING → LEARN LATER  
> Learning status remains **NOT STUDIED / PENDING** until all portfolio builds are complete and dedicated study begins.

---

## 📊 Summary Status Board

| Milestone | Build Status | Test Status | Learning Status |
| :--- | :--- | :--- | :--- |
| **Module 1: Local PySpark & Quality Framework** | 🟢 **COMPLETE** | 🟢 **PASSED (100%)** | ⏳ **NOT STUDIED / PENDING** |
| **Module 2: ADF + ADLS Gen2 Cloud Ingestion** | ⏹️ NOT STARTED | ⏹️ NOT STARTED | ⏳ **NOT STUDIED / PENDING** |
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
