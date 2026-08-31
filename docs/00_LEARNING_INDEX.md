# Azure Lakehouse Data Platform — Learning Roadmap & Index

> **Workflow Rule**: BUILD FIRST → DOCUMENT EVERYTHING → LEARN LATER  
> This roadmap separates engineering build milestones from personal study status.

---

## 🗺️ Curriculum Structure & Status Tracker

| Module | Core Scope & Technology Focus | Build Status | Learning Status |
| :--- | :--- | :--- | :--- |
| **Module 1** | **Local PySpark Data Engineering & Data Quality**<br>*(Python, PySpark, Explicit StructType, Ingestion, Transformations, Cleaning, Joins, Window Functions, Spark SQL, Parquet, Data Quality & Quarantine)* | **COMPLETE** | ⏳ **NOT STUDIED / PENDING** |
| **Module 2** | **Cloud Ingestion: ADF & ADLS Gen2**<br>*(Azure Data Factory Pipelines, Linked Services, Datasets, ADLS Gen2 Hierarchical Namespace, RBAC, Managed Identity)* | **COMPLETE (Deployment-Ready)**<br>*(Cloud Verification Pending)* | ⏳ **NOT STUDIED / PENDING** |
| **Module 3** | **Azure Databricks, Delta Lake & Medallion Architecture**<br>*(Databricks Clusters, Delta Lake ACID Transactions, Time Travel, Bronze / Silver / Gold Medallion Lakehouse)* | ⏹️ NOT STARTED | ⏳ **NOT STUDIED / PENDING** |
| **Module 4** | **Advanced PySpark, Dimensional Modeling & Enterprise Data Quality**<br>*(Star Schema, Facts & Dimensions, SCD Type 1 & 2, Data Quality Frameworks, Schema Evolution)* | ⏹️ NOT STARTED | ⏳ **NOT STUDIED / PENDING** |
| **Module 5** | **Orchestration, Databricks Workflows & Platform Monitoring**<br>*(Databricks Asset Bundles / Workflows, Azure Monitor, Log Analytics, Pipeline Alerts)* | ⏹️ NOT STARTED | ⏳ **NOT STUDIED / PENDING** |
| **Module 6** | **End-to-End Testing, CI/CD & Final Serving Architecture**<br>*(GitHub Actions CI/CD, Unit/Integration Test Suites, Synapse Serverless / Power BI DirectLake)* | ⏹️ NOT STARTED | ⏳ **NOT STUDIED / PENDING** |

---

## 📚 Curriculum Documentation Guide

- [01_DATA_ENGINEERING_FOUNDATIONS.md](01_DATA_ENGINEERING_FOUNDATIONS.md): Core data engineering paradigms, ETL vs ELT, batch processing, idempotency, and full refresh vs incremental loads.
- [02_SPARK_PYSPARK_FOUNDATIONS.md](02_SPARK_PYSPARK_FOUNDATIONS.md): Apache Spark architecture (Driver/Executor), Lazy Evaluation, DAG execution, Transformations vs Actions, Wide vs Narrow dependencies, Shuffling, and Partitioning.
- [03_DATA_QUALITY.md](03_DATA_QUALITY.md): Intentional defect injection, schema enforcement, business key validation, referential integrity via anti-joins, standardized quarantine routing, and reconciliation metrics.
- [04_SPARK_SQL_WINDOWS.md](04_SPARK_SQL_WINDOWS.md): Temporary views, Spark SQL analytics, and Window Functions (`ROW_NUMBER`, `DENSE_RANK`, `LAG`, running sums).
- [05_ADF_ADLS_CLOUD_INGESTION.md](05_ADF_ADLS_CLOUD_INGESTION.md): Azure Data Factory, ADLS Gen2 with Hierarchical Namespace, Managed Identity, Azure RBAC, and parameterized master-child orchestration.
- [IMPLEMENTATION_MAP.md](IMPLEMENTATION_MAP.md): Skill-to-code mapping tracing requirements from domain to implementation functions and Parquet outputs.
- [INTERVIEW_QA.md](INTERVIEW_QA.md): Comprehensive interview questions and expected technical answers covering all Module 1 & 2 topics.
- [PROGRESS.md](PROGRESS.md): Detailed tracking table for build, test, and study milestones.
