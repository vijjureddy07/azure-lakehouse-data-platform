# Azure Lakehouse Data Platform

End-to-End Enterprise Lakehouse Architecture on Microsoft Azure.

## Overview
This repository contains the architecture, data pipelines, infrastructure as code, and analytical models for an enterprise Azure Data Lakehouse platform leveraging Medallion Architecture (Bronze -> Silver -> Gold).

## Tech Stack
- **Cloud Provider:** Microsoft Azure
- **Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2)
- **Data Processing / Engine:** Azure Databricks / Apache Spark / Delta Lake
- **Orchestration:** Azure Data Factory (ADF) / Databricks Workflows
- **Transformation / Data Modeling:** PySpark / SQL / dbt-databricks
- **Serving / BI:** Azure Synapse Analytics / Power BI
- **Infrastructure / CI/CD:** Terraform / GitHub Actions
- **Governance & Catalog:** Microsoft Purview / Unity Catalog

## Architecture
- **Bronze Layer (Raw):** Ingestion of raw transactional, batch, and streaming datasets in native formats/Delta.
- **Silver Layer (Cleaned & Conformed):** Enriched, cleansed, validated, and conformed data structures.
- **Gold Layer (Aggregated / Business-Ready):** Star schema / dimensional models optimized for reporting and analytics.
