# 01. Data Engineering Foundations (Module 1 Study Guide)

> **Status:** Build Complete | **Learning Status:** ⏳ NOT STUDIED / PENDING

---

## 1. Data Engineer

### WHAT IT IS
A Data Engineer designs, builds, tests, and maintains automated data processing systems, pipelines, and storage architectures that transform raw, unformatted, or distributed data into clean, validated, reliable data models for analytics, machine learning, and business decision-making.

### WHY IT MATTERS
Raw data from transaction systems, third-party APIs, and IoT devices is messy, unformatted, corrupted, and distributed. Without reliable data engineering, analytical reports deliver flawed numbers, and machine learning models fail in production.

### WHERE WE USE IT IN THIS PROJECT
Across all modules, defining data models, schemas, ingestion pipelines, quality validation rules, and transformation logic.

### EXACT FILE / FUNCTION
- [local_batch_pipeline.py](../src/pipelines/local_batch_pipeline.py) (`LocalBatchPipeline.run()`)

### SMALL EXAMPLE
```python
# Ingest raw data, apply quality rules, route invalid records to quarantine, write clean Parquet
clean_df, quarantine_df, metric = transform_customers(raw_customers_df)
clean_df.write.mode("overwrite").parquet("output/cleaned/customers")
```

### INTERVIEW QUESTION
> "What is the primary responsibility of a Data Engineer in an enterprise lakehouse ecosystem?"

### EXPECTED ANSWER
> "A Data Engineer is responsible for building scalable, reliable, and idempotent data pipelines that ingest raw data from diverse sources, enforce data quality and schema validation, transform and model the data for analytical workloads, and persist it in optimized storage formats (such as Parquet or Delta Lake) while maintaining data governance, observability, and auditability."

---

## 2. Data Pipeline

### WHAT IT IS
An automated, repeatable sequence of software steps that extracts data from one or more source systems, transforms it (cleaning, filtering, enriching, aggregating), and loads it into target sinks.

### WHY IT MATTERS
Manual data processing is error-prone, unscalable, and cannot meet business SLAs for timeliness and reliability. Pipelines provide repeatable execution and fault isolation.

### WHERE WE USE IT IN THIS PROJECT
In Module 1's end-to-end local batch pipeline runner that orchestrates data generation, ingestion, quality checks, curation, SQL queries, and metrics export.

### EXACT FILE / FUNCTION
- [local_batch_pipeline.py](../src/pipelines/local_batch_pipeline.py) (`LocalBatchPipeline.run()`)

### SMALL EXAMPLE
```python
pipeline = LocalBatchPipeline(scale="small")
result = pipeline.run()
```

### INTERVIEW QUESTION
> "What are the core components of a production-grade data pipeline?"

### EXPECTED ANSWER
> "A production-grade data pipeline consists of: (1) Source connectors/readers, (2) Schema enforcement and data quality validation layers, (3) Quarantine/error routing mechanisms, (4) Transformation and business logic engines, (5) Sinks/storage writers with deterministic write modes, (6) Observability and audit metric collectors, and (7) An orchestrator managing dependencies, retries, and alerting."

---

## 3. Source & Sink

### WHAT IT IS
- **Source:** The origin system or file storage where raw data is produced or extracted from.
- **Sink:** The destination system, database, or file path where processed data is loaded or written to.

### WHY IT MATTERS
Decoupling sources and sinks allows pipelines to be modular and testable, swapping storage media (e.g., local disk during Module 1 development vs ADLS Gen2 in later cloud modules) without rewriting core transformation logic.

### WHERE WE USE IT IN THIS PROJECT
- Sources: CSV files (`customers.csv`, `orders.csv`, etc.) and JSON lines (`payments.json`) in `data/raw/`.
- Sinks: Columnar Parquet directories in `output/cleaned/`, `output/quarantine/`, `output/curated/`, and `output/metrics/`.

### EXACT FILE / FUNCTION
- [local_batch_pipeline.py](../src/pipelines/local_batch_pipeline.py) (`_ingest_raw_data` & `_write_cleaned_and_quarantine`)

### SMALL EXAMPLE
```python
# Source read
raw_df = spark.read.schema(RAW_CUSTOMERS_SCHEMA).csv("data/raw/customers.csv")
# Sink write
clean_df.write.mode("overwrite").parquet("output/cleaned/customers")
```

### INTERVIEW QUESTION
> "How do you handle schema mismatches between a high-throughput source and an analytical sink?"

### EXPECTED ANSWER
> "By implementing explicit schema definition on source read rather than relying on automatic inference, validating incoming records against target contracts, routing non-conforming rows to a dedicated quarantine sink with detailed error metadata, and writing only sanitized, conformed data to the primary analytical sink."

---

## 4. ETL vs ELT

### WHAT IT IS
- **ETL (Extract, Transform, Load):** Data is extracted from sources, transformed in-flight in an external compute/processing layer (such as Spark or an ETL server), and then loaded into the target storage.
- **ELT (Extract, Load, Transform):** Data is extracted and loaded directly in raw format into a scalable storage engine (like a Data Lake or Cloud Warehouse), and then transformed using the processing power of the target platform (e.g., Databricks/Delta Live Tables or dbt/Snowflake).

### WHY IT MATTERS
ETL prevents dirty raw data from landing in target databases but requires dedicated compute outside the database. ELT preserves raw historical fidelity in the lake (Bronze layer), allowing reprocessing and schema evolution at any time.

### WHERE WE USE IT IN THIS PROJECT
Module 1 follows the Lakehouse ETL/ELT hybrid: Raw files are read into PySpark compute, standardized and cleaned, written as clean Parquet, and then transformed into curated business datasets.

### EXACT FILE / FUNCTION
- [sales.py](../src/transformations/sales.py) (`build_curated_sales`)

### SMALL EXAMPLE
```python
# Extract (read cleaned tables) -> Transform (joins, financials, window ranks) -> Load (curated Parquet)
curated_df = build_curated_sales(clean_orders, clean_items, clean_prods, clean_cust, clean_stores)
curated_df.write.mode("overwrite").partitionBy("order_year", "order_month").parquet("output/curated/curated_sales")
```

### INTERVIEW QUESTION
> "When would you choose ETL over ELT, or vice versa, in a modern cloud data platform?"

### EXPECTED ANSWER
> "ELT is preferred when you have a scalable cloud storage and compute platform (like ADLS Gen2 + Databricks or Snowflake), because storing raw data first ensures no data loss and enables re-running transformations as business logic evolves. ETL is preferred when raw data contains strict PII/compliance restrictions that cannot land unmasked in the lake, or when source payloads must be converted into columnar formats before entering the analytical platform."

---

## 5. Batch Processing

### WHAT IT IS
Processing data in discrete, scheduled blocks or chunks (batches) rather than continuously row-by-row in real-time.

### WHY IT MATTERS
Batch processing is computationally efficient for high-throughput historical, daily, or hourly workloads. It allows deep aggregations, multi-table joins, and global sorting across massive datasets at lower cost than continuous streaming.

### WHERE WE USE IT IN THIS PROJECT
The Module 1 pipeline processes the retail dataset as a discrete batch job.

### EXACT FILE / FUNCTION
- [local_batch_pipeline.py](../src/pipelines/local_batch_pipeline.py)

### SMALL EXAMPLE
```python
# Batch execution runs over all files in the batch partition
spark.read.options(header="true").schema(RAW_ORDERS_SCHEMA).csv("data/raw/orders.csv")
```

### INTERVIEW QUESTION
> "What are the trade-offs between Batch Processing and Stream Processing?"

### EXPECTED ANSWER
> "Batch processing offers maximum throughput, lower compute cost, easier error recovery, and simpler state management, but has latency (minutes to hours). Stream processing offers near-zero latency (seconds or sub-seconds) for real-time alerts or immediate analytics, but requires higher infrastructure cost, complex stateful processing, and specialized handling for out-of-order data."

---

## 6. Data Lake vs Data Warehouse vs Lakehouse

### WHAT IT IS
- **Data Lake:** Low-cost, scalable object storage (e.g., ADLS Gen2, AWS S3) storing raw, semi-structured, and unstructured data files (Parquet, JSON, CSV). Lacks ACID transactions and indexing natively.
- **Data Warehouse:** Relational database optimized for structured BI queries (e.g., SQL Data Warehouse, Synapse Dedicated Pool, Snowflake). Fast SQL, ACID transactions, but expensive and rigid.
- **Lakehouse:** Modern architecture that combines the cost-effective scalability of a Data Lake with the reliability, ACID transactions, schema enforcement, and indexing of a Data Warehouse directly on open file formats (such as Delta Lake or Apache Iceberg).

### WHY IT MATTERS
Lakehouse eliminates dual-system architectures where data engineers had to copy data from data lakes to proprietary data warehouses, reducing data duplication, latency, and ETL maintenance costs.

### WHERE WE USE IT IN THIS PROJECT
Module 1 sets up the foundations of the Lakehouse architecture (Raw -> Cleaned -> Curated Parquet layers), preparing for Delta Lake ACID features in Module 3.

### EXACT FILE / FUNCTION
- [retail_schemas.py](../src/schemas/retail_schemas.py)
- [sales.py](../src/transformations/sales.py)

### INTERVIEW QUESTION
> "Why is the Lakehouse architecture replacing traditional two-tier Data Lake + Data Warehouse architectures?"

### EXPECTED ANSWER
> "Traditional two-tier architectures suffered from data staleness, high storage costs from copying data, brittle synchronization ETL pipelines between the lake and warehouse, and inconsistent governance. A Lakehouse applies transactional metadata layers (like Delta Lake) directly on open storage (like ADLS Gen2), enabling both high-performance BI SQL and scalable Machine Learning on a single copy of data."

---

## 7. Structured vs Semi-Structured Data

### WHAT IT IS
- **Structured Data:** Data organized into rigid, tabular schemas with fixed rows and columns (e.g., Relational CSV, SQL tables).
- **Semi-Structured Data:** Data with organizational structure and tags/keys, but without fixed schema rigidity; may contain nested objects, arrays, or optional keys (e.g., JSON, JSON Lines, XML).

### WHY IT MATTERS
Modern enterprise platforms must ingest both tabular transactional tables and flexible JSON event streams.

### WHERE WE USE IT IN THIS PROJECT
- Structured: CSV tables for customers, products, stores, employees, orders, order items, returns.
- Semi-Structured: JSON Lines for payments (`payments.json`).

### EXACT FILE / FUNCTION
- [generate_retail_data.py](../src/data_generation/generate_retail_data.py) (`_write_json`)
- [retail_schemas.py](../src/schemas/retail_schemas.py) (`RAW_PAYMENTS_SCHEMA`)

### SMALL EXAMPLE
```python
# Reading semi-structured JSON lines with an explicit schema
payments_df = spark.read.schema(RAW_PAYMENTS_SCHEMA).json("data/raw/payments.json")
```

### INTERVIEW QUESTION
> "How does PySpark handle parsing nested semi-structured JSON payloads into tabular formats?"

### EXPECTED ANSWER
> "PySpark parses JSON using explicit `StructType` containing nested `StructType` or `ArrayType` fields. Alternatively, if a JSON string resides in a column, PySpark provides the `from_json()` function combined with an explicit schema, allowing extraction of nested elements using dot notation (e.g. `col.field.subfield`) or `explode()` for arrays."

---

## 8. Schema & Schema Enforcement

### WHAT IT IS
Explicit definition of column names, data types, and nullability constraints (`StructType` and `StructField` in PySpark). Schema enforcement guarantees that incoming files strictly adhere to this contract on read/write.

### WHY IT MATTERS
Relying on `inferSchema=true` causes severe performance penalties (Spark must perform a full extra scan over the entire dataset just to guess data types) and introduces production bugs when types change between batches (e.g. integer vs string).

### WHERE WE USE IT IN THIS PROJECT
All 8 raw datasets are ingested using explicit `StructType` definitions in `src/schemas/retail_schemas.py`.

### EXACT FILE / FUNCTION
- [retail_schemas.py](../src/schemas/retail_schemas.py)
- [local_batch_pipeline.py](../src/pipelines/local_batch_pipeline.py)

### SMALL EXAMPLE
```python
RAW_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("unit_price", StringType(), True),
    ...
])
```

### INTERVIEW QUESTION
> "Why should `inferSchema` never be used in production Spark pipelines?"

### EXPECTED ANSWER
> "First, `inferSchema` forces Spark to trigger an extra full-pass action over the input files to sample data types, doubling ingestion I/O. Second, if a batch arrives with missing data or different sample rows, `inferSchema` may infer an incompatible type (e.g., IntegerType instead of DoubleType or StringType), causing runtime failures in downstream queries. Defining an explicit `StructType` ensures deterministic execution and zero extra I/O."

---

## 9. Data Quality & Quarantine Pattern

### WHAT IT IS
A pattern where corrupted, invalid, malformed, duplicate, or orphan records are separated from valid records, augmented with audit metadata (`rejection_reason`, `source_dataset`, `ingested_at`), and persisted to an isolated **Quarantine** sink for triage, rather than failing the entire pipeline or silently dropping data.

### WHY IT MATTERS
Silent drops lead to lost business revenue and unexplainable discrepancies. Pipeline crashes stop all business operations. Quarantine isolates bad records while allowing good records to proceed immediately to downstream reporting.

### WHERE WE USE IT IN THIS PROJECT
Implemented across all transformations (`customers.py`, `products.py`, `orders.py`), outputting to `output/quarantine/` in a uniform `QUARANTINE_SCHEMA`.

### EXACT FILE / FUNCTION
- [rules.py](../src/quality/rules.py) (`format_as_quarantine`)
- [customers.py](../src/transformations/customers.py)

### SMALL EXAMPLE
```python
clean_df = classified_df.filter(col("rejection_reason").isNull())
invalid_df = classified_df.filter(col("rejection_reason").isNotNull())
quarantine_df = format_as_quarantine(invalid_df, "customer_id", "customers", "rejection_reason")
```

### INTERVIEW QUESTION
> "How do you design a robust quarantine pattern in a PySpark ETL pipeline?"

### EXPECTED ANSWER
> "In PySpark, we evaluate data quality rules using `when().otherwise()` expressions or anti-joins, creating a `rejection_reason` column. We split the DataFrame into clean rows (`rejection_reason IS NULL`) and quarantined rows (`rejection_reason IS NOT NULL`). Quarantined rows are transformed into a standardized audit schema containing the record ID, source dataset, rejection reason, JSON serialization of the raw payload, and ingestion timestamp, and written to an isolated quarantine storage location."

---

## 10. Idempotency

### WHAT IT IS
A property of an operation or pipeline where running it multiple times with the same input produces the exact same output without unintended side effects (such as duplicate rows or corrupted state).

### WHY IT MATTERS
In production, pipelines fail due to network drops, cluster preemptions, or infrastructure issues. If a pipeline is idempotent, engineers can safely retry or re-run it without manual data cleanup.

### WHERE WE USE IT IN THIS PROJECT
All Parquet writes in Module 1 use deterministic `mode("overwrite")` semantics, ensuring re-running the batch against the same input yields identical results.

### EXACT FILE / FUNCTION
- [local_batch_pipeline.py](../src/pipelines/local_batch_pipeline.py) (`.write.mode("overwrite").parquet(...)`)

### SMALL EXAMPLE
```python
# Idempotent write: overwrites existing output directory cleanly
curated_sales_df.write.mode("overwrite").partitionBy("order_year", "order_month").parquet("output/curated/curated_sales")
```

### INTERVIEW QUESTION
> "How do you achieve idempotency in batch data pipelines?"

### EXPECTED ANSWER
> "In full refresh batches, idempotency is achieved by writing to target sinks using atomic overwrite semantics (`mode('overwrite')`). In incremental partitioned batch pipelines, idempotency is achieved by dynamic partition overwrites (overwriting only the specific date/time partitions being processed) or using Delta Lake `MERGE` (upsert) operations with unique business keys."

---

## 11. Full Refresh vs Incremental Load

### WHAT IT IS
- **Full Refresh:** The entire target dataset is completely rewritten from scratch on every pipeline run.
- **Incremental Load:** Only new, modified, or deleted records since the previous watermark/checkpoint are ingested and updated in the target sink.

### WHY IT MATTERS
Full refresh is simpler, eliminates state synchronization bugs, and is suitable for small/medium reference tables or dev/test environments. Incremental loading is essential for high-volume fact tables where rewriting terabytes of data daily is cost-prohibitive.

### WHERE WE USE IT IN THIS PROJECT
Module 1 intentionally implements **Full Refresh** with overwrite semantics for local PySpark development. Incremental loading (CDC/Watermarking/Delta MERGE) is part of subsequent modules.

### EXACT FILE / FUNCTION
- [local_batch_pipeline.py](../src/pipelines/local_batch_pipeline.py)

### INTERVIEW QUESTION
> "What are the main methods used to track and extract incremental data from source systems?"

### EXPECTED ANSWER
> "Common methods include: (1) **Timestamp/Watermark tracking:** Querying records where `updated_at > last_processed_watermark`, (2) **Change Data Capture (CDC):** Reading database transaction logs (e.g. Debezium, Azure SQL CDC), (3) **Auto-incrementing IDs:** Filtering `id > last_max_id` for append-only tables, and (4) **File notification / Delta Change Data Feed:** Ingesting only newly created cloud storage files or Delta commit versions."
