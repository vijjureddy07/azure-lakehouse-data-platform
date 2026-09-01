# Interview Questions & Technical Answers (Module 1)

> **Workflow Rule:** BUILD FIRST → DOCUMENT EVERYTHING → LEARN LATER  
> Curated Data Engineering and PySpark interview questions mapped to this project's implementation.

---

### Q1: Why do we use PySpark over native Python/Pandas for this data platform?
**Answer:**  
Pandas loads the entire dataset into the memory of a single machine core, which causes Out-Of-Memory (OOM) failures when scaling beyond single-machine RAM. PySpark operates as a distributed computation engine that can scale across multi-core machines locally (`local[*]`) or thousands of worker nodes in a cloud cluster (e.g. Databricks/Synapse). Furthermore, PySpark utilizes the Catalyst Optimizer and Tungsten engine for optimized memory management, off-heap data processing, and query plan execution.

---

### Q2: What is the difference between Spark Transformations and Actions? Give examples from Module 1.
**Answer:**  
- **Transformations** (e.g., `filter()`, `select()`, `withColumn()`, `join()`, `groupBy()`) are **lazy**. They do not compute immediately but instead build an execution DAG (Directed Acyclic Graph) of logical operations.
- **Actions** (e.g., `count()`, `show()`, `write.parquet()`, `collect()`) trigger the physical computation of the DAG across executors to return a result or write data to a storage sink.  
*In Module 1:* All string trimming and anti-joins in `src/transformations/` are transformations; the pipeline only computes data when calling `df.write.parquet()` or `df.count()`.

---

### Q3: Why is `inferSchema=True` avoided in production PySpark pipelines?
**Answer:**  
1. **Performance Overhead:** `inferSchema` forces Spark to trigger an additional full scan across all input files to sample and infer data types, doubling read I/O.
2. **Schema Instability:** If a batch arrives with missing data or edge cases, automatic inference can unpredictably alter column types (e.g. string vs long vs double), breaking downstream queries.
3. **Financial Precision:** Automatic inference defaults numbers to DoubleType (floating point), which introduces rounding errors for currency. Defining an explicit `StructType` with `DecimalType(10, 2)` guarantees exact monetary precision.

---

### Q4: Explain the difference between Narrow and Wide transformations in Spark.
**Answer:**  
- **Narrow Transformations:** Each input partition contributes to at most one output partition (e.g. `filter()`, `withColumn()`, `select()`). Computation occurs locally on the executor with zero network data transfer.
- **Wide Transformations:** Multiple input partitions contribute to multiple output partitions, requiring a **Shuffle** (redistributing rows across executors over the network, e.g. `groupBy()`, `join()`, `distinct()`, `orderBy()`). Wide transformations are significantly more expensive and represent the primary performance tuning area in Spark.

---

### Q5: How did you implement referential integrity checks and quarantine routing in PySpark without failing the pipeline?
**Answer:**  
We use `LEFT JOIN` or `LEFT_ANTI` joins between transactional tables (e.g., `orders`, `order_items`) and primary dimension tables (`customers`, `stores`, `products`). Rows with non-matching foreign keys are identified via `col("ref_pk").isNull()`, tagged with a specific `rejection_reason` (e.g., `ORPHAN_CUSTOMER_FK`), serialized into a standardized `QUARANTINE_SCHEMA` (storing the raw row as a JSON string alongside error metadata and ingestion timestamp), and written to `output/quarantine/`. Only valid rows continue to the cleaned layer.

---

### Q6: Why should you use `DecimalType` instead of `FloatType` or `DoubleType` for monetary fields?
**Answer:**  
`FloatType` and `DoubleType` use IEEE 754 floating-point representation, which cannot represent certain base-10 fractions (like 0.10 or 0.01) exactly. As millions of transactions are multiplied and summed, precision drift causes audit discrepancies. `DecimalType(precision, scale)` stores exact fixed-point decimal digits, ensuring 100% mathematical accuracy for pricing, taxes, discounts, and payments.

---

### Q7: What are the differences between `ROW_NUMBER()`, `RANK()`, and `DENSE_RANK()`?
**Answer:**  
- `ROW_NUMBER()` assigns a unique, strictly incrementing integer to every row (1, 2, 3, 4, 5), arbitrarily breaking ties.
- `RANK()` assigns identical ranks to tied rows and **skips** subsequent ranks (e.g. 1, 2, 2, 4, 5).
- `DENSE_RANK()` assigns identical ranks to tied rows and **does not skip** subsequent ranks (e.g. 1, 2, 2, 3, 4).  
*In Module 1:* We use `ROW_NUMBER()` for deterministic customer deduplication and order sequencing, and `DENSE_RANK()` for category product revenue leaderboards.

---

### Q8: What is the purpose of the `rowsBetween()` clause in Window functions?
**Answer:**  
`rowsBetween(start, end)` defines the physical boundary of rows included in the window calculation relative to the current row. For example, `rowsBetween(Window.unboundedPreceding, Window.currentRow)` specifies a cumulative frame starting from the first row of the partition up to the current row, which is essential for computing running totals (e.g. cumulative customer spend).

---

### Q9: Why is Parquet the standard storage format in Lakehouse architectures?
**Answer:**  
1. **Columnar Storage:** Only the columns requested in a query are read from disk, drastically reducing I/O compared to row-based CSV/JSON.
2. **Compression:** Homogeneous column data enables high-ratio compression algorithms (e.g. Snappy, ZSTD).
3. **Predicate Pushdown:** Parquet files store min/max statistics in headers/footers. Spark uses these statistics to skip entire row groups without reading raw data if query filters fall outside the range.

---

### Q10: What is the "Small File Problem" in data lakes, and how do you avoid it?
**Answer:**  
Over-partitioning (e.g., partitioning by high-cardinality keys like `customer_id` or `order_id`) generates millions of tiny files (a few kilobytes each). This degrades performance because the Spark driver and storage file system spend more time listing files and opening/closing I/O handles than reading data. It is avoided by partitioning only by coarse temporal (e.g. `order_year`, `order_month`) or regional dimensions, aiming for file sizes between 128 MB and 1 GB in production.

---

### Q11: How do Temporary Views work in Spark SQL, and do they persist data to disk?
**Answer:**  
`createOrReplaceTempView("view_name")` registers the DataFrame's logical plan in Spark's in-memory session catalog. It does **not** materialize or write data to disk. When a SQL query is executed against the view, Spark parses the SQL into the DataFrame's Catalyst execution plan, executing with the exact same distributed performance as native DataFrame API code. The view is session-scoped and discarded when the SparkSession stops.

---

### Q12: What is pipeline idempotency and how is it implemented in Module 1?
**Answer:**  
Idempotency means re-running a pipeline against the same source input produces the exact same output without side effects, duplicates, or corrupted state. In Module 1, all dataset writers use Spark's `mode("overwrite")` semantics, ensuring the target output directories are cleanly replaced on every batch execution.

---

## Azure Cloud Ingestion (Module 2 Focus)

### Q13: What is the architectural advantage of enabling Hierarchical Namespace (HNS) in ADLS Gen2 for Lakehouse platforms?
**Answer:**  
Standard Azure Blob storage uses a flat object store where directory structures are virtual key prefixes. Renaming a directory requires $O(N)$ copy-and-delete API operations across all blobs. ADLS Gen2 with Hierarchical Namespace (HNS) implements true filesystem directory hierarchies. Renaming or moving a directory becomes an $O(1)$ atomic metadata pointer update. This is critical for big data engines like Spark and Delta Lake, which rely on atomic directory rename operations for job commits, idempotent partition overwrites, and ACID transaction commits.

---

### Q14: How does a metadata-driven ingestion architecture work in Azure Data Factory?
**Answer:**  
Instead of creating dozens of hardcoded pipelines, we use a Master-Child orchestration pattern:
1. **Master Pipeline (`pl_master_retail_ingestion`):** Defines a metadata configuration array specifying dataset parameters (`dataset_name`, `source_relative_url`, `destination_file_name`, format).
2. **ForEach Activity:** Iterates across the metadata array items concurrently.
3. **Child Pipeline (`pl_ingest_single_file`):** A single generic, reusable pipeline containing a parameterized Copy Activity that reads from a parameterized HTTP source dataset and writes directly to a parameterized ADLS Gen2 landing dataset.
When a new dataset needs to be onboarded, we append a JSON object to the metadata configuration without editing visual pipeline canvases.

---

### Q15: Why is Managed Identity with Azure RBAC preferred over Storage Account Keys or SAS Tokens?
**Answer:**  
- **Storage Account Keys:** Grant unrestricted root-level access to the entire storage account, bypass fine-grained access policies, cannot be scoped to individual containers, and introduce credential leakage risks if committed to code repositories.
- **SAS Tokens:** Expire over time, require key vault storage, and demand ongoing manual rotation workflows.
- **System-Assigned Managed Identity:** Leverages Microsoft Entra ID to authenticate ADF directly to ADLS Gen2 with zero stored passwords or keys. The ADF resource is granted the least-privilege role `Storage Blob Data Contributor`. Azure automatically handles token issuance, rotation, and lifecycle management without code intervention.

---

### Q16: When would you use Azure RBAC vs ADLS Gen2 POSIX Access Control Lists (ACLs)?
**Answer:**  
- **Azure RBAC:** Coarse-grained access control applied at the Subscription, Resource Group, Storage Account, or Container level (e.g., granting ADF or Databricks `Storage Blob Data Contributor` across the entire `lakehouse` container).
- **POSIX ACLs:** Fine-grained access control applied directly to specific subdirectories and individual files within an HNS-enabled storage account (e.g., granting Data Science teams read-only access to `lakehouse/curated/features/` while restricting access to `lakehouse/raw/pii/`).

---

### Q17: Why must the raw landing zone preserve source files without transformation?
**Answer:**  
1. **Source Fidelity & Auditability:** Preserving the exact original byte stream (CSV as CSV, JSON as JSON) provides an immutable record of what external upstream systems delivered, enabling compliance verification and historical audit.
2. **Decoupling Transport from Compute:** Ingestion (ADF Copy Activity) is lightweight, fast, and serverless. Heavy computation, schema enforcement, deduplication, and transformations are offloaded to distributed compute engines (Spark / Delta Lake in Module 3).
3. **Reprocessability:** If downstream transformation logic contains a bug or business rules change, having unaltered raw landing data partitioned by `ingestion_date` and `run_id` allows backfilling without requesting re-exports from source providers.

---

## 3. Azure Databricks, Delta Lake & Medallion Architecture (Module 3)

### Q18: What is the architectural difference between Landing files in ADLS Gen2 and Bronze Delta tables?
**Answer:**  
- **Landing Files:** Raw, immutable files (CSV, JSON) sitting in the storage account landing zone (`landing/retail/<dataset>/ingestion_date=*/run_id=*/*`) exactly as emitted by upstream systems. They lack ACID transactions, schema enforcement, and query optimization.
- **Bronze Delta Tables:** Structured Delta Lake tables (`output/delta/bronze/<dataset>/` or `retail_lakehouse.bronze.<dataset>`) that ingest the raw files verbatim (as strings) while appending rich system lineage metadata columns (`_source_file`, `_source_path`, `_ingestion_date`, `_adf_run_id`, `_ingested_timestamp`). Bronze provides ACID transaction isolation, fast columnar query scans, and an ingestion audit trail without altering raw column values.

---

### Q19: How does the Delta Lake transaction log (`_delta_log`) guarantee ACID properties?
**Answer:**  
Delta Lake maintains an ordered sequence of JSON commit files (`00000000000000000000.json`, etc.) in the `_delta_log/` directory:
1. **Atomicity:** When a transaction modifies a table, it writes new Parquet data files and attempts to commit a single JSON log entry containing `add` or `remove` file actions. If any part of the write fails, the commit JSON is not created, meaning readers will never see the partial files.
2. **Consistency:** Readers scan the transaction log to construct the table state. State transitions are strictly governed by schema invariants and constraints.
3. **Isolation (Snapshot Isolation):** Readers construct a snapshot of the table at the exact transaction log version active when their query began, unaffected by concurrent writes.
4. **Durability:** Once the commit file is written to durable cloud object storage (ADLS Gen2), the transaction is permanent. Checkpoint Parquet files are generated every 10 commits to optimize snapshot reconstruction.

---

### Q20: How does Delta Lake implement time travel, and what are its production use cases?
**Answer:**  
Because Delta Lake's transaction log tracks every file added and removed across every commit without immediately deleting underlying Parquet files, users can query past snapshots:
```sql
-- Query by version
SELECT * FROM retail_lakehouse.silver.customers VERSION AS OF 2;

-- Query by timestamp
SELECT * FROM retail_lakehouse.silver.customers TIMESTAMP AS OF '2026-08-31 10:00:00';
```
**Production Use Cases:**
1. **Auditing & Reproducibility:** Re-running financial or ML models against the exact data state from the prior month or quarter.
2. **Rollback & Disaster Recovery:** Restoring a table after an accidental write or bad data deployment using `RESTORE TABLE ... TO VERSION AS OF N`.
3. **Debugging & Root Cause Analysis:** Comparing data before and after an ETL job failure to isolate defective incoming records.

---

### Q21: Explain the difference between Delta Lake Schema Enforcement and Schema Evolution.
**Answer:**  
- **Schema Enforcement (Schema Validation):** The default safety mechanism in Delta Lake. If an incoming DataFrame has columns not defined in the target table schema or has conflicting data types, Delta throws an `AnalysisException` and rejects the write. This prevents accidental schema corruption and column pollution.
- **Schema Evolution:** Explicitly opted-in capability allowing Delta to alter its schema dynamically during a write operation. Enabled by setting `.option("mergeSchema", "true")` during an append/overwrite or `spark.databricks.delta.schema.autoMerge.enabled = true`. Newly added columns in the DataFrame are automatically appended to the table schema with null values populated for historical records.

---

### Q22: How does Delta MERGE provide idempotency compared to simple APPEND or OVERWRITE?
**Answer:**  
- **Append:** Adds all incoming rows unconditionally. Rerunning a pipeline causes duplicated records.
- **Overwrite:** Replaces the entire table or partition. While idempotent, it cannot perform row-level incremental updates without rewriting the whole partition.
- **Delta MERGE (Upsert):** Evaluates a match condition on primary keys (`ON target.id = source.id`). When matched, it updates existing records in place; when not matched, it inserts new records; and unreferenced records remain untouched. Rerunning the exact same source dataset on an already updated table matches all keys, updates with identical values, inserts 0 new rows, and produces 0 duplicates.

---

### Q23: How does Unity Catalog enable enterprise data governance with zero stored credentials?
**Answer:**  
Unity Catalog introduces a central 3-level namespace (`<catalog>.<schema>.<table>`) across all Databricks workspaces. Rather than embedding storage access keys or SAS tokens in notebooks or using legacy DBFS mounts:
1. An **Azure Databricks Access Connector** is deployed with a System-Assigned Managed Identity in Microsoft Entra ID.
2. The Access Connector is granted the Azure RBAC role **Storage Blob Data Contributor** on ADLS Gen2.
3. In Unity Catalog, an administrator creates a **Storage Credential** pointing to the Access Connector resource ID, and an **External Location** pointing to the ADLS Gen2 container URL (`abfss://...`).
4. Data engineers and analysts query tables using standard ANSI SQL permissions (`GRANT SELECT ON TABLE...`) without having direct storage keys or seeing cloud connection strings.

---

## 4. Advanced PySpark, Dimensional Modeling & Slowly Changing Dimensions (Module 4)

### Q24: What are surrogate keys, and why should you avoid `monotonically_increasing_id()` in distributed Spark data warehouses?
**Answer:**  
- **Surrogate Keys:** Artificial, single-column integer keys generated by the data warehouse (e.g. `customer_key = 101`) to uniquely identify dimension records independently of natural business keys (`customer_id = 'C-001'`). They isolate the warehouse from upstream natural key changes, enable SCD Type 2 historical versioning, and optimize join performance in columnar analytical databases.
- **Why Avoid `monotonically_increasing_id()`:** Spark's `monotonically_increasing_id()` generates 64-bit non-contiguous integers with wide gaps where the upper 33 bits encode the partition ID. Because partition assignments and execution planning change dynamically based on cluster size and shuffles, this function is **non-deterministic** across pipeline runs.
- **The Correct Pattern:** Allocate deterministic surrogate keys by reading the existing maximum key from the target Delta table (`max_existing_key`) and adding `ROW_NUMBER() OVER (ORDER BY natural_key)`.

---

### Q25: Explain the difference between SCD Type 1 and SCD Type 2 with concrete retail examples.
**Answer:**  
- **SCD Type 1 (In-Place Overwrite):** Directly overwrites previous attribute values in the dimension table without preserving history.
  - *Example:* Fixing a typo in a customer's last name or updating a product's subcategory in `dim_product`.
  - *Implementation in Delta:* Executed via `DeltaTable.merge()` matching on natural key `product_id` and updating attributes in place while preserving the original `product_key`.
- **SCD Type 2 (Historical Versioning):** Inserts a new dimension row whenever tracked attributes change, establishing non-overlapping validity intervals (`[effective_from, effective_to)`), a current record flag (`is_current`), and an incremented version number (`version_number`).
  - *Example:* A customer moving from Texas to California or upgrading from `GOLD` to `PLATINUM` loyalty tier in `dim_customer`.
  - *Implementation in Delta:* Compute SHA-256 attribute hash over tracked columns, left join incoming records against current active records (`is_current = true`), expire matched records with `effective_to = now, is_current = false` via Delta MERGE, and append new version records.

---

### Q26: Why is joining fact transactions to `is_current = true` in SCD Type 2 dimensions an architectural error?
**Answer:**  
Joining fact records to the active dimension record (`is_current = true`) causes **historical misattribution**:
- *The Flaw:* If an order was placed in February 2026 when the customer was in the `SILVER` loyalty tier, and the customer upgraded to `PLATINUM` in June 2026, joining February orders to `is_current = true` attributes February sales to the `PLATINUM` tier, corrupting historical performance reports.
- *The Solution (Point-in-Time Fact Resolution):* Join the fact table using the transaction's event timestamp against the dimension's historical validity interval:
```sql
ON fact.customer_id = dim.customer_id
AND fact.order_timestamp >= dim.effective_from
AND (fact.order_timestamp < dim.effective_to OR dim.effective_to IS NULL)
```

---

### Q27: How do you handle Late-Arriving Dimensions or Orphan Foreign Keys in fact tables without dropping rows?
**Answer:**  
Use the **Unknown Member Pattern (Surrogate Key 0)**:
1. Every dimension table is initialized with an "Unknown" record having surrogate key `0` (e.g. `dim_customer` has `customer_key = 0, customer_id = 'UNKNOWN', loyalty_tier = 'UNKNOWN'`; `dim_date` has `date_key = 0, full_date = 1900-01-01`).
2. When resolving foreign keys during fact table creation, wrap the lookup in `COALESCE(dim.surrogate_key, 0)`.
3. If an incoming transaction references a customer or product that has not yet been processed in the dimension table, the fact row is preserved with foreign key `0` rather than being dropped by an inner join or inserting a `NULL` foreign key.
4. When the late-arriving dimension record is subsequently ingested, an update process can backfill the foreign key from `0` to the allocated surrogate key.

---

### Q28: What are Enterprise Data Quality Gates, and how do they differ from simple data cleaning?
**Answer:**  
- **Data Cleaning:** Transforming, formatting, and standardizing data during ingestion and Silver conformance (e.g. regex trimming, date formatting).
- **Data Quality Gates:** Automated, programmatic validation suites executed prior to publishing data to the warehouse or serving layer. They validate 6 core pillars:
  1. *Completeness:* Enforcing non-null surrogate keys, grain keys, and essential measures.
  2. *Uniqueness:* Enforcing 0 duplicate primary keys on dimensions and 1 row per grain on facts.
  3. *Referential Integrity:* Proving 0 orphan foreign keys exist across all fact-dimension relationships.
  4. *SCD2 Temporal Invariants:* Proving exactly one active version exists per natural key with non-overlapping half-open intervals.
  5. *Measure Validity:* Enforcing business rules (`gross_amount >= net_amount`, `quantity > 0`, `profit_amount = net_amount - cost_amount`).
  6. *Reconciliation:* Verifying mathematical and financial equality between source and target layers.
- If a `CRITICAL` gate fails, the pipeline raises `WarehouseQualityGateError` and terminates immediately, logging audit records to `delta/warehouse/quality_audit`.

---

### Q29: What is the difference between Star Schema and Snowflake Schema, and why is Star Schema preferred in Delta Lake?
**Answer:**  
- **Star Schema:** Completely denormalizes dimension tables around a central fact table. Each dimension is represented by a single table (e.g. `dim_product` contains `category`, `subcategory`, and `brand` in one flat row).
- **Snowflake Schema:** Normalizes dimension tables into multiple related sub-tables (e.g. `dim_product` joins to `dim_subcategory`, which joins to `dim_category`).
- **Why Star Schema is Preferred in Delta Lake / Lakehouse:**
  1. *Join Minimization:* Distributed joins across multiple tables cause expensive network shuffles in Apache Spark. Star schemas minimize joins to a single hop between facts and dimensions.
  2. *Columnar Scan Efficiency:* Delta Lake's columnar Parquet storage and file skipping (Data Skipping / Z-Order) make wide denormalized tables highly efficient to scan and compress.
  3. *BI Performance:* Modern analytical tools (Power BI DirectLake, Azure Synapse) are architected specifically to generate optimized queries against flat star schema dimensions.

---

### Q30: How do you prove 100% financial and row-count reconciliation between conformed Silver tables and the Warehouse Fact layer?
**Answer:**  
In `reconcile_warehouse_sales()`, compute aggregates on the source (`silver_order_items` joined to `silver_orders`) and target (`fact_sales`):
1. **Row Count:** Confirm `COUNT(eligible_silver_order_items) == COUNT(fact_sales)`.
2. **Gross Amount:** Confirm `SUM(silver_gross) == SUM(fact_gross)` using `DecimalType(10, 2)` (0 diff).
3. **Discount Amount:** Confirm `SUM(silver_discount) == SUM(fact_discount)` (0 diff).
4. **Net Amount:** Confirm `SUM(silver_net) == SUM(fact_net)` (0 diff).
If any absolute difference is greater than `0.00`, raise `ValueError` and halt the pipeline.



