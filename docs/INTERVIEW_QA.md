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

