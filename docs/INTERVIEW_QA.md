# Interview Questions & Technical Answers (Modules 1–6)

> **Workflow Rule:** BUILD FIRST → DOCUMENT EVERYTHING → LEARN LATER  
> Curated Data Engineering, Lakehouse, and Cloud Architecture interview questions mapped to this project's implementation.

---

## PySpark & Data Engineering Foundations (Module 1 Focus)

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
1. **Master Pipeline (`pl_master_retail_ingestion`):** Contains a parameterized `ForEach` activity that iterates over a metadata JSON array defining dataset names and source paths for all 8 entities.
2. **Child Pipeline (`pl_ingest_single_file`):** A generic, reusable pipeline containing a binary `Copy Activity` parameterized with `@dataset().file_name` and `@dataset().folder_path`.
3. **Adding New Datasets:** New datasets can be onboarded solely by updating the metadata configuration array without modifying the visual pipeline canvas or redeploying ADF assets.

---

### Q15: Why is Managed Identity with Azure RBAC preferred over Storage Account Access Keys?
**Answer:**  
1. **Zero Hardcoded Secrets:** Managed Identities authenticate directly against Azure Entra ID (formerly Azure AD). No shared keys or SAS tokens exist in version control, ARM templates, or ADF linked services.
2. **Least Privilege Principle:** Azure RBAC allows granting scoped permissions (e.g., `Storage Blob Data Contributor` specifically on the `lakehouse` container) without exposing account-wide administrative master keys that could delete or modify unrelated resources.
3. **Automated Credential Rotation:** Azure handles certificate rotation and identity token lifecycle automatically, eliminating manual secret rotation outages.

---

### Q16: How do you guarantee raw file fidelity during ADF ingestion into ADLS Gen2?
**Answer:**  
In the child pipeline's Copy Activity, we use **Binary Copy** (`type: "Binary"`). This streams raw byte streams directly from the source HTTP endpoints into ADLS Gen2 without parsing schemas, mutating delimiters, converting character encodings, or altering timestamps. Raw CSV files land as exact CSVs, and JSON lines land as exact JSONs, preserving an immutable source-of-truth landing zone.

---

## Databricks & Delta Lake Medallion Architecture (Module 3 Focus)

### Q17: What is the Delta Lake Transaction Log (`_delta_log`), and how does it guarantee ACID transactions?
**Answer:**  
Delta Lake maintains a directory of JSON-formatted commit files in `_delta_log/*.json`. Every transaction (append, update, delete, merge) writes a new commit file listing added and removed Parquet files. Concurrency is managed via **Optimistic Concurrency Control (OCC)** with atomic commit protocols. Readers read snapshot state as of a specific commit version, guaranteeing isolated, repeatable reads and atomic commits without dirty read anomalies.

---

### Q18: What is the architectural purpose of each layer in the Medallion Lakehouse?
**Answer:**  
- **Bronze (Raw Ingestion):** Append-only raw data preserving original source fidelity with ingestion audit metadata (`_source_file`, `_source_path`, `_ingestion_date`, `_adf_run_id`, `_ingested_timestamp`).
- **Silver (Cleaned & Conformed):** Deduplicated, validated, strongly-typed data with business rules enforced. Bad/orphan records are routed to a parallel quarantine sink with error codes, while valid data supports enterprise transformations.
- **Gold (Business Aggregates & KPIs):** High-performance analytical aggregates and KPI tables designed for executive reporting, BI dashboards, and ad-hoc SQL querying.

---

### Q19: How does Delta Lake handle schema enforcement vs. schema evolution?
**Answer:**  
- **Schema Enforcement:** By default, Delta Lake rejects any write operation containing columns or data types that do not match the target table's schema, preventing silent table corruption.
- **Schema Evolution:** When intentional schema changes occur (e.g., adding a new attribute), engineers specify `.option("mergeSchema", "true")`. Delta updates table metadata to include the new column, backfilling previous files with `NULL` on read.

---

### Q20: Explain how Delta Lake Time Travel works under the hood.
**Answer:**  
Because Delta Lake's transaction log tracks the history of added and removed files per commit, querying `VERSION AS OF N` or `TIMESTAMP AS OF '2026-08-31'` instructs the Delta engine to reconstruct the table snapshot by reading only the transaction log commits up to that point. It skips newer files and reads historical Parquet files that were active at that version.

---

## Dimensional Modeling & SCD Architecture (Module 4 Focus)

### Q21: What is the difference between SCD Type 1 and SCD Type 2?
**Answer:**  
- **SCD Type 1 (Overwrite):** Overwrites historical attribute values with current values in-place. No historical record is preserved. Surrogate keys remain unchanged. (e.g., updating `current_retail_price` or `category` in `dim_product`).
- **SCD Type 2 (Historical Versioning):** Preserves full historical context by creating a new record version when tracked attributes change. Uses `effective_from`, `effective_to` (NULL for current), `is_current`, and `version_number`. (e.g., tracking customer loyalty tier upgrades or address relocations in `dim_customer`).

---

### Q22: Why should distributed lakehouses avoid `monotonically_increasing_id()` for surrogate keys?
**Answer:**  
`monotonically_increasing_id()` generates 64-bit integers where the upper 33 bits represent the partition ID and the lower 31 bits represent the row ID within the partition. This produces non-consecutive, sparse integer IDs (e.g., 0, 8589934592, 17179869184) that change whenever partition boundaries shift. In dimensional modeling, compact surrogate keys are allocated deterministically using `max_existing_key + ROW_NUMBER() OVER (ORDER BY natural_key)`.

---

### Q23: How does Point-in-Time (PIT) Surrogate Key resolution work for Fact tables?
**Answer:**  
When populating `fact_sales`, transaction rows are joined to `dim_customer` on `customer_id` using a **non-equi temporal join**:
```sql
order_timestamp >= effective_from AND (order_timestamp < effective_to OR effective_to IS NULL)
```
This resolves the historical surrogate `customer_key` that was active when the customer made the purchase, preventing historical revenue from being retroactively attributed to subsequent loyalty tiers or relocations.

---

## Lakeflow Jobs & Orchestration Architecture (Module 5 Focus)

### Q35: How do tasks communicate operational metadata in Lakeflow Jobs without violating big data architecture principles?
**Answer:**  
Lakeflow tasks communicate via **Task Values**:
- Inside Databricks tasks: `dbutils.jobs.taskValues.set(key="discovered_count", value=8)` and `dbutils.jobs.taskValues.get(...)`.
- In Lakeflow Jobs YAML: `{{tasks.<task_name>.values.<value_name>}}`.
- **Architectural Rule:** Task values are strictly reserved for small metadata primitives (record counts, status booleans, table URIs). Tabular data stays persisted in ADLS Gen2 / Delta Lake and is NEVER passed across task memory.

---

### Q36: How does a Databricks Repair Run work, and why is your lakehouse pipeline design 100% repair-safe?
**Answer:**  
- **Repair Run Mechanics:** When a multi-task Lakeflow Job fails midway (e.g. at `dimensional_warehouse`), Databricks allows engineers to trigger a repair run. Databricks re-executes **only the failed task and downstream dependent tasks**, completely bypassing successful upstream stages (`validate_landing_batch`, `bronze_ingestion`, `silver_transformation`).
- **Idempotency Guarantee:**
  1. Bronze uses `_ingestion_audit` to skip already-ingested files.
  2. Silver deduplication windows and table overwrites prevent duplicate generation.
  3. SCD1 uses null-safe Delta MERGE; SCD2 uses deterministic temporal intervals.
  4. Fact surrogate key lookups are deterministic.

---

### Q37: How do you implement operational run auditing across your Lakeflow DAG, and why is the publish task configured with `run_if: ALL_DONE`?
**Answer:**  
Every execution appends exactly one record to the `delta/operations/job_run_audit` Delta table.
- **`run_if: ALL_DONE` Semantics:** Ensures the summary task executes whether upstream stages succeeded or failed.
- **Schema & Nullability:** On successful runs, it captures throughput (Bronze rows, Silver valid/quarantine, fact sales count, duration). On failed early runs, downstream metric fields are stored as `NULL`, while error details (`failure_task`, `failure_classification`, `error_message`) are populated for RCA.

---

### Q38: How does conditional branching work in Lakeflow Jobs, and what is the architectural difference between a Quarantine Warning and a Critical Data Quality Failure?
**Answer:**  
- **Lakeflow Condition Task:** Configured using `condition_task` with an operator (e.g. `op: EQUAL_TO`, `left: "{{tasks.silver_transformation.values.quarantine_alert_triggered}}"`, `right: "true"`). Downstream tasks declare a dependency with `outcome: "true"` (e.g. `quality_attention` branch).
- **Quarantine Warning vs. Critical Quality Failure:**
  - *Quarantine Warning:* When the quarantine rate exceeds a warning threshold (e.g. >20%), the bad records are successfully isolated, and the conformed valid data still mathematically balances with Bronze. The pipeline executes the `quality_attention` task to alert on-call engineers, but downstream Gold & Warehouse workloads continue normally.
  - *Critical Quality Failure:* If mathematical reconciliation fails (`bronze != valid + quarantine`) or referential integrity in fact tables is violated, the pipeline hard-fails immediately and aborts downstream processing.

---

### Q39: Why are legacy `/mnt/` DBFS mount paths deprecated in modern Azure Databricks architectures, and how should storage be accessed?
**Answer:**  
- **Problems with `/mnt/`:** DBFS mount points rely on cluster-wide credentials, cannot enforce Unity Catalog fine-grained access control (row/column filters, ABAC), and introduce hidden external dependencies.
- **Modern Pattern:** Access storage directly via governed **ABFSS URIs** (`abfss://<container>@<account>.dfs.core.windows.net/...`) authenticated with Azure Databricks Access Connectors and Managed Identities, and manage tables through the **Unity Catalog 3-level namespace** (`<catalog>.<schema>.<table>`).

---

## Production CI/CD, Bundles & Governed SQL Serving (Module 6 Focus)

### Q40: What happens after a developer opens a Pull Request or merges to `main` in this repository?
**Answer:**  
GitHub Actions automatically triggers the Continuous Integration (`.github/workflows/ci.yml`) workflow:
1. Provisions Python 3.11 and Java 17 runners.
2. Executes Ruff static analysis (`ruff check .`) for linting and style enforcement.
3. Executes the full Pytest suite (87+ unit and integration tests across Modules 1–6).
4. Builds the Python release wheel (`python -m build --wheel`).
5. Installs the wheel into an isolated clean environment and smoke-tests core imports.
6. Validates Declarative Automation Bundle structure, variable schemas, and serving SQL view contracts.
7. Quality Gate: Pull requests cannot merge if any lint, test, build, or security check fails.

---

### Q41: How is production deployment authenticated securely without storing long-lived passwords or PATs in GitHub?
**Answer:**  
We utilize **GitHub Workload Identity Federation (OIDC)**:
1. The deployment workflow requests a short-lived OIDC JSON Web Token (JWT) from GitHub (`id-token: write`).
2. The workflow presents the JWT to Azure Databricks (`DATABRICKS_AUTH_TYPE: "github-oidc"`).
3. Databricks verifies the token against GitHub's OpenID Connect provider and issues a short-lived federated access token mapped to a dedicated Databricks Service Principal (`DATABRICKS_CLIENT_ID`).
4. **Security Benefit:** Zero Personal Access Tokens (PATs) or client secrets exist in GitHub Secrets, eliminating credential expiration incidents and token leakage risks.

---

### Q42: What is a Databricks Declarative Automation Bundle and why is it preferred over manual workspace configuration?
**Answer:**  
Declarative Automation Bundles (formerly *Databricks Asset Bundles*) represent Databricks-native Infrastructure as Code (IaC):
- **Declarative Structure:** All Lakeflow Jobs, Serverless SQL Warehouses, Python wheel artifacts, and parameter overrides are version-controlled in `databricks.yml` and resource YAML files.
- **Multi-Environment Support:** Declarative targets (`dev`, `prod`) isolate developer sandboxes from governed production workspaces.
- **Auditable & Repeatable:** Deploys identical resource graphs through `databricks bundle deploy`, eliminating UI click-ops and configuration drift.

---

### Q43: Why are CI and CD separated into distinct workflows, and why is production deployment triggered via `workflow_dispatch`?
**Answer:**  
- **Separation of Concerns:** CI focuses on code quality, automated testing, and artifact packaging without requiring Databricks credentials. CD focuses on authenticated resource provisioning and deployment.
- **Safe by Default:** Automated deployment on every push is dangerous for enterprise data platforms where deployments must coordinate with maintenance windows or peer reviews. Triggering CD via `workflow_dispatch` (with optional GitHub Environment production approval protection) gives platform leads explicit release control.

---

### Q44: What is the purpose of the Python wheel artifact in this lakehouse architecture?
**Answer:**  
Building a standard Python wheel (`retail_lakehouse_data_platform-0.1.0-py3-none-any.whl`) packages all reusable business logic (`src/medallion`, `src/modeling`, `src/orchestration`, `src/quality`, `src/schemas`) into a versioned, distributable binary. It allows Databricks clusters and jobs to install the library directly, preventing script path coupling and guaranteeing identical dependency resolution across local and cloud environments.

---

### Q45: Why use a Serverless Databricks SQL Warehouse for BI serving instead of querying Lakehouse Delta tables directly from an all-purpose Spark cluster?
**Answer:**  
1. **Instant Compute & Serverless Auto-Stop:** Serverless SQL Warehouses start in seconds and aggressively shut down after 10 minutes of inactivity (`auto_stop_mins: 10`), minimizing idle compute costs compared to 10–15 minute all-purpose cluster spin-down times.
2. **Decoupled Concurrency:** SQL Warehouses handle concurrent BI analyst queries without contending for CPU/memory with heavy batch ETL pipelines.
3. **ANSI SQL Optimization:** Uses Databricks Photon engine optimized specifically for vectorised SQL aggregations, joins, and BI query workloads.

---

### Q46: Why does the serving layer use Unity Catalog Views over Gold & Warehouse tables rather than copying data into a separate physical reporting table?
**Answer:**  
1. **Zero Data Redundancy & Storage Cost:** Views execute queries directly against the underlying optimized Delta tables, eliminating duplicate storage costs.
2. **Zero Ingestion Latency:** When the warehouse pipeline updates `fact_sales` or Gold KPI tables, views immediately reflect the newest committed data without needing secondary ETL extract jobs.
3. **Abstraction & Governance:** Views expose clean business column names and join relationships in `<catalog>.serving.*` while shielding physical storage locations and schema structures from end-users.

---

### Q47: In the `sales_detail` serving view, why is the join between `fact_sales` and `dim_customer` performed on `customer_key` rather than a range join on transaction timestamp?
**Answer:**  
During the Module 4 warehouse ETL pipeline, the point-in-time surrogate key lookup already evaluated `order_timestamp >= effective_from AND (order_timestamp < effective_to OR effective_to IS NULL)` to stamp `fact_sales.customer_key`. In the serving layer, joining on `customer_key` is a simple integer equi-join that preserves the exact historical customer loyalty tier and address valid at purchase time, without imposing expensive and error-prone range scans on BI queries.

---

### Q48: What platform aspects remain unverified without active Azure cloud credentials?
**Answer:**  
While all code, schema contracts, DAG cycle validation, secret scanning, wheel packaging, and local PySpark integration pipelines pass with 100% test coverage locally:
- **Cloud Pending Elements:** Live Azure Data Factory pipeline execution, live ADLS Gen2 Hierarchical Namespace blob movement, Azure Databricks workspace cluster provisioning, Serverless SQL Warehouse cloud allocation, live Unity Catalog metastore registration, and live GitHub OIDC token exchange with Azure Entra ID.
