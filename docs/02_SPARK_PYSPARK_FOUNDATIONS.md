# 02. Apache Spark & PySpark Foundations (Module 1 Study Guide)

> **Status:** Build Complete | **Learning Status:** ⏳ NOT STUDIED / PENDING

---

## 1. Apache Spark & PySpark Overview

### WHAT IT IS
- **Apache Spark:** A distributed, in-memory computing engine designed for large-scale data processing and analytics.
- **PySpark:** The official Python API for Apache Spark, allowing developers to write Spark applications using idiomatic Python while delegating heavy distributed computations to Spark's JVM-based Catalyst Optimizer and Tungsten execution engine.

### ARCHITECTURE: DRIVER & EXECUTOR
```
+-------------------------------------------------------------+
|                        DRIVER NODE                          |
|  - Creates SparkSession                                     |
|  - Translates code into Logical & Physical DAG Plans         |
|  - Coordinates Tasks via DAGScheduler & TaskScheduler       |
+-------------------------------------------------------------+
                              |
                Cluster Manager / Local Engine
                              |
       +----------------------+----------------------+
       |                                             |
+---------------+                             +---------------+
| EXECUTOR 1    |                             | EXECUTOR 2    |
| - JVM Core    |                             | - JVM Core    |
| - Tasks (1..N)|                             | - Tasks (1..N)|
| - Cache/RAM   |                             | - Cache/RAM   |
+---------------+                             +---------------+
```

- **Driver:** The master process that runs the user's `main()` program, creates the `SparkSession`, translates DataFrame operations into execution DAGs, schedules tasks, and collects summary metrics.
- **Executors:** Worker processes that run on cluster nodes (or multi-threaded processes in `local[*]` mode) responsible for executing assigned tasks and storing cached data partitions in RAM/disk.

---

## 2. SparkSession

### WHAT IT IS
The unified entry point for PySpark applications (introduced in Spark 2.0). It encapsulates `SparkContext`, `SQLContext`, and `HiveContext` under a single, cohesive API.

### EXACT FILE / CONFIGURATION
- [spark.py](../src/utils/spark.py) (`get_spark_session`)

```python
spark = (
    SparkSession.builder
    .appName("AzureLakehouse_LocalModule1")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
```

### WHY WE CONFIGURE THESE SETTINGS
1. **`master("local[*]")`**: Instructs Spark to run locally using as many worker threads as logical CPU cores on the laptop.
2. **`spark.sql.shuffle.partitions = 4`**: Spark defaults to 200 shuffle partitions for joins and group-bys (sized for large clusters). On local laptop datasets, 200 partitions creates severe task scheduling and small file overhead. Setting it to 4 matches local CPU core count.
3. **`spark.sql.session.timeZone = "UTC"`**: Guarantees that timestamp parsing, date casting, and window calculations evaluate uniformly regardless of what local timezone the host operating system is set to.
4. **`spark.sql.adaptive.enabled = "true"`**: Enables Adaptive Query Execution (AQE), allowing Spark to dynamically optimize join strategies and coalesce shuffle partitions at runtime.

---

## 3. Lazy Evaluation & DAG Execution

### WHAT IT IS
- **Lazy Evaluation:** In Spark, transformations (e.g., `filter`, `select`, `join`, `withColumn`) do NOT compute immediately when called. Instead, Spark records these operations as a recipe or lineage graph.
- **DAG (Directed Acyclic Graph):** The graph of logical stages and tasks created by Spark. Physical computation ONLY triggers when an **Action** is invoked.

### TRANSFORMATIONS VS ACTIONS

| Type | Examples | What It Does |
| :--- | :--- | :--- |
| **Transformations** | `select()`, `filter()`, `withColumn()`, `groupBy()`, `join()`, `dropDuplicates()`, `orderBy()` | Returns a new DataFrame. Does not compute data immediately. |
| **Actions** | `count()`, `collect()`, `show()`, `write.parquet()`, `first()`, `take()` | Triggers DAG computation across executors and returns results or writes to storage. |

```
[Raw Ingestion (Lazy)] -> [Trim Strings (Lazy)] -> [Anti-Join (Lazy)] -> [write.parquet() (ACTION!)]
                                                                                  |
                                                                        Triggers execution of DAG
```

### INTERVIEW QUESTION
> "Why does Spark use lazy evaluation instead of executing line-by-line eagerly?"

### EXPECTED ANSWER
> "Lazy evaluation allows Spark's Catalyst Optimizer to inspect the entire end-to-end DAG before execution. It can apply query optimizations such as predicate pushdown (filtering rows at storage read time), column pruning (loading only required columns), and combining adjacent filter operations, which dramatically reduces memory consumption and network I/O."

---

## 4. Narrow vs Wide Transformations & Shuffling

### NARROW TRANSFORMATIONS (No Shuffle)
- Each partition in the parent DataFrame contributes to **at most one** partition in the child DataFrame.
- Data does NOT need to move across the network between executors.
- *Examples:* `filter()`, `select()`, `withColumn()`, `drop()`, `map()`.

### WIDE TRANSFORMATIONS (Requires Shuffle)
- Each partition in the parent DataFrame contributes to **multiple** partitions in the child DataFrame.
- Requires a **Shuffle**: data must be redistributed across the network based on partition keys.
- *Examples:* `groupBy()`, `join()`, `distinct()`, `dropDuplicates()`, `orderBy()`, Window operations with `partitionBy()`.

```
Narrow Transformation (filter/withColumn):
Partition A1 ---------------> Partition B1 (No network transfer)
Partition A2 ---------------> Partition B2

Wide Transformation (groupBy/join):
Partition A1 -----\ /-------> Partition B1 (Network Shuffle)
                   X
Partition A2 -----/ \-------> Partition B2
```

### INTERVIEW QUESTION
> "What is a Spark Shuffle and why is it expensive?"

### EXPECTED ANSWER
> "A shuffle is the process of redistributing data across executors so that rows sharing the same key reside on the same partition (e.g., during a join or group-by). It is expensive because it involves serializing data to disk on mapper tasks, transferring it across the network to reducer tasks, and deserializing it into memory, making it a common bottleneck for high-volume pipelines."

---

## 5. Key PySpark DataFrame Operations in Module 1

### 1. `select()` and `filter()`
```python
# Select required columns and filter invalid records
clean_df = classified_df.filter(F.col("rejection_reason").isNull()).select("customer_id", "full_name", "email")
```

### 2. `withColumn()` and `when().otherwise()`
```python
# Add derived column with conditional logic
df = df.withColumn(
    "loyalty_tier",
    F.when(F.col("spend") > 1000, F.lit("PLATINUM"))
     .when(F.col("spend") > 500, F.lit("GOLD"))
     .otherwise(F.lit("STANDARD"))
)
```

### 3. `groupBy()` and `agg()`
```python
# Product revenue aggregations
product_totals = financials_df.groupBy("category", "product_id").agg(
    F.sum("net_sales").alias("total_prod_cat_sales"),
    F.count("order_item_id").alias("total_units_sold")
)
```

### 4. `join()` & Join Types
- **Inner Join:** Retains only matching rows from both DataFrames (used in curated sales where all dimensional keys are validated).
- **Left Outer Join:** Retains all rows from the left table and matching rows from the right table (used when attaching optional return records or looking up reference dimensions).
- **Left Anti Join:** Returns rows from the left table that have **no match** in the right table (used in referential integrity validation to identify orphan foreign keys).

```python
# Orphan check using Left Anti Join
orphans_df = child_df.join(parent_df, child_df.fk == parent_df.pk, "left_anti")
```

### 5. `dropDuplicates()` vs Window Ranking
- `dropDuplicates(["col"])` retains an arbitrary row among duplicates.
- Window ranking with `ROW_NUMBER().over(Window.partitionBy("key").orderBy("timestamp.desc"))` allows deterministic deduplication, keeping the newest or highest quality record and routing the duplicate instance to quarantine.

---

## 6. Parquet Columnar Storage & Partitioning

### WHY PARQUET?
1. **Columnar Layout:** Data is organized by column rather than by row. Queries scanning 2 columns out of 50 only read 4% of the data from disk/S3/ADLS.
2. **High Compression:** Homogeneous column data types enable superior compression (Snappy, Gzip, ZSTD) reducing storage size by 70–80% compared to CSV/JSON.
3. **Embedded Metadata & Statistics:** Each Parquet file stores min/max column values, row counts, and data types in the file footer.

### PREDICATE PUSHDOWN
Spark uses Parquet footer statistics to skip reading entire row groups if the query `WHERE` condition falls outside the file's `[min, max]` range without reading the data blocks.

### PARTITIONING STRATEGY & SMALL-FILE PROBLEM
- In Module 1, `curated_sales` is partitioned by `order_year` and `order_month`:
```python
curated_sales_df.write.mode("overwrite").partitionBy("order_year", "order_month").parquet("output/curated/curated_sales")
```
- **The Small-File Problem:** Over-partitioning by high-cardinality columns (e.g. `customer_id` or `order_id`) generates millions of tiny kilobyte-sized files. This overwhelms file system metadata operations and kills read performance. Best practice is to partition by low-to-medium cardinality temporal or regional dimensions resulting in file sizes between 128MB and 1GB per partition in production.
