# 04. Spark SQL & Advanced Window Functions (Module 1 Study Guide)

> **Status:** Build Complete | **Learning Status:** ⏳ NOT STUDIED / PENDING

---

## 1. Spark SQL & Temporary Views

### WHAT IT IS
Spark SQL is Apache Spark's module for structured data processing. It allows querying DataFrames using standard ANSI SQL syntax by registering DataFrames as **Temporary Views** (`createOrReplaceTempView`).

### HOW IT WORKS
When a temporary view is registered, it creates an entry in Spark's in-memory session catalog. It does **not** write data to disk or create a physical database table. The SQL query is parsed into an unresolved logical plan, resolved against the catalog, and optimized by Catalyst into the exact same physical RDD execution plan as DataFrame API code.

### EXACT FILE / IMPLEMENTATION
- [local_batch_pipeline.py](file:///Users/vijjureddy/Job%20Switch%20Projects/azure-lakehouse-data-platform/src/pipelines/local_batch_pipeline.py#L173-L199) (`_execute_spark_sql_analytics`)

```python
# Register view in memory
curated_sales_df.createOrReplaceTempView("v_curated_sales")
clean_returns_df.createOrReplaceTempView("v_returns")

# Execute ANSI SQL
result_df = spark.sql("SELECT order_year, SUM(net_sales) FROM v_curated_sales GROUP BY order_year")
```

---

## 2. Implemented SQL Analytics Queries

All queries are maintained as modular SQL files in [sql/](file:///Users/vijjureddy/Job%20Switch%20Projects/azure-lakehouse-data-platform/sql/):

### 1. Daily & Monthly Revenue Trends ([daily_monthly_revenue.sql](file:///Users/vijjureddy/Job%20Switch%20Projects/azure-lakehouse-data-platform/sql/daily_monthly_revenue.sql))
Computes daily revenue, distinct order volume, unique active buyers, and average line item value across time partitions.

### 2. Top Products by Category via Window Ranking ([top_products_by_revenue.sql](file:///Users/vijjureddy/Job%20Switch%20Projects/azure-lakehouse-data-platform/sql/top_products_by_revenue.sql))
Employs `DENSE_RANK() OVER (PARTITION BY category ORDER BY total_net_revenue DESC)` inside a Common Table Expression (CTE) to filter the top 5 products per category.

### 3. Regional Performance & Channel Contribution ([revenue_by_region.sql](file:///Users/vijjureddy/Job%20Switch%20Projects/azure-lakehouse-data-platform/sql/revenue_by_region.sql))
Analyzes revenue and profit margin breakdown across store regions (West, East, South, Central) and sales channels (WEB, IN_STORE, MOBILE_APP).

### 4. Category Return Rate & Realized Revenue ([average_order_value_and_returns.sql](file:///Users/vijjureddy/Job%20Switch%20Projects/azure-lakehouse-data-platform/sql/average_order_value_and_returns.sql))
Joins sales line items with returns to calculate unit return rate percentages, refunded totals, and final net realized revenue per merchandise category.

---

## 3. PySpark Window Functions Guide

A Window Function performs a calculation across a set of table rows that are related to the current row, without collapsing the individual rows (unlike `groupBy`).

### Window Specifications in PySpark
A window specification defines three clauses:
1. **`partitionBy(*cols)`**: Divides rows into groups/partitions.
2. **`orderBy(*cols)`**: Determines the ordering of rows within each partition.
3. **`rowsBetween(start, end)`** or **`rangeBetween(start, end)`**: Defines the sliding frame of rows relative to the current row (e.g. `unboundedPreceding` to `currentRow`).

---

## 4. Window Functions Implemented in Module 1

### 1. `ROW_NUMBER()` — Customer Order Sequence
Assigns a unique, sequential 1-based integer to each customer's order in chronological order:

```python
cust_order_window = Window.partitionBy("customer_id").orderBy(
    F.col("order_timestamp").asc(),
    F.col("order_id").asc()
)

curated_df = curated_df.withColumn(
    "customer_order_sequence",
    F.row_number().over(cust_order_window)
)
```
*Business Use Case:* Identify first-time orders (`sequence = 1`) vs repeat buyers (`sequence > 1`) for customer lifetime value (LTV) cohort analysis.

---

### 2. Running Total / Cumulative Spend — `SUM() OVER (ROWS BETWEEN ...)`
Calculates the running total dollar amount spent by a customer up to the current transaction:

```python
cust_running_window = (
    Window.partitionBy("customer_id")
    .orderBy(F.col("order_timestamp").asc(), F.col("order_item_id").asc())
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

curated_df = curated_df.withColumn(
    "customer_running_spend",
    F.sum("net_sales").over(cust_running_window).cast(DecimalType(14, 2))
)
```
*Business Use Case:* Dynamic tier calculation and VIP customer identification at the exact moment spend crosses thresholds.

---

### 3. `LAG()` — Days Since Prior Order
Fetches the date of the customer's previous order to measure purchase frequency:

```python
cust_lag_window = Window.partitionBy("customer_id").orderBy(F.col("order_date").asc(), F.col("order_id").asc())

curated_df = (
    curated_df.withColumn("prev_order_date", F.lag("order_date", 1).over(cust_lag_window))
              .withColumn("days_since_prior_order", F.datediff(F.col("order_date"), F.col("prev_order_date")))
              .drop("prev_order_date")
)
```
*Business Use Case:* Churn risk prediction, purchase cycle modeling, and automated re-engagement triggers.

---

### 4. `DENSE_RANK()` vs `RANK()` — Product Category Leaderboards
Ranks products by total net revenue within each product category:

```python
cat_rank_window = Window.partitionBy("category").orderBy(F.col("total_prod_cat_sales").desc())

ranked_products = product_totals.withColumn(
    "category_product_rank",
    F.dense_rank().over(cat_rank_window)
)
```

| Function | Ties Handling | Sequence Example with Ties |
| :--- | :--- | :--- |
| **`ROW_NUMBER()`** | Distinct row integers, arbitrarily breaking ties | 1, 2, 3, 4, 5 |
| **`RANK()`** | Same rank for ties, skips subsequent numbers | 1, 2, 2, 4, 5 (Skips 3) |
| **`DENSE_RANK()`** | Same rank for ties, **no gaps** in ranking | 1, 2, 2, 3, 4 (No gaps) |

---

## 5. Interview Questions & Expected Answers

### INTERVIEW QUESTION 1
> "What is the difference between `rowsBetween()` and `rangeBetween()` in Spark window functions?"

### EXPECTED ANSWER
> "`rowsBetween()` defines the window frame based on physical row offsets relative to the current row (e.g. 5 rows before to current row). `rangeBetween()` defines the frame based on logical value differences in the `orderBy` column (e.g. timestamps within the last 7 days or prices within $10 of the current row's value)."

### INTERVIEW QUESTION 2
> "Why does an unpartitioned window specification like `Window.orderBy('salary')` pose a severe performance risk in Spark?"

### EXPECTED ANSWER
> "If a window specification omits `partitionBy()`, Spark must move ALL rows across the entire dataset onto a single partition on a single executor to compute global ordering. This causes massive network shuffling, eliminates parallelism, and frequently causes Executor OutOfMemory (OOM) crashes on large datasets."
