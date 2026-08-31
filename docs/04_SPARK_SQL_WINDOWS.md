# 04. Spark SQL & Advanced Window Functions (Module 1 Study Guide)

> **Status:** Build Complete | **Learning Status:** ⏳ NOT STUDIED / PENDING

---

## 1. Spark SQL & Temporary Views

### WHAT IT IS
Spark SQL is Apache Spark's module for structured data processing. It allows querying DataFrames using standard ANSI SQL syntax by registering DataFrames as **Temporary Views** (`createOrReplaceTempView`).

### HOW IT WORKS
When a temporary view is registered, it creates an entry in Spark's in-memory session catalog. It does **not** write data to disk or create a physical database table. The SQL query is parsed into an unresolved logical plan, resolved against the catalog, and optimized by Catalyst into the exact same physical RDD execution plan as DataFrame API code.

### EXACT FILE / IMPLEMENTATION
- [local_batch_pipeline.py](../src/pipelines/local_batch_pipeline.py) (`_execute_spark_sql_analytics`)

```python
# Register view in memory
curated_sales_df.createOrReplaceTempView("v_curated_sales")
clean_returns_df.createOrReplaceTempView("v_returns")

# Execute ANSI SQL
result_df = spark.sql("SELECT order_year, SUM(net_sales) FROM v_curated_sales GROUP BY order_year")
```

---

## 2. Implemented SQL Analytics Queries

All queries are maintained as modular SQL files in [sql/](../sql/):

### 1. Daily & Monthly Revenue Trends ([daily_monthly_revenue.sql](../sql/daily_monthly_revenue.sql))
Computes daily revenue, distinct order volume, unique active buyers, and average line item value across time partitions.

### 2. Top Products by Category via Window Ranking ([top_products_by_revenue.sql](../sql/top_products_by_revenue.sql))
Employs `DENSE_RANK() OVER (PARTITION BY category ORDER BY total_net_revenue DESC)` inside a Common Table Expression (CTE) to filter the top 5 products per category.

### 3. Regional Performance & Channel Contribution ([revenue_by_region.sql](../sql/revenue_by_region.sql))
Analyzes revenue and profit margin breakdown across store regions (West, East, South, Central) and sales channels (WEB, IN_STORE, MOBILE_APP).

### 4. Category Return Rate & Realized Revenue ([category_returns_profitability.sql](../sql/category_returns_profitability.sql))
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

Order-level customer metrics (`customer_order_sequence`, `days_since_prior_order`, `customer_running_spend`) are calculated at **unique order grain** first, then joined back to the line-item grain DataFrame:

```python
# 1. Derive unique order-grain DataFrame with total order net sales
order_grain_df = (
    financials_df.groupBy("customer_id", "order_id", "order_timestamp", "order_date")
    .agg(F.sum("net_sales").alias("order_net_sales"))
)

# 2. Define customer order timeline window
cust_order_window = Window.partitionBy("customer_id").orderBy(
    F.col("order_timestamp").asc(),
    F.col("order_id").asc()
)

# 3. Define customer cumulative spend window (unbounded preceding to current row)
cust_running_window = (
    Window.partitionBy("customer_id")
    .orderBy(F.col("order_timestamp").asc(), F.col("order_id").asc())
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

# 4. Calculate sequence, lag days, and cumulative spend at order grain
order_metrics_df = (
    order_grain_df
    .withColumn("customer_order_sequence", F.row_number().over(cust_order_window))
    .withColumn("prev_order_date", F.lag("order_date", 1).over(cust_order_window))
    .withColumn("days_since_prior_order", F.datediff(F.col("order_date"), F.col("prev_order_date")))
    .withColumn("customer_running_spend", F.sum("order_net_sales").over(cust_running_window).cast(DecimalType(14, 2)))
    .drop("prev_order_date", "order_net_sales", "order_timestamp", "order_date")
)

# 5. Join order-grain metrics back onto line-item sales
curated_df = financials_df.join(order_metrics_df, on=["customer_id", "order_id"], how="left")
```

### 1. `ROW_NUMBER()` — Customer Order Sequence
Assigns a unique, sequential 1-based integer to each customer's unique order in chronological order. All line items in the same order share the exact same sequence number.  
*Business Use Case:* Identify first-time orders (`sequence = 1`) vs repeat buyers (`sequence > 1`) for customer lifetime value (LTV) cohort analysis.

---

### 2. Running Total / Cumulative Spend — `SUM() OVER (ROWS BETWEEN ...)`
Calculates the total dollar amount spent by a customer across all completed orders up through the current transaction.  
*Business Use Case:* Dynamic tier calculation and VIP customer identification at the exact moment cumulative spend crosses thresholds.

---

### 3. `LAG()` — Days Since Prior Order
Fetches the date of the customer's previous order to measure purchase frequency at the order grain.  
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
