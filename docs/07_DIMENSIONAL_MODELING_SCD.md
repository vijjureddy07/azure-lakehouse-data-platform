# 07. Dimensional Modeling, Slowly Changing Dimensions & Enterprise Quality Gates (Module 4 Study Guide)

> **Build Status:** 🟢 COMPLETE & LOCAL-VERIFIED  
> **Cloud Verification Status:** ⏳ LIVE DATABRICKS CLOUD VERIFICATION PENDING (Azure Databricks credentials required)  
> **Learning Status:** ⏳ NOT STUDIED / PENDING  

---

## 1. Dimensional Modeling & The Kimball Methodology

### ENTERPRISE DATA WAREHOUSING PARADIGMS: KIMBALL VS INMON

| Dimension | Ralph Kimball (Dimensional Modeling) | Bill Inmon (Corporate Information Factory) |
| :--- | :--- | :--- |
| **Philosophy** | Bottom-up, business-process oriented | Top-down, enterprise-wide normalized model |
| **Core Structure** | Star Schemas & Snowflake Schemas (Facts & Dimensions) | 3rd Normal Form (3NF) relational enterprise data model |
| **Primary Goal** | Fast query performance, intuitive for BI analysts | Single enterprise version of truth, eliminate data redundancy |
| **Serving Layer** | Directly queryable star schemas & dimensional marts | Data marts derived downstream from normalized enterprise warehouse |
| **Lakehouse Fit** | **Ideal for Delta Lake & Spark SQL** (minimized joins, columnar scan speed) | High join complexity, inefficient for distributed columnar analytical engines |

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                               THE KIMBALL 4-STEP DESIGN PROCESS                                  │
│                                                                                                  │
│  1. SELECT THE BUSINESS PROCESS ➔ Retail Sales Orders & Product Returns                          │
│  2. DECLARE THE GRAIN           ➔ Exactly ONE row per line-item (fact_sales) / return event      │
│  3. IDENTIFY THE DIMENSIONS     ➔ Customer (SCD2), Product (SCD1), Store, Employee, Date         │
│  4. IDENTIFY THE NUMERIC FACTS  ➔ quantity, unit_price, gross_amount, discount_amount, net, cost │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Lakehouse Star Schema Architecture

In the Lakehouse Dimensional Warehouse layer (`delta/warehouse/`), we transform conformed Silver Delta tables into an enterprise dimensional model optimized for business intelligence, slicing/dicing, and historical analytics.

```
                                  ┌───────────────────────────┐
                                  │         dim_date          │
                                  │ ───────────────────────── │
                                  │ PK: date_key (Int)        │
                                  │ full_date (Date)          │
                                  │ day_name, month_name...   │
                                  │ year, quarter, is_weekend │
                                  └─────────────┬─────────────┘
                                                │
                                                │ 1:N (order_date_key)
                                                ▼
┌──────────────────────────┐      ┌───────────────────────────┐      ┌──────────────────────────┐
│       dim_customer       │      │        fact_sales         │      │       dim_product        │
│ ──────────────────────── │      │ ───────────────────────── │      │ ──────────────────────── │
│ PK: customer_key (Int)   ├─────►│ PK/Grain: order_item_id   │◄─────┤ PK: product_key (Int)    │
│ NK: customer_id (String) │ 1:N  │ FK: customer_key (PIT)    │ 1:N  │ NK: product_id (String)  │
│ loyalty_tier, address... │      │ FK: product_key           │      │ product_name, category   │
│ effective_from, to       │      │ FK: store_key             │      │ subcategory, cost_price  │
│ is_current, version_num  │      │ FK: order_date_key        │      │ is_active (SCD Type 1)   │
└──────────────────────────┘      │ Degenerate: order_id      │      └──────────────────────────┘
                                  │ quantity, unit_price      │
                                  │ gross_amount, discount    │
                                  │ net_amount, cost_amount   │
                                  │ profit_amount             │
                                  └─────────────▲─────────────┘
                                                │
                                                │ 1:N (store_key)
                                                ▼
                                  ┌───────────────────────────┐
                                  │         dim_store         │
                                  │ ───────────────────────── │
                                  │ PK: store_key (Int)       │
                                  │ NK: store_id (String)     │
                                  │ store_name, type, region  │
                                  └───────────────────────────┘
```

### DIMENSION CLASSIFICATIONS
1. **Conformed Dimensions:** Consistent, standardized dimensions (`dim_customer`, `dim_product`, `dim_date`) shared across multiple business processes (`fact_sales` and `fact_returns`).
2. **Degenerate Dimensions:** Dimension attributes stored directly in the fact table without a separate dimension table (`order_id`, `order_status`, `channel`).
3. **Role-Playing Dimensions:** A single dimension referenced multiple times in different business contexts (e.g. `dim_date` playing roles as `order_date_key` in `fact_sales` and `return_date_key` in `fact_returns`).
4. **Unknown Member / Late-Arriving Records:** Dedicated record with surrogate key `0` (`date_key = 0`, `customer_key = 0`, `product_key = 0`, `store_key = 0`) representing missing, late-arriving, or corrupted foreign keys, preventing fact rows from being dropped during inner joins.

---

## 3. Slowly Changing Dimensions (SCD) Deep Dive

### THE 6 SCD TYPES COMPARISON

| Type | Name | Strategy | History Preserved? | Storage Impact | Typical Use Case |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Type 0** | Fixed / Retain | Ignore incoming changes; keep original value | No | None | Original signup date, date of birth |
| **Type 1** | Overwrite | In-place update of dimension attributes | **No** (Overwrites past) | Minimal | Typo corrections, product category renames |
| **Type 2** | Add New Row | Insert new version record with validity interval | **Yes** (Full historical trail) | Proportional to changes | Customer address changes, loyalty tier upgrades |
| **Type 3** | Add New Attribute| Add `current_<attr>` and `previous_<attr>` columns | Partial (Only 1 past state) | Adds columns | Previous department, previous territory |
| **Type 4** | Mini-Dimension | Separate high-frequency changing attributes | Yes | New dimension table | Rapidly changing customer credit score or age band |
| **Type 6** | Hybrid (1+2+3) | Type 2 row versioning + Type 1 current column | Full | Higher | Real-time reporting with historical tracking |

---

### SCD TYPE 1 IMPLEMENTATION (DELTA MERGE)
For `dim_product`, changes to `product_name`, `category`, `subcategory`, `cost_price`, `unit_price`, or `is_active` are updated in place using Delta MERGE, preserving the original deterministic `product_key`:

```sql
MERGE INTO delta/warehouse/dim_product AS target
USING incoming_products AS source
ON target.product_id = source.product_id
WHEN MATCHED AND (target.product_name != source.product_name OR target.unit_price != source.unit_price) THEN
  UPDATE SET 
    target.product_name = source.product_name,
    target.category = source.category,
    target.unit_price = source.unit_price,
    target.is_active = source.is_active
WHEN NOT MATCHED THEN
  INSERT (product_key, product_id, product_name, category, unit_price, is_active)
  VALUES (source.new_product_key, source.product_id, source.product_name, source.category, source.unit_price, source.is_active);
```

---

### SCD TYPE 2 IMPLEMENTATION (HISTORICAL VERSIONING)
For `dim_customer`, changes to tracked attributes (`loyalty_tier`, `address`, `city`, `state`, `postal_code`) trigger historical versioning.

#### 1. Deterministic SHA-256 Attribute Hashing
To detect attribute changes instantly without wide column comparisons:
```python
hash_expr = sha2(
    concat_ws("||", *[coalesce(col(c).cast(StringType()), lit("<NULL>")) for c in TRACKED_SCD2_COLS]),
    256,
)
```

#### 2. Half-Open Validity Interval Semantics
Every record in `dim_customer` is governed by the half-open interval `[effective_from, effective_to)`:
- **Active Record:** `effective_to IS NULL` and `is_current = true`.
- **Expired Record:** `effective_to = change_timestamp` and `is_current = false`.
- **New Record:** `effective_from = change_timestamp`, `effective_to = NULL`, `is_current = true`, `version_number = previous_version + 1`.

```
Customer C-001 Lifecycle:
Version 1 (GOLD):     [2026-01-01 00:00:00, 2026-06-01 12:00:00) | is_current = false | key = 1
Version 2 (PLATINUM): [2026-06-01 12:00:00, NULL)                | is_current = true  | key = 51
```

#### 3. Deterministic Surrogate Key Allocation in Distributed Spark
> **CRITICAL ARCHITECTURAL RULE:** Never use `monotonically_increasing_id()` for persistent surrogate keys. It generates non-contiguous, 64-bit integers with partition-dependent gaps that change on every shuffle or repartition.

Instead, allocate deterministic surrogate keys by reading the existing `max(surrogate_key)` from the target Delta table and adding `ROW_NUMBER() OVER (ORDER BY natural_key)`:
```python
max_key = delta_table.select(max("customer_key")).collect()[0][0] or 0
new_keys_df = (
    incoming_df
    .withColumn("customer_key", row_number().over(Window.orderBy("customer_id")) + lit(max_key))
)
```

---

## 4. Point-in-Time Fact Resolution (Temporal Joins)

### THE CRITICAL PITFALL OF JOINING TO `is_current = true`
A pervasive bug in amateur data pipelines is joining incoming transactions to `dim_customer` on `customer_id` where `is_current = true`.
- **The Consequence:** If an order was placed in March 2026 when the customer was in `GOLD` tier, and the customer was upgraded to `PLATINUM` in June 2026, querying historical March revenue by loyalty tier will incorrectly attribute March sales to the `PLATINUM` tier!
- **The Solution:** Always perform **Point-in-Time (PIT) Surrogate Key Resolution** using the transaction's event timestamp:

```sql
SELECT 
    io.order_item_id,
    io.order_id,
    COALESCE(dc.customer_key, 0) AS customer_key,
    COALESCE(dp.product_key, 0)  AS product_key,
    COALESCE(ds.store_key, 0)    AS store_key,
    COALESCE(dd.date_key, 0)     AS order_date_key,
    io.quantity,
    io.unit_price,
    CAST(io.quantity * io.unit_price AS DECIMAL(10, 2)) AS gross_amount,
    io.discount_amount,
    io.net_amount,
    CAST(io.quantity * dp.cost_price AS DECIMAL(10, 2)) AS cost_amount,
    CAST(io.net_amount - (io.quantity * dp.cost_price) AS DECIMAL(10, 2)) AS profit_amount
FROM silver_order_items io
INNER JOIN silver_orders o 
    ON io.order_id = o.order_id
LEFT JOIN dim_customer dc 
    ON io.customer_id = dc.customer_id
   AND o.order_timestamp >= dc.effective_from 
   AND (o.order_timestamp < dc.effective_to OR dc.effective_to IS NULL)
LEFT JOIN dim_product dp 
    ON io.product_id = dp.product_id
LEFT JOIN dim_store ds 
    ON o.store_id = ds.store_id
LEFT JOIN dim_date dd 
    ON o.order_date = dd.full_date;
```

---

## 5. Enterprise Data Quality Gates & Audit Framework

Every pipeline run must enforce automated quality gates across all six core pillars before data is published to enterprise consumers:

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 ENTERPRISE QUALITY GATE SUITE                                    │
│                                                                                                  │
│  1. COMPLETENESS GATES         ➔ Reject NULL surrogate keys, business keys, or critical metrics │
│  2. UNIQUENESS GATES           ➔ Enforce 0 duplicate PKs on dimensions and 1 row/grain on facts  │
│  3. REFERENTIAL INTEGRITY      ➔ 0 orphan foreign keys; unmapped keys must resolve to 0          │
│  4. SCD2 TEMPORAL INVARIANTS   ➔ Exactly 1 active version per customer; no interval overlaps     │
│  5. MEASURE VALIDITY GATES     ➔ gross >= net, quantity > 0, unit_price >= 0, profit = net-cost  │
│  6. FINANCIAL RECONCILIATION   ➔ Fact row count == Silver items; Fact net == Silver net (0 diff) │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### AUDIT PERSISTENCE & HARD-FAIL PIPELINE SEMANTICS
- Every quality check produces a structured `QualityCheckResult` record:
  - `check_name`, `entity_name`, `passed`, `observed_value`, `expected_value`, `severity` (`CRITICAL` / `WARNING`), `check_timestamp`.
- Audit logs are appended to `delta/warehouse/quality_audit` Delta table.
- If **ANY** check with severity `CRITICAL` fails, the pipeline raises `WarehouseQualityGateError` and immediately halts, preventing dirty data propagation.

---

## 6. Enterprise Warehouse Reconciliation Invariant

```
================================================================================
WAREHOUSE SALES RECONCILIATION REPORT
================================================================================
Metric                           Silver Source         Warehouse Fact   Difference
--------------------------------------------------------------------------------
Eligible Order Items Row Count             154                    154            0
Total Gross Sales Amount            $27,842.50             $27,842.50        $0.00
Total Discount Amount                $2,145.10              $2,145.10        $0.00
Total Net Sales Amount              $25,697.40             $25,697.40        $0.00
--------------------------------------------------------------------------------
STATUS: 100% PERFECT FINANCIAL AND ROW-COUNT RECONCILIATION (MATCH)
================================================================================
```

---

## 7. Unity Catalog 3-Level Namespace Warehouse Registration

All warehouse entities are registered in Azure Databricks Unity Catalog under the `retail_lakehouse.warehouse.*` schema:

```sql
CREATE SCHEMA IF NOT EXISTS retail_lakehouse.warehouse
COMMENT 'Enterprise Star Schema Dimensional Model and Fact Tables';

CREATE TABLE IF NOT EXISTS retail_lakehouse.warehouse.dim_customer
USING DELTA
LOCATION 'abfss://lakehouse@<storage_account>.dfs.core.windows.net/delta/warehouse/dim_customer';

CREATE TABLE IF NOT EXISTS retail_lakehouse.warehouse.dim_product
USING DELTA
LOCATION 'abfss://lakehouse@<storage_account>.dfs.core.windows.net/delta/warehouse/dim_product';

CREATE TABLE IF NOT EXISTS retail_lakehouse.warehouse.dim_date
USING DELTA
LOCATION 'abfss://lakehouse@<storage_account>.dfs.core.windows.net/delta/warehouse/dim_date';

CREATE TABLE IF NOT EXISTS retail_lakehouse.warehouse.dim_store
USING DELTA
LOCATION 'abfss://lakehouse@<storage_account>.dfs.core.windows.net/delta/warehouse/dim_store';

CREATE TABLE IF NOT EXISTS retail_lakehouse.warehouse.dim_employee
USING DELTA
LOCATION 'abfss://lakehouse@<storage_account>.dfs.core.windows.net/delta/warehouse/dim_employee';

CREATE TABLE IF NOT EXISTS retail_lakehouse.warehouse.fact_sales
USING DELTA
LOCATION 'abfss://lakehouse@<storage_account>.dfs.core.windows.net/delta/warehouse/fact_sales';

CREATE TABLE IF NOT EXISTS retail_lakehouse.warehouse.fact_returns
USING DELTA
LOCATION 'abfss://lakehouse@<storage_account>.dfs.core.windows.net/delta/warehouse/fact_returns';

CREATE TABLE IF NOT EXISTS retail_lakehouse.warehouse.quality_audit
USING DELTA
LOCATION 'abfss://lakehouse@<storage_account>.dfs.core.windows.net/delta/warehouse/quality_audit';
```

---

## 8. Data Engineering Interview Preparation (Module 4 Q&A)

### Q1: Why do we use surrogate keys in a data warehouse instead of relying on natural keys from source systems?
**Answer:**
1. **Source System Isolation:** Natural keys can change, get recycled, or have format differences across disparate source systems (e.g. ERP vs CRM). Surrogate keys provide a single, immutable internal identifier.
2. **SCD Type 2 History Tracking:** Under SCD Type 2, a single natural key (e.g. `customer_id = 'C-001'`) has multiple rows representing different points in time. Each version must have a unique surrogate key (`customer_key = 1`, `customer_key = 51`) so fact records can bind to the exact historical snapshot.
3. **Query Performance:** Compact integer surrogate keys (`IntegerType` or `LongType`) produce much faster join performance and compression in columnar engines compared to wide UUID or composite string natural keys.
4. **Integration of Unknown / Missing Members:** Allows mapping orphaned or null foreign keys to `0` without violating database constraints.

---

### Q2: How do you implement SCD Type 2 in Apache Spark / Delta Lake efficiently?
**Answer:**
1. Compute a deterministic SHA-256 attribute hash (`attribute_hash`) over all tracked attributes.
2. Left-join the incoming batch against the current active dimension records (`is_current = true`).
3. Identify new customers (`cur_customer_id IS NULL`) and changed customers (`cur_attribute_hash != incoming.attribute_hash`).
4. Materialize the new version records with incremented `version_number`, `effective_from = now_ts`, `effective_to = NULL`, and `is_current = true`.
5. Execute an atomic Delta MERGE to expire changed active records in place (`SET effective_to = now_ts, is_current = false`).
6. Append the new version records to the Delta table.

---

### Q3: Why is `monotonically_increasing_id()` dangerous for surrogate key allocation in production data warehouses?
**Answer:**
`monotonically_increasing_id()` generates 64-bit integers where the top 33 bits represent the partition ID and the lower 31 bits represent the row number within that partition. 
- It is **not contiguous** (contains massive gaps between partition boundaries).
- It is **non-deterministic across pipeline runs**: if the number of partitions or upstream data order changes, completely different IDs are assigned to the same records.
- In production, surrogate keys must be generated using `max_existing_key + ROW_NUMBER() OVER (ORDER BY natural_key)`.

---

### Q4: Explain the difference between SCD Type 1 and SCD Type 2 and when you would choose each.
**Answer:**
- **SCD Type 1 (Overwrite):** Updates dimension records in place. Does not preserve history. Used when historical tracking is unnecessary or misleading (e.g. correcting misspelled customer names, updating product categorization structures, or updating store managers).
- **SCD Type 2 (Versioning):** Preserves full historical accuracy by creating a new record for every change with validity date intervals. Used when historical metrics must be reported accurately based on conditions at transaction time (e.g. customer address for tax calculations, customer loyalty tier for margin analysis, or sales rep territory alignment).

---

### Q5: How do you perform Point-in-Time (PIT) joins between facts and SCD Type 2 dimensions in PySpark?
**Answer:**
Join the fact table to the SCD Type 2 dimension using both the natural business key and a temporal range condition:
```python
(col("fact.customer_id") == col("dim.customer_id"))
& (col("fact.transaction_time") >= col("dim.effective_from"))
& ((col("fact.transaction_time") < col("dim.effective_to")) | col("dim.effective_to").isNull())
```
This ensures facts resolve to the exact dimension version that was active when the transaction occurred.

---

### Q6: What is the "Unknown Member" pattern and why is it critical for data warehouse referential integrity?
**Answer:**
The Unknown Member pattern involves pre-populating every dimension table with a default row where `surrogate_key = 0` and descriptive attributes are set to `"Unknown"` or `"N/A"` (e.g. `dim_date` record `date_key = 0, full_date = 1900-01-01`).
When incoming fact transactions contain missing, null, or late-arriving foreign keys, the pipeline falls back to `COALESCE(dim.surrogate_key, 0)`:
1. Prevents fact records from being dropped during inner joins in BI tools.
2. Avoids `NULL` foreign keys in fact tables.
3. Maintains 100% referential integrity without halting the pipeline for minor reference data latency.
