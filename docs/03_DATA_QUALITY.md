# 03. Data Quality, Defect Injection & Quarantine Architecture

> **Status:** Build Complete | **Learning Status:** ⏳ NOT STUDIED / PENDING

---

## 1. Real-World Data Quality Philosophy

In production data engineering, raw data from operational systems is inherently dirty. Real systems experience:
- Upstream software releases that drop validation checks
- Network retries generating duplicate rows
- Legacy systems exporting non-standard date formats
- Distributed microservices generating out-of-order foreign key events
- Malformed inputs passing through loosely validated UI forms

Rather than assuming perfect data, Module 1 implements an enterprise **Data Quality & Quarantine Framework** that detects defects, isolates invalid rows into an auditable quarantine sink, and guarantees reconciliation metrics.

---

## 2. Injected Defect Catalog & Handling

All defects are generated deterministically in [generate_retail_data.py](file:///Users/vijjureddy/Job%20Switch%20Projects/azure-lakehouse-data-platform/src/data_generation/generate_retail_data.py) using a fixed seed (`seed=42`).

| Injected Defect | Target Dataset & Columns | Injection Mechanism | Detection & Quarantine Rule |
| :--- | :--- | :--- | :--- |
| **`NULL_MANDATORY_FIELD`** | `customers` (customer_id, email), `products` (product_id, name, price), `orders` (order_id, total), `order_items` (id, qty, price), `payments` (id, amount) | Injected empty strings (`""`) or null values at configurable rate (2%). | Checked via `(col IS NULL) | (col == "")`. Routed to quarantine with reason `NULL_MANDATORY_FIELD`. |
| **`DUPLICATE_CUSTOMER_ID`** / **`DUPLICATE_ORDER_ID`** | `customers.customer_id`, `products.product_id`, `orders.order_id`, `order_items.order_item_id`, `payments.payment_id` | Reuses existing primary key with altered row details or duplicate rows (1.5%). | Window `ROW_NUMBER().over(Window.partitionBy(pk).orderBy(date.desc))`. Rows with `row_num > 1` quarantined. |
| **`INVALID_EMAIL_FORMAT`** | `customers.email` | Strips `@` symbol or domain (e.g. `john_doe_at_example_no_domain`). | Regex validation: `~col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")`. Quarantined with `INVALID_EMAIL_FORMAT`. |
| **`MALFORMED_SIGNUP_DATE`** / **`MALFORMED_ORDER_TIMESTAMP`** | `customers.signup_date`, `orders.order_timestamp` | Injected invalid dates such as `"2023-99-99"` or `"2024-02-31 25:61:99"`. | Parsed via `to_date()` / `to_timestamp()`. Rows where parsed value is null but raw string was non-empty are quarantined. |
| **`INVALID_PRICE_NON_POSITIVE`** | `products.unit_price`, `order_items.unit_price` | Injected negative decimal prices (`-10.00`) or `$0.00`. | Evaluated via `col("parsed_unit_price") <= 0`. Quarantined with `INVALID_PRICE_NON_POSITIVE`. |
| **`INVALID_QUANTITY_NON_POSITIVE`** | `order_items.quantity` | Injected `0` or `-1` quantities in line items. | Evaluated via `col("parsed_quantity") <= 0`. Quarantined with `INVALID_QUANTITY_NON_POSITIVE`. |
| **`ORPHAN_CUSTOMER_FK`** | `orders.customer_id` | Set to non-existent ID `CUST-999999`. | Verified via `LEFT JOIN` against `clean_customers`. Quarantined with `ORPHAN_CUSTOMER_FK`. |
| **`ORPHAN_STORE_FK`** | `orders.store_id`, `employees.store_id` | Set to non-existent ID `STR-9999`. | Verified via `LEFT JOIN` against `clean_stores`. Quarantined with `ORPHAN_STORE_FK`. |
| **`ORPHAN_PRODUCT_FK`** | `order_items.product_id` | Set to non-existent ID `PROD-99999`. | Verified via `LEFT JOIN` against `clean_products`. Quarantined with `ORPHAN_PRODUCT_FK`. |
| **`ORPHAN_ORDER_FK`** | `order_items.order_id`, `payments.order_id` | Set to non-existent ID `ORD-9999999`. | Verified via `LEFT JOIN` against `clean_orders`. Quarantined with `ORPHAN_ORDER_FK`. |
| **`PAYMENT_AMOUNT_UNRECONCILED`** | `payments.payment_amount` | Injected extra $25.00 discrepancy compared to `orders.total_amount`. | Compared via `abs(payment_amount - order_total) > 0.01` for SUCCESS payments. Quarantined with `PAYMENT_AMOUNT_UNRECONCILED`. |
| **`INVALID_STATUS`** | `orders.order_status`, `payments.payment_status` | Injected status `"UNKNOWN_STATUS_INVALID"`. | Validated via `~col("status").isin(VALID_STATUSES)`. Quarantined. |

---

## 3. Standardized Quarantine Schema

Every quarantined record is transformed into a uniform schema defined in [retail_schemas.py](file:///Users/vijjureddy/Job%20Switch%20Projects/azure-lakehouse-data-platform/src/schemas/retail_schemas.py#L79-L87):

```python
QUARANTINE_SCHEMA = StructType([
    StructField("record_id", StringType(), True),         # Primary key or Identifier
    StructField("source_dataset", StringType(), False),   # Name of source table (e.g. 'customers')
    StructField("rejection_reason", StringType(), False), # Standardized failure code
    StructField("raw_record", StringType(), False),       # Full raw row serialized as JSON string
    StructField("ingested_at", TimestampType(), False),   # Timestamp when quarantine occurred
])
```

### Quarantine Serialization Implementation
In [rules.py](file:///Users/vijjureddy/Job%20Switch%20Projects/azure-lakehouse-data-platform/src/quality/rules.py#L42-L65):
```python
def format_as_quarantine(df, record_id_col, source_dataset, rejection_reason_col):
    cols_to_json = [c for c in df.columns if c != rejection_reason_col]
    return (
        df.withColumn("record_id", F.col(record_id_col).cast("string"))
          .withColumn("source_dataset", F.lit(source_dataset))
          .withColumn("rejection_reason", F.col(rejection_reason_col))
          .withColumn("raw_record", F.to_json(F.struct([F.col(c) for c in cols_to_json])))
          .withColumn("ingested_at", F.current_timestamp())
          .select("record_id", "source_dataset", "rejection_reason", "raw_record", "ingested_at")
    )
```

---

## 4. Data Quality Reconciliation Invariant

For every dataset processed by the pipeline, row counts must reconcile:

$$\text{source\_row\_count} = \text{valid\_row\_count} + \text{quarantine\_row\_count}$$

*Note on Duplicate Records:* Because duplicate primary keys and duplicate rows are explicitly detected, the original record is retained in `valid_df` and the extra duplicate instances are routed to `quarantine_df`. Thus, the sum of clean records plus quarantined records strictly equals the total raw source row count.

---

## 5. Audit Metrics Table

At the end of pipeline execution, audit metrics across all 8 datasets are consolidated into `QUALITY_METRICS_SCHEMA` and written to `output/metrics/quality_summary/`:

```
+-------------+----------------+---------------+--------------------+---------------+---------------------+-------------------------+
|dataset_name |source_row_count|valid_row_count|quarantine_row_count|duplicate_count|null_mandatory_count |referential_orphan_count |
+-------------+----------------+---------------+--------------------+---------------+---------------------+-------------------------+
|stores       |10              |10             |0                   |0              |0                    |0                        |
|employees    |50              |49             |1                   |0              |0                    |1                        |
|customers    |517             |470            |47                  |13             |11                   |0                        |
|products     |51              |49             |2                   |1              |1                    |0                        |
|orders       |2023            |1879           |144                 |20             |0                    |78                       |
|order_items  |5055            |4738           |317                 |0              |0                    |204                      |
|payments     |2023            |1882           |141                 |20             |0                    |42                       |
|returns      |408             |388            |20                  |0              |0                    |20                       |
+-------------+----------------+---------------+--------------------+---------------+---------------------+-------------------------+
```
*(Example counts from `--scale small` run)*
