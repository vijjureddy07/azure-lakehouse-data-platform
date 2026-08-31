"""
Explicit PySpark Schemas for Retail Lakehouse Datasets (Module 1).

Follows strict Data Engineering principles:
- Explicit StructType definitions (no inferSchema in production pipelines)
- DecimalType(10, 2) / DecimalType(12, 2) for currency and financial calculations
- Clear separation between Raw Ingestion schemas, Cleaned Target schemas,
  and Quarantine schemas.
"""

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ==============================================================================
# RAW INGESTION SCHEMAS (Matches Source Files: CSV/JSON)
# Strings are used for fields with potential formatting/casing defects to allow
# graceful ingestion and explicit quarantine/transformation without silent drops.
# ==============================================================================

RAW_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("loyalty_tier", StringType(), True),
])

RAW_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_sku", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("cost_price", StringType(), True),
    StructField("is_active", StringType(), True),
    StructField("description", StringType(), True),
])

RAW_STORES_SCHEMA = StructType([
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("store_type", StringType(), True),
    StructField("region", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("opened_date", StringType(), True),
])

RAW_EMPLOYEES_SCHEMA = StructType([
    StructField("employee_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("role", StringType(), True),
    StructField("hire_date", StringType(), True),
    StructField("is_active", StringType(), True),
])

RAW_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("employee_id", StringType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("shipping_cost", StringType(), True),
    StructField("tax_amount", StringType(), True),
    StructField("order_subtotal", StringType(), True),
    StructField("total_amount", StringType(), True),
])

RAW_ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_item_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("discount_percent", StringType(), True),
])

RAW_PAYMENTS_SCHEMA = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("payment_timestamp", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_amount", StringType(), True),
    StructField("transaction_reference", StringType(), True),
])

RAW_RETURNS_SCHEMA = StructType([
    StructField("return_id", StringType(), True),
    StructField("order_item_id", StringType(), True),
    StructField("return_timestamp", StringType(), True),
    StructField("return_reason", StringType(), True),
    StructField("refund_amount", StringType(), True),
    StructField("return_status", StringType(), True),
])

# ==============================================================================
# QUARANTINE SCHEMA (Uniform audit structure for rejected records)
# ==============================================================================

QUARANTINE_SCHEMA = StructType([
    StructField("record_id", StringType(), True),
    StructField("source_dataset", StringType(), False),
    StructField("rejection_reason", StringType(), False),
    StructField("raw_record", StringType(), False),
    StructField("ingested_at", TimestampType(), False),
])

# ==============================================================================
# QUALITY METRICS SCHEMA (Audit summary table)
# ==============================================================================

QUALITY_METRICS_SCHEMA = StructType([
    StructField("dataset_name", StringType(), False),
    StructField("source_row_count", LongType(), False),
    StructField("valid_row_count", LongType(), False),
    StructField("quarantine_row_count", LongType(), False),
    StructField("duplicate_count", LongType(), False),
    StructField("null_mandatory_count", LongType(), False),
    StructField("referential_orphan_count", LongType(), False),
    StructField("calculated_at", TimestampType(), False),
])
