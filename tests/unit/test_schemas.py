"""
Unit tests for explicit PySpark StructType schemas.
"""

from pyspark.sql.types import (
    TimestampType,
)

from src.schemas.retail_schemas import (
    QUALITY_METRICS_SCHEMA,
    QUARANTINE_SCHEMA,
    RAW_CUSTOMERS_SCHEMA,
    RAW_EMPLOYEES_SCHEMA,
    RAW_ORDER_ITEMS_SCHEMA,
    RAW_ORDERS_SCHEMA,
    RAW_PAYMENTS_SCHEMA,
    RAW_PRODUCTS_SCHEMA,
    RAW_RETURNS_SCHEMA,
    RAW_STORES_SCHEMA,
)


def test_schema_field_presence():
    """Verify all 8 source schemas have required primary and foreign keys."""
    assert "customer_id" in RAW_CUSTOMERS_SCHEMA.fieldNames()
    assert "product_id" in RAW_PRODUCTS_SCHEMA.fieldNames()
    assert "store_id" in RAW_STORES_SCHEMA.fieldNames()
    assert "employee_id" in RAW_EMPLOYEES_SCHEMA.fieldNames()
    assert "order_id" in RAW_ORDERS_SCHEMA.fieldNames()
    assert "order_item_id" in RAW_ORDER_ITEMS_SCHEMA.fieldNames()
    assert "payment_id" in RAW_PAYMENTS_SCHEMA.fieldNames()
    assert "return_id" in RAW_RETURNS_SCHEMA.fieldNames()


def test_quarantine_schema_structure():
    """Verify quarantine schema contains standard audit fields."""
    expected_fields = ["record_id", "source_dataset", "rejection_reason", "raw_record", "ingested_at"]
    assert QUARANTINE_SCHEMA.fieldNames() == expected_fields
    assert isinstance(QUARANTINE_SCHEMA["ingested_at"].dataType, TimestampType)


def test_quality_metrics_schema_structure():
    """Verify quality metrics schema contains reconciliation fields."""
    expected_fields = [
        "dataset_name",
        "source_row_count",
        "valid_row_count",
        "quarantine_row_count",
        "duplicate_count",
        "null_mandatory_count",
        "referential_orphan_count",
        "calculated_at",
    ]
    assert QUALITY_METRICS_SCHEMA.fieldNames() == expected_fields
