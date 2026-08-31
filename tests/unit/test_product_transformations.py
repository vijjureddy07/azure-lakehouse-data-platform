"""
Unit tests for product transformations and price validation.
"""

from decimal import Decimal

from src.schemas.retail_schemas import RAW_PRODUCTS_SCHEMA
from src.transformations.products import transform_products


def test_product_cleaning_and_price_validation(spark):
    """Verify monetary casting, non-positive price rejection, and duplicate filtering."""
    sample_data = [
        # Valid product
        ("PROD-001", "SKU-001", " Wireless Mouse ", " ELECTRONICS ", " accessories ", " 29.99 ", "15.00", "True", "A great mouse"),
        # Negative price defect
        ("PROD-002", "SKU-002", "Broken Product", "Electronics", "Accessories", "-10.00", "5.00", "True", "Defective"),
        # Duplicate product_id
        ("PROD-001", "SKU-001B", "Duplicate Mouse", "Electronics", "Accessories", "35.00", "18.00", "True", "Duplicate"),
        # Null mandatory product_name
        ("PROD-003", "SKU-003", "", "Beauty", "Skincare", "19.99", "8.00", "True", "Null name"),
    ]

    raw_df = spark.createDataFrame(sample_data, schema=RAW_PRODUCTS_SCHEMA)
    clean_df, quarantine_df, metric = transform_products(raw_df)

    clean_rows = clean_df.collect()
    quar_rows = quarantine_df.collect()

    assert len(clean_rows) == 1
    assert clean_rows[0]["product_id"] == "PROD-001"
    assert clean_rows[0]["category"] == "Electronics"
    assert clean_rows[0]["subcategory"] == "Accessories"
    assert clean_rows[0]["unit_price"] == Decimal("29.99")
    assert clean_rows[0]["is_active"] is True

    assert len(quar_rows) == 3
    reasons = {r["rejection_reason"] for r in quar_rows}
    assert "INVALID_PRICE_NON_POSITIVE" in reasons
    assert "DUPLICATE_PRODUCT_ID" in reasons
    assert "NULL_MANDATORY_FIELD" in reasons

    assert metric.source_row_count == 4
    assert metric.valid_row_count == 1
    assert metric.quarantine_row_count == 3
