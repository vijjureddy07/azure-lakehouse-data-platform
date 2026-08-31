"""
Unit tests for customer transformations, normalization, and quality validation.
"""

from src.schemas.retail_schemas import RAW_CUSTOMERS_SCHEMA
from src.transformations.customers import transform_customers


def test_customer_cleaning_and_quarantine(spark):
    """Verify whitespace trimming, email validation, duplicate rejection, and quarantine routing."""
    sample_data = [
        # Valid customer with whitespace
        ("CUST-001", " John ", "Doe ", " JOHN.DOE@EXAMPLE.COM ", "+1-555-0100", "123 Main St", "Metropolis", "ca", "US", "90210", "2023-05-10", "gold"),
        # Duplicate of CUST-001
        ("CUST-001", "Johnny", "Doe", "john.doe@example.com", "+1-555-0100", "123 Main St", "Metropolis", "CA", "US", "90210", "2023-01-01", "silver"),
        # Invalid email format
        ("CUST-002", "Jane", "Smith", "invalid_email_at_nowhere", "+1-555-0200", "456 Oak St", "Gotham", "NY", "US", "10001", "2023-06-15", "STANDARD"),
        # Null mandatory customer_id
        ("", "No", "Id", "noid@example.com", "+1-555-0300", "789 Pine St", "Star City", "WA", "US", "98101", "2023-07-20", "STANDARD"),
        # Malformed signup date
        ("CUST-003", "Alice", "Wonder", "alice@example.com", "+1-555-0400", "321 Elm St", "Central City", "TX", "US", "75001", "2023-99-99", "SILVER"),
    ]

    raw_df = spark.createDataFrame(sample_data, schema=RAW_CUSTOMERS_SCHEMA)
    clean_df, quarantine_df, metric = transform_customers(raw_df)

    clean_rows = clean_df.collect()
    quar_rows = quarantine_df.collect()

    # Only CUST-001 (first record) should be valid
    assert len(clean_rows) == 1
    valid_cust = clean_rows[0]
    assert valid_cust["customer_id"] == "CUST-001"
    assert valid_cust["first_name"] == "John"
    assert valid_cust["last_name"] == "Doe"
    assert valid_cust["email"] == "john.doe@example.com"
    assert valid_cust["state"] == "CA"
    assert valid_cust["loyalty_tier"] == "GOLD"

    # 4 records quarantined
    assert len(quar_rows) == 4
    reasons = {r["rejection_reason"] for r in quar_rows}
    assert "DUPLICATE_CUSTOMER_ID" in reasons
    assert "INVALID_EMAIL_FORMAT" in reasons
    assert "NULL_MANDATORY_FIELD" in reasons
    assert "MALFORMED_SIGNUP_DATE" in reasons

    # Metric verification
    assert metric.source_row_count == 5
    assert metric.valid_row_count == 1
    assert metric.quarantine_row_count == 4
