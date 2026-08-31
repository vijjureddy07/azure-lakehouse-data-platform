import pytest

from src.quality.rules import (
    DataQualityReconciliationError,
    DatasetQualityMetric,
    find_orphans,
    metrics_to_dataframe,
    validate_reconciliation,
)


def test_find_orphans_left_anti_join(spark):
    """Verify find_orphans correctly isolates missing parent keys."""
    child_df = spark.createDataFrame([
        ("ORD-1", "CUST-A"),
        ("ORD-2", "CUST-B"),
        ("ORD-3", "CUST-ORPHAN"),
    ], ["order_id", "customer_id"])

    parent_df = spark.createDataFrame([
        ("CUST-A", "Customer A"),
        ("CUST-B", "Customer B"),
    ], ["customer_id", "name"])

    orphans_df = find_orphans(child_df, parent_df, "customer_id", "customer_id")
    orphan_rows = orphans_df.collect()

    assert len(orphan_rows) == 1
    assert orphan_rows[0]["order_id"] == "ORD-3"
    assert orphan_rows[0]["customer_id"] == "CUST-ORPHAN"


def test_metrics_to_dataframe(spark):
    """Verify metrics conversion produces valid PySpark DataFrame."""
    metrics = [
        DatasetQualityMetric("customers", 100, 95, 5, 2, 3, 0),
        DatasetQualityMetric("orders", 500, 480, 20, 5, 5, 10),
    ]
    df = metrics_to_dataframe(spark, metrics)
    assert df.count() == 2
    assert "source_row_count" in df.columns
    assert "quarantine_row_count" in df.columns


def test_validate_reconciliation_success():
    """Verify validate_reconciliation passes when source_row_count == valid + quarantine."""
    valid_metrics = [
        DatasetQualityMetric("customers", source_row_count=100, valid_row_count=95, quarantine_row_count=5),
        DatasetQualityMetric("orders", source_row_count=500, valid_row_count=480, quarantine_row_count=20),
    ]
    # Should execute without raising any exception
    validate_reconciliation(valid_metrics)


def test_validate_reconciliation_mismatch_raises_error():
    """Verify validate_reconciliation raises DataQualityReconciliationError on mismatch."""
    mismatched_metrics = [
        DatasetQualityMetric("customers", source_row_count=100, valid_row_count=95, quarantine_row_count=5),
        DatasetQualityMetric("orders", source_row_count=500, valid_row_count=450, quarantine_row_count=20),  # Sum is 470 != 500
    ]
    with pytest.raises(DataQualityReconciliationError) as exc_info:
        validate_reconciliation(mismatched_metrics)

    assert "orders" in str(exc_info.value)
    assert "source_row_count=500" in str(exc_info.value)
    assert "valid_row_count=450" in str(exc_info.value)
    assert "quarantine_row_count=20" in str(exc_info.value)
