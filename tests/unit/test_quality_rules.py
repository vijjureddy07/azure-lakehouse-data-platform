"""
Unit tests for data quality rules, orphan finder, and metrics dataframe formatting.
"""

from src.quality.rules import DatasetQualityMetric, find_orphans, metrics_to_dataframe


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
