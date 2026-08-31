"""
Unit tests for Spark SQL queries and analytics execution on test fixtures.
"""

from datetime import date
from decimal import Decimal

from src.config.settings import SQL_DIR


def test_spark_sql_query_execution(spark):
    """Verify all SQL scripts in sql/ directory execute without syntax error on registered temporary views."""
    # Create minimal fixture for v_curated_sales
    sales_data = [
        ("ORD-1", "ITEM-1", "PROD-1", "Laptop", "Electronics", "CUST-1", "Alice", "alice@example.com", "CA", "GOLD", "STR-1", "Store 1", "Flagship", "West", "CA", "WEB", 2, Decimal("100.00"), Decimal("10.00"), Decimal("90.00"), Decimal("40.00"), Decimal("50.00"), date(2023, 5, 1), 2023, 5, 1, 1, Decimal("90.00"), 0, 1),
    ]
    columns = [
        "order_id", "order_item_id", "product_id", "product_name", "category", "customer_id", "customer_name", "customer_email", "customer_state", "loyalty_tier", "store_id", "store_name", "store_type", "store_region", "store_state", "channel", "quantity", "gross_sales", "discount_amount", "net_sales", "cost_amount", "gross_profit", "order_date", "order_year", "order_month", "order_day", "customer_order_sequence", "customer_running_spend", "days_since_prior_order", "category_product_rank"
    ]
    curated_df = spark.createDataFrame(sales_data, columns)
    curated_df.createOrReplaceTempView("v_curated_sales")

    returns_df = spark.createDataFrame([
        ("RET-1", "ITEM-1", "2023-05-05 10:00:00", "DEFECTIVE", Decimal("45.00"), "APPROVED")
    ], ["return_id", "order_item_id", "return_timestamp", "return_reason", "refund_amount", "return_status"])
    returns_df.createOrReplaceTempView("v_returns")

    # Run each SQL script
    for sql_path in SQL_DIR.glob("*.sql"):
        with open(sql_path, "r", encoding="utf-8") as f:
            query = f.read()
        res_df = spark.sql(query)
        assert res_df is not None
        assert res_df.count() >= 0
