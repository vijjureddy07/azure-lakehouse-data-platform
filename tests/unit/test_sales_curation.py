"""
Unit tests for curated sales dataset generation, decimal calculations, and financial derivations.
"""

from datetime import date
from decimal import Decimal

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.transformations.sales import build_curated_sales


def test_build_curated_sales_financials_and_windows(spark):
    """Verify joins, financial metrics (gross, discount, net, profit), and window derivations."""
    orders_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("store_id", StringType(), False),
        StructField("employee_id", StringType(), False),
        StructField("order_timestamp", StringType(), False),
        StructField("order_date", DateType(), False),
        StructField("order_status", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("shipping_cost", DecimalType(10, 2), False),
        StructField("tax_amount", DecimalType(10, 2), False),
        StructField("order_subtotal", DecimalType(12, 2), False),
        StructField("total_amount", DecimalType(12, 2), False),
    ])
    clean_orders = spark.createDataFrame([
        ("ORD-1", "CUST-1", "STR-1", "EMP-1", "2023-05-01 10:00:00", date(2023, 5, 1), "COMPLETED", "WEB", Decimal("0.00"), Decimal("8.00"), Decimal("100.00"), Decimal("108.00")),
        ("ORD-2", "CUST-1", "STR-1", "EMP-1", "2023-06-01 11:00:00", date(2023, 6, 1), "COMPLETED", "WEB", Decimal("0.00"), Decimal("4.00"), Decimal("50.00"), Decimal("54.00")),
    ], schema=orders_schema)

    items_schema = StructType([
        StructField("order_item_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(10, 2), False),
        StructField("discount_percent", DecimalType(5, 2), False),
    ])
    clean_items = spark.createDataFrame([
        ("ITEM-1", "ORD-1", "PROD-1", 2, Decimal("50.00"), Decimal("0.10")),  # Gross 100.00, Disc 10.00, Net 90.00
        ("ITEM-2", "ORD-2", "PROD-1", 1, Decimal("50.00"), Decimal("0.00")),  # Gross 50.00, Disc 0.00, Net 50.00
    ], schema=items_schema)

    prods_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("product_sku", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("subcategory", StringType(), False),
        StructField("unit_price", DecimalType(10, 2), False),
        StructField("cost_price", DecimalType(10, 2), False),
        StructField("is_active", StringType(), False),
        StructField("description", StringType(), False),
    ])
    clean_prods = spark.createDataFrame([
        ("PROD-1", "SKU-1", "Product 1", "Electronics", "Gadgets", Decimal("50.00"), Decimal("20.00"), "True", "Desc"),
    ], schema=prods_schema)

    cust_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("full_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("address", StringType(), False),
        StructField("city", StringType(), False),
        StructField("state", StringType(), False),
        StructField("country", StringType(), False),
        StructField("postal_code", StringType(), False),
        StructField("signup_date", DateType(), False),
        StructField("loyalty_tier", StringType(), False),
    ])
    clean_cust = spark.createDataFrame([
        ("CUST-1", "Alice", "Smith", "Alice Smith", "alice@example.com", "555-0100", "123 St", "City", "CA", "US", "90210", date(2023, 1, 1), "GOLD"),
    ], schema=cust_schema)

    stores_schema = StructType([
        StructField("store_id", StringType(), False),
        StructField("store_name", StringType(), False),
        StructField("store_type", StringType(), False),
        StructField("region", StringType(), False),
        StructField("state", StringType(), False),
        StructField("country", StringType(), False),
        StructField("opened_date", DateType(), False),
    ])
    clean_stores = spark.createDataFrame([
        ("STR-1", "Main Store", "Flagship", "West", "CA", "US", date(2020, 1, 1)),
    ], schema=stores_schema)

    curated_df = build_curated_sales(clean_orders, clean_items, clean_prods, clean_cust, clean_stores)
    rows = curated_df.orderBy("order_date").collect()

    assert len(rows) == 2

    # Row 1 (ITEM-1)
    r1 = rows[0]
    assert r1["gross_sales"] == Decimal("100.00")
    assert r1["discount_amount"] == Decimal("10.00")
    assert r1["net_sales"] == Decimal("90.00")
    assert r1["gross_profit"] == Decimal("50.00")  # 90.00 net - (2 * 20.00 cost)
    assert r1["customer_order_sequence"] == 1
    assert r1["customer_running_spend"] == Decimal("90.00")
    assert r1["order_year"] == 2023
    assert r1["order_month"] == 5

    # Row 2 (ITEM-2)
    r2 = rows[1]
    assert r2["gross_sales"] == Decimal("50.00")
    assert r2["discount_amount"] == Decimal("0.00")
    assert r2["net_sales"] == Decimal("50.00")
    assert r2["gross_profit"] == Decimal("30.00")  # 50.00 net - (1 * 20.00 cost)
    assert r2["customer_order_sequence"] == 2
    assert r2["customer_running_spend"] == Decimal("140.00")  # 90.00 + 50.00
    assert r2["days_since_prior_order"] == 31  # May 1 to June 1 is 31 days
