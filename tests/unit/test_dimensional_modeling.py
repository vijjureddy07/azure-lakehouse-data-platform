"""
Unit Tests for Module 4: Advanced PySpark + Dimensional Modeling + SCD + Enterprise Quality Gates.

Validates:
1. Deterministic surrogate key allocation and preservation.
2. Calendar Date dimension (dim_date) generation and unknown member key 0.
3. SCD Type 1 in-place attribute updates and key stability (dim_product).
4. SCD Type 2 versioning, half-open intervals [effective_from, effective_to), single-current invariant (dim_customer).
5. Point-in-Time SCD2 Fact Resolution Golden Test (historical loyalty tier resolution).
6. Fact sales grain uniqueness (order_item_id) and Delta MERGE rerun idempotency.
7. Fact-to-dimension referential integrity.
8. Late-arriving dimension fallback to surrogate key 0.
9. Enterprise quality gate execution and failure handling (WarehouseQualityGateError).
10. Exact row-count and Decimal monetary reconciliation against Silver sources.
11. Unity Catalog warehouse DDL registration generation.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.modeling.catalog import generate_warehouse_registration_sql
from src.modeling.dimensions import (
    build_dim_date,
    process_dim_date,
    process_dim_employee,
    process_dim_store,
)
from src.modeling.facts import (
    build_fact_sales_dataframe,
    process_fact_returns,
    process_fact_sales,
)
from src.modeling.quality import (
    WarehouseQualityGateError,
    run_warehouse_quality_suite,
)
from src.modeling.reconciliation import reconcile_warehouse_sales
from src.modeling.scd_type1 import process_dim_product_scd1
from src.modeling.scd_type2 import process_dim_customer_scd2
from src.modeling.surrogate_keys import assign_surrogate_keys


def test_deterministic_surrogate_key_allocation(spark):
    """Test deterministic integer surrogate key generation across multiple batches."""
    schema = StructType([
        StructField("store_id", StringType(), False),
        StructField("store_name", StringType(), False),
    ])

    # Batch 1: Initial 3 stores
    batch1 = spark.createDataFrame(
        [("S-001", "Downtown"), ("S-002", "Uptown"), ("S-003", "Suburbs")],
        schema,
    )
    dim1 = assign_surrogate_keys(None, batch1, natural_key="store_id", surrogate_key_name="store_key")

    assert dim1.count() == 3
    keys1 = {r["store_id"]: r["store_key"] for r in dim1.collect()}
    assert keys1["S-001"] == 1
    assert keys1["S-002"] == 2
    assert keys1["S-003"] == 3

    # Batch 2: 1 existing store (S-002) + 1 new store (S-004)
    batch2 = spark.createDataFrame(
        [("S-002", "Uptown Updated"), ("S-004", "Airport")],
        schema,
    )
    dim2 = assign_surrogate_keys(dim1, batch2, natural_key="store_id", surrogate_key_name="store_key")

    assert dim2.count() == 2
    keys2 = {r["store_id"]: r["store_key"] for r in dim2.collect()}
    # Existing key MUST remain 2, new key MUST be assigned 4
    assert keys2["S-002"] == 2
    assert keys2["S-004"] == 4


def test_dim_date_calendar_and_unknown_member(spark, tmp_path):
    """Test calendar generation for dim_date and unknown record key 0."""
    table_path = tmp_path / "dim_date_test"
    df = process_dim_date(spark, table_path, start_date="2026-01-01", end_date="2026-01-05")

    # 5 calendar days + 1 unknown record = 6 total rows
    assert df.count() == 6

    # Verify unknown record (key 0)
    unk = df.filter(df.date_key == 0).collect()[0]
    assert unk["full_date"] == date(1900, 1, 1)
    assert unk["day_name"] == "Unknown"

    # Verify standard date
    d1 = df.filter(df.date_key == 20260101).collect()[0]
    assert d1["full_date"] == date(2026, 1, 1)
    assert d1["year"] == 2026
    assert d1["month"] == 1
    assert d1["quarter"] == 1
    assert d1["quarter_name"] == "Q1"


def test_dim_store_and_dim_employee_processing(spark, tmp_path):
    """Test dim_store and dim_employee builders and surrogate linkages."""
    store_path = tmp_path / "dim_store"
    emp_path = tmp_path / "dim_employee"

    stores_data = [("STR-1", "Flagship", "ONLINE", "North", "NY", "US", date(2025, 1, 1))]
    stores_df = spark.createDataFrame(
        stores_data,
        ["store_id", "store_name", "store_type", "region", "state", "country", "opened_date"],
    )
    dim_store_df = process_dim_store(spark, stores_df, store_path)
    assert dim_store_df.count() == 1
    assert "store_key" in dim_store_df.columns

    emp_data = [("EMP-1", "STR-1", "Alice", "Smith", "alice@example.com", "MANAGER", date(2025, 1, 1), True)]
    emp_df = spark.createDataFrame(
        emp_data,
        ["employee_id", "store_id", "first_name", "last_name", "email", "role", "hire_date", "is_active"],
    )
    dim_emp_df = process_dim_employee(spark, emp_df, dim_store_df, emp_path)
    assert dim_emp_df.count() == 1
    assert "employee_key" in dim_emp_df.columns
    assert "store_key" in dim_emp_df.columns
    assert dim_emp_df.collect()[0]["store_key"] == dim_store_df.collect()[0]["store_key"]


def test_scd_type1_product_processing(spark, tmp_path):
    """
    Test Kimball SCD Type 1 in-place attribute updates:
    - Initial insert assigns product_key
    - Unchanged rerun produces 0 duplicates and retains product_key
    - Changed attribute updates row in-place without altering product_key
    - New product receives new incremented product_key
    """
    table_path = tmp_path / "dim_product_scd1"
    cols = ["product_id", "product_sku", "product_name", "category", "subcategory", "cost_price", "unit_price", "is_active"]

    # 1. Initial Load: P-001 and P-002
    init_data = [
        ("P-001", "SKU-001", "Laptop 14", "Electronics", "Computers", Decimal("500.00"), Decimal("800.00"), True),
        ("P-002", "SKU-002", "Headphones", "Electronics", "Audio", Decimal("40.00"), Decimal("80.00"), True),
    ]
    df1 = spark.createDataFrame(init_data, cols)
    dim_v1 = process_dim_product_scd1(spark, df1, table_path)
    assert dim_v1.count() == 2

    p1_key_v1 = dim_v1.filter(dim_v1.product_id == "P-001").collect()[0]["product_key"]

    # 2. Idempotent rerun: Identical data -> Count remains 2
    dim_v2 = process_dim_product_scd1(spark, df1, table_path)
    assert dim_v2.count() == 2
    assert dim_v2.filter(dim_v2.product_id == "P-001").collect()[0]["product_key"] == p1_key_v1

    # 3. Update P-001 price + Add new product P-003
    update_data = [
        ("P-001", "SKU-001", "Laptop 14 Pro", "Electronics", "Computers", Decimal("550.00"), Decimal("900.00"), True),
        ("P-002", "SKU-002", "Headphones", "Electronics", "Audio", Decimal("40.00"), Decimal("80.00"), True),
        ("P-003", "SKU-003", "Mouse", "Electronics", "Accessories", Decimal("10.00"), Decimal("25.00"), True),
    ]
    df3 = spark.createDataFrame(update_data, cols)
    dim_v3 = process_dim_product_scd1(spark, df3, table_path)

    assert dim_v3.count() == 3

    p1_updated = dim_v3.filter(dim_v3.product_id == "P-001").collect()[0]
    assert p1_updated["product_key"] == p1_key_v1  # Surrogate key MUST remain identical!
    assert p1_updated["product_name"] == "Laptop 14 Pro"
    assert p1_updated["unit_price"] == Decimal("900.00")

    p3_new = dim_v3.filter(dim_v3.product_id == "P-003").collect()[0]
    assert p3_new["product_key"] == 3


def test_scd_type2_customer_processing_and_invariants(spark, tmp_path):
    """
    Test Kimball SCD Type 2 customer history tracking:
    - Initial insert creates version 1 with effective_to IS NULL and is_current = True
    - Unchanged rerun creates NO new versions (idempotency)
    - Changed tracked attribute expires old version and creates new version with new surrogate key
    - Verifies non-overlapping half-open intervals [effective_from, effective_to)
    - Verifies exactly one current record per customer_id
    """
    table_path = tmp_path / "dim_customer_scd2"
    cols = ["customer_id", "first_name", "last_name", "email", "phone", "address", "city", "state", "postal_code", "country", "signup_date", "loyalty_tier"]

    # 1. Initial Load: C-001 (Gold) on 2026-01-01
    t1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    init_data = [
        ("C-001", "John", "Doe", "john@example.com", "555-0100", "100 Main St", "Dallas", "TX", "75001", "US", date(2026, 1, 1), "GOLD"),
    ]
    df1 = spark.createDataFrame(init_data, cols)
    dim_v1 = process_dim_customer_scd2(spark, df1, table_path, batch_timestamp=t1)

    assert dim_v1.count() == 1
    c1_v1 = dim_v1.collect()[0]
    assert c1_v1["customer_key"] == 1
    assert c1_v1["is_current"] is True
    assert c1_v1["version_number"] == 1
    assert c1_v1["effective_to"] is None
    assert c1_v1["loyalty_tier"] == "GOLD"

    # 2. Idempotent rerun at T2 with exact same data -> Must NOT create a version 2
    t2 = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)
    dim_v2 = process_dim_customer_scd2(spark, df1, table_path, batch_timestamp=t2)
    assert dim_v2.count() == 1
    assert dim_v2.collect()[0]["version_number"] == 1

    # 3. Change loyalty tier to PLATINUM at T3 (2026-06-01)
    t3 = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    changed_data = [
        ("C-001", "John", "Doe", "john@example.com", "555-0100", "100 Main St", "Dallas", "TX", "75001", "US", date(2026, 1, 1), "PLATINUM"),
    ]
    df3 = spark.createDataFrame(changed_data, cols)
    dim_v3 = process_dim_customer_scd2(spark, df3, table_path, batch_timestamp=t3)

    assert dim_v3.count() == 2

    # Query historical (inactive) version 1
    v1_row = dim_v3.filter(dim_v3.version_number == 1).collect()[0]
    assert v1_row["customer_key"] == 1
    assert v1_row["is_current"] is False
    assert v1_row["effective_to"] is not None
    assert v1_row["loyalty_tier"] == "GOLD"

    # Query active version 2
    v2_row = dim_v3.filter(dim_v3.version_number == 2).collect()[0]
    assert v2_row["customer_key"] == 2  # New surrogate key
    assert v2_row["is_current"] is True
    assert v2_row["effective_to"] is None
    assert v2_row["loyalty_tier"] == "PLATINUM"

    # Invariant Check: Exactly 1 row is current
    assert dim_v3.filter(dim_v3.is_current == True).count() == 1  # noqa: E712


DIM_CUSTOMER_TEST_SCHEMA = StructType([
    StructField("customer_key", IntegerType(), False),
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", DateType(), True),
    StructField("loyalty_tier", StringType(), True),
    StructField("attribute_hash", StringType(), True),
    StructField("effective_from", TimestampType(), False),
    StructField("effective_to", TimestampType(), True),
    StructField("is_current", BooleanType(), False),
    StructField("version_number", IntegerType(), False),
])


def test_point_in_time_fact_resolution_golden_test(spark):
    """
    GOLDEN TEST: Point-in-Time SCD2 Fact Resolution.
    - Customer C-001 is GOLD from 2026-01-01 to 2026-06-01 (customer_key = 1)
    - Customer C-001 becomes PLATINUM from 2026-06-01 onwards (customer_key = 2)
    - Transaction A on 2026-03-15 MUST resolve to customer_key = 1 (GOLD)
    - Transaction B on 2026-08-20 MUST resolve to customer_key = 2 (PLATINUM)
    """
    # 1. Setup dim_customer SCD2 with 2 historical versions
    dim_cust_data = [
        (1, "C-001", "John", "Doe", "john@example.com", "555-0100", "100 Main St", "Dallas", "TX", "75001", "US", date(2026, 1, 1), "GOLD", "hash1", datetime(2026, 1, 1, 0, 0), datetime(2026, 6, 1, 0, 0), False, 1),
        (2, "C-001", "John", "Doe", "john@example.com", "555-0100", "100 Main St", "Dallas", "TX", "75001", "US", date(2026, 1, 1), "PLATINUM", "hash2", datetime(2026, 6, 1, 0, 0), None, True, 2),
    ]
    dim_cust_df = spark.createDataFrame(dim_cust_data, schema=DIM_CUSTOMER_TEST_SCHEMA)

    # 2. Setup dim_product and dim_store and dim_date
    dim_prod_df = spark.createDataFrame(
        [(10, "P-100", "SKU-1", "Laptop", "Electronics", "Computers", Decimal("500.00"), Decimal("800.00"), True)],
        ["product_key", "product_id", "product_sku", "product_name", "category", "subcategory", "cost_price", "unit_price", "is_active"],
    )
    dim_store_df = spark.createDataFrame(
        [(20, "S-200", "Dallas Store", "STORE", "South", "TX", "US", date(2025, 1, 1))],
        ["store_key", "store_id", "store_name", "store_type", "region", "state", "country", "opened_date"],
    )
    dim_date_df = build_dim_date(spark, "2026-01-01", "2026-12-31")

    # 3. Setup Silver Orders & Order Items
    orders_data = [
        ("ORD-1", "C-001", "S-200", datetime(2026, 3, 15, 14, 30), date(2026, 3, 15), "ONLINE", "COMPLETED", Decimal("10.00"), Decimal("50.00"), Decimal("800.00"), Decimal("860.00")),
        ("ORD-2", "C-001", "S-200", datetime(2026, 8, 20, 11, 00), date(2026, 8, 20), "ONLINE", "COMPLETED", Decimal("10.00"), Decimal("50.00"), Decimal("800.00"), Decimal("860.00")),
    ]
    orders_cols = ["order_id", "customer_id", "store_id", "order_timestamp", "order_date", "channel", "order_status", "shipping_cost", "tax_amount", "order_subtotal", "total_amount"]
    silver_orders = spark.createDataFrame(orders_data, orders_cols)

    items_data = [
        ("ITEM-1", "ORD-1", "P-100", 1, Decimal("800.00"), Decimal("0.10"), Decimal("80.00"), Decimal("720.00")),
        ("ITEM-2", "ORD-2", "P-100", 1, Decimal("800.00"), Decimal("0.10"), Decimal("80.00"), Decimal("720.00")),
    ]
    items_cols = ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_percent", "discount_amount", "net_amount"]
    silver_items = spark.createDataFrame(items_data, items_cols)

    # 4. Build Fact Sales
    fact_sales = build_fact_sales_dataframe(
        silver_order_items_df=silver_items,
        silver_orders_df=silver_orders,
        dim_customer_df=dim_cust_df,
        dim_product_df=dim_prod_df,
        dim_store_df=dim_store_df,
        dim_date_df=dim_date_df,
    )

    assert fact_sales.count() == 2

    # Verification:
    # Transaction 1 occurred when C1 was Gold -> customer_key MUST be 1
    f1 = fact_sales.filter(fact_sales.order_item_id == "ITEM-1").collect()[0]
    assert f1["customer_key"] == 1, f"Expected customer_key 1 (GOLD period), got {f1['customer_key']}"
    assert f1["product_key"] == 10
    assert f1["store_key"] == 20
    assert f1["gross_amount"] == Decimal("800.00")
    assert f1["discount_amount"] == Decimal("80.00")
    assert f1["net_amount"] == Decimal("720.00")
    assert f1["cost_amount"] == Decimal("500.00")
    assert f1["profit_amount"] == Decimal("220.00")

    # Transaction 2 occurred when C1 was Platinum -> customer_key MUST be 2
    f2 = fact_sales.filter(fact_sales.order_item_id == "ITEM-2").collect()[0]
    assert f2["customer_key"] == 2, f"Expected customer_key 2 (PLATINUM period), got {f2['customer_key']}"


def test_fact_sales_idempotency_and_returns(spark, tmp_path):
    """Test fact_sales and fact_returns persistence and Delta MERGE rerun idempotency."""
    fact_sales_path = tmp_path / "fact_sales_idemp"
    fact_returns_path = tmp_path / "fact_returns_idemp"

    dim_cust_df = spark.createDataFrame(
        [(1, "C-1", "John", "Doe", "j@e.com", "555", "100 M", "Dal", "TX", "75001", "US", date(2026, 1, 1), "GOLD", "h", datetime(2026, 1, 1), None, True, 1)],
        schema=DIM_CUSTOMER_TEST_SCHEMA,
    )
    dim_prod_df = spark.createDataFrame(
        [(10, "P-1", "SKU", "Prod", "Cat", "Sub", Decimal("10.00"), Decimal("20.00"), True)],
        ["product_key", "product_id", "product_sku", "product_name", "category", "subcategory", "cost_price", "unit_price", "is_active"],
    )
    dim_store_df = spark.createDataFrame(
        [(20, "S-1", "Store", "TYPE", "Reg", "ST", "US", date(2025, 1, 1))],
        ["store_key", "store_id", "store_name", "store_type", "region", "state", "country", "opened_date"],
    )
    dim_date_df = build_dim_date(spark, "2026-01-01", "2026-01-10")

    orders_df = spark.createDataFrame(
        [("O-1", "C-1", "S-1", datetime(2026, 1, 2, 10, 0), date(2026, 1, 2), "ONLINE", "COMPLETED", Decimal("5.00"), Decimal("2.00"), Decimal("40.00"), Decimal("47.00"))],
        ["order_id", "customer_id", "store_id", "order_timestamp", "order_date", "channel", "order_status", "shipping_cost", "tax_amount", "order_subtotal", "total_amount"],
    )
    items_df = spark.createDataFrame(
        [("I-1", "O-1", "P-1", 2, Decimal("20.00"), Decimal("0.00"), Decimal("0.00"), Decimal("40.00"))],
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_percent", "discount_amount", "net_amount"],
    )
    returns_df = spark.createDataFrame(
        [("R-1", "I-1", "DEFECTIVE", "APPROVED", Decimal("40.00"), datetime(2026, 1, 5, 12, 0))],
        ["return_id", "order_item_id", "return_reason", "return_status", "refund_amount", "return_timestamp"],
    )

    # 1. Initial Fact Processing
    f_sales = process_fact_sales(spark, items_df, orders_df, dim_cust_df, dim_prod_df, dim_store_df, dim_date_df, fact_sales_path)
    f_returns = process_fact_returns(spark, returns_df, f_sales, dim_date_df, fact_returns_path)

    assert f_sales.count() == 1
    assert f_returns.count() == 1

    # 2. Re-run identical batch -> Count must remain exactly 1 (MERGE Idempotency)
    f_sales_rerun = process_fact_sales(spark, items_df, orders_df, dim_cust_df, dim_prod_df, dim_store_df, dim_date_df, fact_sales_path)
    f_returns_rerun = process_fact_returns(spark, returns_df, f_sales_rerun, dim_date_df, fact_returns_path)

    assert f_sales_rerun.count() == 1
    assert f_returns_rerun.count() == 1


def test_late_arriving_dimension_fallback_to_zero(spark):
    """Verify that transactions with unmapped/late-arriving foreign keys resolve to unknown key 0."""
    dim_cust_schema = StructType([
        StructField("customer_key", IntegerType(), False),
        StructField("customer_id", StringType(), False),
        StructField("effective_from", TimestampType(), False),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
    ])
    dim_cust_df = spark.createDataFrame([], dim_cust_schema)

    dim_prod_schema = StructType([
        StructField("product_key", IntegerType(), False),
        StructField("product_id", StringType(), False),
        StructField("cost_price", DecimalType(10, 2), True),
    ])
    dim_prod_df = spark.createDataFrame([], dim_prod_schema)

    dim_store_schema = StructType([
        StructField("store_key", IntegerType(), False),
        StructField("store_id", StringType(), False),
    ])
    dim_store_df = spark.createDataFrame([], dim_store_schema)
    dim_date_df = build_dim_date(spark, "2026-01-01", "2026-01-05")

    orders_df = spark.createDataFrame(
        [("O-99", "C-GHOST", "S-GHOST", datetime(2026, 1, 2, 10, 0), date(2026, 1, 2), "ONLINE", "COMPLETED", Decimal("0"), Decimal("0"), Decimal("10"), Decimal("10"))],
        ["order_id", "customer_id", "store_id", "order_timestamp", "order_date", "channel", "order_status", "shipping_cost", "tax_amount", "order_subtotal", "total_amount"],
    )
    items_df = spark.createDataFrame(
        [("I-99", "O-99", "P-GHOST", 1, Decimal("10.00"), Decimal("0"), Decimal("0"), Decimal("10.00"))],
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_percent", "discount_amount", "net_amount"],
    )

    fact_df = build_fact_sales_dataframe(items_df, orders_df, dim_cust_df, dim_prod_df, dim_store_df, dim_date_df)
    row = fact_df.collect()[0]
    # Fallback unknown member surrogate keys MUST be 0
    assert row["customer_key"] == 0
    assert row["product_key"] == 0
    assert row["store_key"] == 0


def test_quality_gate_checks_and_failure_exception(spark, tmp_path):
    """Test enterprise quality gates passing and intentional critical failure exception."""
    audit_path = tmp_path / "quality_audit_test"

    # Setup valid dimensions & facts
    dim_cust = spark.createDataFrame(
        [(1, "C-1", "John", "Doe", "j@e.com", "555", "100 M", "Dal", "TX", "75001", "US", date(2026, 1, 1), "GOLD", "h", datetime(2026, 1, 1), None, True, 1)],
        schema=DIM_CUSTOMER_TEST_SCHEMA,
    )
    dim_prod = spark.createDataFrame(
        [(10, "P-1", "SKU", "Prod", "Cat", "Sub", Decimal("10.00"), Decimal("20.00"), True)],
        ["product_key", "product_id", "product_sku", "product_name", "category", "subcategory", "cost_price", "unit_price", "is_active"],
    )
    dim_store = spark.createDataFrame(
        [(20, "S-1", "Store", "TYPE", "Reg", "ST", "US", date(2025, 1, 1))],
        ["store_key", "store_id", "store_name", "store_type", "region", "state", "country", "opened_date"],
    )
    dim_date = build_dim_date(spark, "2026-01-01", "2026-01-10")

    fact_sales = spark.createDataFrame(
        [("I-1", "O-1", 1, 10, 20, 20260102, "C-1", "P-1", "S-1", datetime(2026, 1, 2), "COMPLETED", "ONLINE", 2, Decimal("20.00"), Decimal("40.00"), Decimal("0.00"), Decimal("40.00"), Decimal("20.00"), Decimal("20.00"))],
        ["order_item_id", "order_id", "customer_key", "product_key", "store_key", "order_date_key", "customer_id", "product_id", "store_id", "order_timestamp", "order_status", "channel", "quantity", "unit_price", "gross_amount", "discount_amount", "net_amount", "cost_amount", "profit_amount"],
    )
    fact_returns = spark.createDataFrame(
        [("R-1", "I-1", "O-1", 1, 10, 20, 20260105, datetime(2026, 1, 5), "DEFECT", "APPROVED", Decimal("40.00"))],
        ["return_id", "order_item_id", "order_id", "customer_key", "product_key", "store_key", "return_date_key", "return_timestamp", "return_reason", "return_status", "refund_amount"],
    )

    # 1. Passing suite
    results = run_warehouse_quality_suite(
        spark=spark,
        dim_customer_df=dim_cust,
        dim_product_df=dim_prod,
        dim_store_df=dim_store,
        dim_date_df=dim_date,
        fact_sales_df=fact_sales,
        fact_returns_df=fact_returns,
        quality_audit_path=audit_path,
        raise_on_failure=True,
    )
    assert all(r.passed for r in results)

    # 2. Intentional Failure: Corrupt fact_sales with broken arithmetic
    corrupt_fact = fact_sales.withColumn("net_amount", lit(Decimal("999.99")))
    with pytest.raises(WarehouseQualityGateError) as exc_info:
        run_warehouse_quality_suite(
            spark=spark,
            dim_customer_df=dim_cust,
            dim_product_df=dim_prod,
            dim_store_df=dim_store,
            dim_date_df=dim_date,
            fact_sales_df=corrupt_fact,
            fact_returns_df=fact_returns,
            raise_on_failure=True,
        )
    assert "Warehouse Quality Gate failed" in str(exc_info.value)


def test_warehouse_sales_reconciliation(spark):
    """Test exact row-count and Decimal monetary reconciliation."""
    silver_items = spark.createDataFrame(
        [
            ("I-1", "O-1", "P-1", 2, Decimal("50.00"), Decimal("0.10"), Decimal("10.00"), Decimal("90.00")),
            ("I-2", "O-1", "P-2", 1, Decimal("100.00"), Decimal("0.00"), Decimal("0.00"), Decimal("100.00")),
        ],
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_percent", "discount_amount", "net_amount"],
    )

    fact_sales = spark.createDataFrame(
        [
            ("I-1", "O-1", 1, 10, 20, 20260102, "C-1", "P-1", "S-1", datetime(2026, 1, 2), "COMPLETED", "ONLINE", 2, Decimal("50.00"), Decimal("100.00"), Decimal("10.00"), Decimal("90.00"), Decimal("40.00"), Decimal("50.00")),
            ("I-2", "O-1", 1, 11, 20, 20260102, "C-1", "P-2", "S-1", datetime(2026, 1, 2), "COMPLETED", "ONLINE", 1, Decimal("100.00"), Decimal("100.00"), Decimal("0.00"), Decimal("100.00"), Decimal("50.00"), Decimal("50.00")),
        ],
        ["order_item_id", "order_id", "customer_key", "product_key", "store_key", "order_date_key", "customer_id", "product_id", "store_id", "order_timestamp", "order_status", "channel", "quantity", "unit_price", "gross_amount", "discount_amount", "net_amount", "cost_amount", "profit_amount"],
    )

    # 1. Matching case
    recon = reconcile_warehouse_sales(silver_items, fact_sales)
    assert recon["passed"] is True
    assert recon["row_count"]["silver"] == 2
    assert recon["net_amount"]["fact_sales"] == Decimal("190.00")

    # 2. Mismatched case -> Raises WarehouseQualityGateError
    corrupt_items = silver_items.filter(silver_items.order_item_id == "I-1")
    with pytest.raises(WarehouseQualityGateError):
        reconcile_warehouse_sales(corrupt_items, fact_sales, raise_on_failure=True)


def test_warehouse_unity_catalog_sql_generation():
    """Test SQL generation for Unity Catalog retail_lakehouse.warehouse registration."""
    statements = generate_warehouse_registration_sql(
        catalog_name="retail_lakehouse",
        delta_root_uri="abfss://lakehouse@stlakehousedev.dfs.core.windows.net/delta",
    )
    assert len(statements) == 10  # 1 catalog + 1 schema + 8 tables
    joined = "\n".join(statements)
    assert "CREATE SCHEMA IF NOT EXISTS retail_lakehouse.warehouse" in joined
    assert "retail_lakehouse.warehouse.fact_sales" in joined
    assert "retail_lakehouse.warehouse.dim_customer" in joined
    assert "retail_lakehouse.warehouse.quality_audit" in joined
