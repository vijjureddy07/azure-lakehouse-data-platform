"""
Unit Tests for Module 3: Azure Databricks + Delta Lake + Medallion Lakehouse.

Validates:
1. Delta Lake write, read, and history inspection.
2. Ingestion audit logging and incremental landing file discovery (local and cloud ABFSS path parsing).
3. Multi-record JSON Lines Bronze ingestion without multiLine bug.
4. Bronze lineage metadata columns (_source_file, _source_path, _ingestion_date, _adf_run_id, _ingested_timestamp).
5. Exact financial discount calculation (discount_amount = qty * price * discount_percent) and Gold revenue flow.
6. Deterministic deduplication with content-hash tie-breaking across identical timestamps.
7. Silver quarantine routing and runtime reconciliation validator (passing + intentional failure raising ReconciliationError).
8. Idempotent Delta MERGE operations using actual Silver schema.
9. Delta table history (DESCRIBE HISTORY) and Time Travel queries (VERSION AS OF).
10. Schema enforcement failure and controlled schema evolution (mergeSchema = true).
11. Delta table existence verification via DeltaTable.isDeltaTable.
12. Unity Catalog registration SQL generation across the 3-level namespace.
13. Gold analytical KPI aggregations correctness.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from delta.tables import DeltaTable
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
)

from src.config.settings import ScaleConfig
from src.data_generation.generate_retail_data import generate_all_datasets
from src.delta.features import (
    demonstrate_schema_enforcement,
    demonstrate_schema_evolution,
    get_delta_history,
    inspect_transaction_log,
    query_time_travel_by_version,
)
from src.medallion.bronze import ingest_bronze_layer, load_bronze_table
from src.medallion.catalog import generate_unity_catalog_registration_sql
from src.medallion.discovery import (
    discover_landing_files,
    filter_uningested_files,
    parse_landing_path,
    record_ingested_files,
)
from src.medallion.gold import (
    build_gold_daily_sales,
    process_gold_layer,
)
from src.medallion.merge import upsert_customers, upsert_delta_table
from src.medallion.silver import (
    ReconciliationError,
    process_silver_layer,
    transform_silver_customers,
    transform_silver_order_items,
    validate_silver_reconciliation,
)


@pytest.fixture(scope="module")
def sample_landing_dir(tmp_path_factory):
    """Generate structured synthetic landing files for testing."""
    base_dir = tmp_path_factory.mktemp("landing_test")
    raw_temp = base_dir / "_temp"
    scale = ScaleConfig(
        name="test_scale",
        num_customers=50,
        num_products=20,
        num_stores=5,
        num_employees=10,
        num_orders=80,
        max_items_per_order=3,
        return_rate=0.15,
        seed=123,
    )
    generate_all_datasets(scale, raw_temp)

    landing_root = base_dir / "landing"
    date_str = "2026-08-31"
    run_id = "test-run-001"

    for file_path in raw_temp.glob("*"):
        if file_path.is_file():
            ds_name = file_path.stem.split(".")[0]
            dest = landing_root / "retail" / ds_name / f"ingestion_date={date_str}" / f"run_id={run_id}"
            dest.mkdir(parents=True, exist_ok=True)
            (dest / file_path.name).write_bytes(file_path.read_bytes())

    return landing_root


def test_delta_write_read_and_history(spark, tmp_path):
    """Test basic Delta Lake write, read, and history inspection."""
    table_path = tmp_path / "delta_basic"
    df = spark.createDataFrame([(1, "Product A", 10.5), (2, "Product B", 20.0)], ["id", "name", "price"])
    df.write.format("delta").mode("overwrite").save(table_path.as_posix())

    # Read verification
    read_df = spark.read.format("delta").load(table_path.as_posix())
    assert read_df.count() == 2

    # History verification
    history_df = get_delta_history(spark, table_path)
    assert history_df.count() >= 1
    assert "version" in history_df.columns
    assert "operation" in history_df.columns

    # Direct transaction log inspection
    commits = inspect_transaction_log(table_path)
    assert len(commits) >= 1
    assert commits[0]["version"] == 0


def test_multi_record_json_lines_bronze_ingestion(spark, tmp_path):
    """
    Regression Test: Verify that newline-delimited JSON (JSON Lines) with multiple
    records is ingested in its entirety into Bronze without multiLine collapsing records.
    """
    landing_dir = tmp_path / "landing_jsonl"
    dest = landing_dir / "retail" / "payments" / "ingestion_date=2026-08-31" / "run_id=test-jsonl"
    dest.mkdir(parents=True, exist_ok=True)
    jsonl_file = dest / "payments.json"

    # Write 5 distinct JSON Lines records
    lines = [
        '{"payment_id": "PAY-001", "order_id": "ORD-001", "payment_timestamp": "2026-08-31 10:00:00", "payment_method": "CREDIT_CARD", "payment_status": "SUCCESS", "payment_amount": 50.00, "transaction_reference": "TXN-001"}',
        '{"payment_id": "PAY-002", "order_id": "ORD-002", "payment_timestamp": "2026-08-31 10:05:00", "payment_method": "DEBIT_CARD", "payment_status": "SUCCESS", "payment_amount": 75.50, "transaction_reference": "TXN-002"}',
        '{"payment_id": "PAY-003", "order_id": "ORD-003", "payment_timestamp": "2026-08-31 10:10:00", "payment_method": "PAYPAL", "payment_status": "SUCCESS", "payment_amount": 120.00, "transaction_reference": "TXN-003"}',
        '{"payment_id": "PAY-004", "order_id": "ORD-004", "payment_timestamp": "2026-08-31 10:15:00", "payment_method": "APPLE_PAY", "payment_status": "SUCCESS", "payment_amount": 35.25, "transaction_reference": "TXN-004"}',
        '{"payment_id": "PAY-005", "order_id": "ORD-005", "payment_timestamp": "2026-08-31 10:20:00", "payment_method": "CREDIT_CARD", "payment_status": "SUCCESS", "payment_amount": 200.00, "transaction_reference": "TXN-005"}',
    ]
    jsonl_file.write_text("\n".join(lines), encoding="utf-8")

    bronze_root = tmp_path / "bronze_jsonl"
    counts = ingest_bronze_layer(spark, landing_dir, bronze_root, datasets=["payments"])

    assert counts["payments"] == 5, f"Expected 5 payments ingested from JSON Lines, got {counts['payments']}"
    bronze_df = load_bronze_table(spark, bronze_root, "payments")
    assert bronze_df.count() == 5


def test_landing_discovery_and_audit_tracking(spark, sample_landing_dir, tmp_path):
    """Test landing file discovery and incremental audit tracking."""
    audit_table_path = tmp_path / "audit_log"
    discovered = discover_landing_files(spark, sample_landing_dir)
    assert len(discovered) == 8

    # Verify discovery metadata extraction
    cust_info = next(f for f in discovered if f.dataset_name == "customers")
    assert cust_info.ingestion_date == "2026-08-31"
    assert cust_info.adf_run_id == "test-run-001"
    assert cust_info.file_name == "customers.csv"
    assert len(cust_info.file_sha256) == 64

    # Filter uningested (all 8 pending)
    pending = filter_uningested_files(spark, discovered, audit_table_path)
    assert len(pending) == 8

    # Record 4 files
    record_ingested_files(spark, pending[:4], audit_table_path)

    # Filter again: remaining should be 4
    pending_after = filter_uningested_files(spark, discovered, audit_table_path)
    assert len(pending_after) == 4


def test_landing_path_parsing_cloud_and_local():
    """Test landing path parser against both local filesystem paths and cloud ABFSS URIs."""
    cloud_uri = "abfss://lakehouse@stlakehousedev.dfs.core.windows.net/landing/retail/customers/ingestion_date=2026-08-31/run_id=adf-run-999/customers.csv"
    ds, date_str, run_id, filename, fmt = parse_landing_path(cloud_uri)
    assert ds == "customers"
    assert date_str == "2026-08-31"
    assert run_id == "adf-run-999"
    assert filename == "customers.csv"
    assert fmt == "csv"

    json_uri = "abfss://lakehouse@stlakehousedev.dfs.core.windows.net/landing/retail/payments/ingestion_date=2026-08-31/run_id=adf-run-888/payments.json"
    ds2, _, _, filename2, fmt2 = parse_landing_path(json_uri)
    assert ds2 == "payments"
    assert filename2 == "payments.json"
    assert fmt2 == "json"


def test_bronze_layer_ingestion(spark, sample_landing_dir, tmp_path):
    """Test Bronze ingestion across all 8 datasets and lineage column presence."""
    bronze_root = tmp_path / "bronze"
    counts = ingest_bronze_layer(spark, sample_landing_dir, bronze_root)

    assert len(counts) == 8
    for ds in ["customers", "products", "stores", "employees", "orders", "order_items", "payments", "returns"]:
        assert counts[ds] > 0
        df = load_bronze_table(spark, bronze_root, ds)
        assert "_source_file" in df.columns
        assert "_source_path" in df.columns
        assert "_ingestion_date" in df.columns
        assert "_adf_run_id" in df.columns
        assert "_ingested_timestamp" in df.columns

    # Rerun should be idempotent and ingest 0 new rows
    rerun_counts = ingest_bronze_layer(spark, sample_landing_dir, bronze_root)
    assert all(c == 0 for c in rerun_counts.values())


def test_silver_order_items_discount_calculation_and_gold_flow(spark):
    """
    Exact Financial Correctness Test:
    quantity = 2, unit_price = 50.00, discount_percent = 0.10 (10%)
    Expected: gross = 100.00, discount_amount = 10.00, net_amount = 90.00.
    Verifies that discount_percent is NOT divided by 100 again and flows accurately into Gold.
    """
    bronze_items_data = [
        ("ITEM-001", "ORD-001", "PROD-001", "2", "50.00", "0.10", "items.csv", "/path", "2026-08-31", "run-1", datetime.now(timezone.utc)),
    ]
    bronze_items_df = spark.createDataFrame(
        bronze_items_data,
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_percent", "_source_file", "_source_path", "_ingestion_date", "_adf_run_id", "_ingested_timestamp"],
    )

    valid_orders = spark.createDataFrame([("ORD-001", date(2026, 8, 31), "COMPLETED")], ["order_id", "order_date", "order_status"])
    valid_products = spark.createDataFrame([("PROD-001", Decimal("25.00"), "Electronics", "Audio")], ["product_id", "cost_price", "category", "subcategory"])
    empty_returns = spark.createDataFrame([], "order_item_id STRING, refund_amount DECIMAL(10,2), return_id STRING")

    valid_items_df, quarantine_items_df = transform_silver_order_items(bronze_items_df, valid_orders, valid_products)

    assert quarantine_items_df.count() == 0
    assert valid_items_df.count() == 1

    row = valid_items_df.collect()[0]
    assert row["quantity"] == 2
    assert row["unit_price"] == Decimal("50.00")
    assert row["discount_percent"] == Decimal("0.10")
    assert row["discount_amount"] == Decimal("10.00")
    assert row["net_amount"] == Decimal("90.00")

    # Verify Gold daily sales aggregation receives exact net sales (90.00) and discounts (10.00)
    gold_daily = build_gold_daily_sales(valid_orders, valid_items_df, valid_products, empty_returns)
    gold_row = gold_daily.collect()[0]
    assert gold_row["gross_revenue"] == Decimal("100.00")
    assert gold_row["total_discounts"] == Decimal("10.00")
    assert gold_row["net_sales"] == Decimal("90.00")
    assert gold_row["total_cogs"] == Decimal("50.00")  # 2 * 25.00
    assert gold_row["gross_profit"] == Decimal("40.00")  # 90.00 - 50.00


def test_deterministic_deduplication(spark):
    """
    Verify that two records with the same primary key and same ingestion timestamp
    use the deterministic content hash tie-breaker to select the exact same winner every run.
    """
    ts = datetime(2026, 8, 31, 12, 0, 0, tzinfo=timezone.utc)
    bronze_data = [
        ("CUST-DUP", "Alice", "Smith", "alice.primary@example.com", "555-0101", "100 Main St", "Dallas", "TX", "75001", "USA", "2026-01-01", "GOLD", "f.csv", "/p", "2026-08-31", "r1", ts),
        ("CUST-DUP", "Alice", "Smith", "alice.secondary@example.com", "555-0102", "100 Main St", "Dallas", "TX", "75001", "USA", "2026-01-01", "GOLD", "f.csv", "/p", "2026-08-31", "r1", ts),
    ]
    cols = ["customer_id", "first_name", "last_name", "email", "phone", "address", "city", "state", "postal_code", "country", "signup_date", "loyalty_tier", "_source_file", "_source_path", "_ingestion_date", "_adf_run_id", "_ingested_timestamp"]

    # Run 1: Order A
    df_run1 = spark.createDataFrame(bronze_data, cols)
    valid1, quar1 = transform_silver_customers(df_run1)
    winner1 = valid1.collect()[0]["email"]

    # Run 2: Order B (reversed insertion)
    df_run2 = spark.createDataFrame(list(reversed(bronze_data)), cols)
    valid2, quar2 = transform_silver_customers(df_run2)
    winner2 = valid2.collect()[0]["email"]

    assert winner1 == winner2, "Deterministic deduplication must select identical winner regardless of row scan order"
    assert quar1.count() == 1
    assert quar2.count() == 1


def test_silver_reconciliation_validator():
    """Test runtime reconciliation validator passing and failing cases."""
    # Passing case: bronze == valid + quarantine
    validate_silver_reconciliation("orders", bronze_count=100, valid_count=92, quarantine_count=8)

    # Intentionally failing case: mismatch raises ReconciliationError with exact counts
    with pytest.raises(ReconciliationError) as exc_info:
        validate_silver_reconciliation("orders", bronze_count=100, valid_count=90, quarantine_count=5)

    msg = str(exc_info.value)
    assert "orders" in msg
    assert "100" in msg
    assert "90" in msg
    assert "5" in msg


def test_silver_layer_transformation_and_reconciliation(spark, sample_landing_dir, tmp_path):
    """Test Silver transformation, typing, quarantine routing, and mathematical reconciliation."""
    bronze_root = tmp_path / "bronze_recon"
    silver_root = tmp_path / "silver_recon"
    quarantine_root = tmp_path / "quarantine_recon"

    ingest_bronze_layer(spark, sample_landing_dir, bronze_root)
    metrics = process_silver_layer(spark, bronze_root, silver_root, quarantine_root)

    assert len(metrics) == 8
    for m in metrics.values():
        assert m["bronze"] > 0
        assert m["silver_valid"] > 0
        assert m["quarantine"] >= 0
        # Invariant: Bronze rows are accounted for (reconciliation check)
        assert m["bronze"] == m["silver_valid"] + m["quarantine"]

    # Verify strong typing on Silver tables
    silver_cust = spark.read.format("delta").load(str(silver_root / "customers"))
    assert isinstance(silver_cust.schema["customer_id"].dataType, StringType)
    assert isinstance(silver_cust.schema["signup_date"].dataType, DateType)

    silver_items = spark.read.format("delta").load(str(silver_root / "order_items"))
    assert isinstance(silver_items.schema["unit_price"].dataType, DecimalType)
    assert isinstance(silver_items.schema["net_amount"].dataType, DecimalType)
    assert isinstance(silver_items.schema["quantity"].dataType, IntegerType)


def test_delta_merge_idempotency_and_updates(spark, tmp_path):
    """Test Delta MERGE operations for idempotency, updating matched records, and inserting new ones."""
    table_path = tmp_path / "merge_test"

    # 1. Initial State (Version 0)
    initial_data = [
        (1, "Alice", "Smith", "alice@example.com", "ACTIVE"),
        (2, "Bob", "Jones", "bob@example.com", "ACTIVE"),
    ]
    df_init = spark.createDataFrame(initial_data, ["customer_id", "first_name", "last_name", "email", "status"])
    df_init.write.format("delta").mode("overwrite").save(table_path.as_posix())

    # 2. Re-run identical batch -> Should result in 0 new rows (idempotency)
    upsert_delta_table(spark, table_path, df_init, primary_key="customer_id")
    df_after_rerun = spark.read.format("delta").load(table_path.as_posix())
    assert df_after_rerun.count() == 2

    # 3. Apply updates + 1 new insert
    incoming_data = [
        (2, "Bob", "Jones", "bob.updated@example.com", "SUSPENDED"),  # Update Bob
        (3, "Charlie", "Brown", "charlie@example.com", "ACTIVE"),      # Insert Charlie
    ]
    df_incoming = spark.createDataFrame(incoming_data, ["customer_id", "first_name", "last_name", "email", "status"])
    upsert_delta_table(spark, table_path, df_incoming, primary_key="customer_id")

    df_result = spark.read.format("delta").load(table_path.as_posix())
    assert df_result.count() == 3

    # Verify Alice (customer 1) is completely untouched
    alice = df_result.filter(df_result.customer_id == 1).collect()[0]
    assert alice["email"] == "alice@example.com"
    assert alice["status"] == "ACTIVE"

    # Verify Bob (customer 2) is updated
    bob = df_result.filter(df_result.customer_id == 2).collect()[0]
    assert bob["email"] == "bob.updated@example.com"
    assert bob["status"] == "SUSPENDED"

    # Verify Charlie (customer 3) is inserted
    charlie = df_result.filter(df_result.customer_id == 3).collect()[0]
    assert charlie["first_name"] == "Charlie"


def test_upsert_customers_actual_schema(spark, tmp_path):
    """Test upsert_customers using the exact conformed Silver customer schema."""
    table_path = tmp_path / "silver_cust_merge"
    cols = ["customer_id", "first_name", "last_name", "email", "phone", "address", "city", "state", "postal_code", "country", "signup_date", "loyalty_tier"]

    init_data = [
        ("C-001", "John", "Doe", "john@example.com", "555-0100", "123 Main St", "Dallas", "TX", "75001", "US", date(2026, 1, 1), "STANDARD"),
        ("C-002", "Jane", "Smith", "jane@example.com", "555-0200", "456 Oak Ave", "Austin", "TX", "78701", "US", date(2026, 2, 1), "STANDARD"),
    ]
    df_init = spark.createDataFrame(init_data, cols)
    df_init.write.format("delta").mode("overwrite").save(str(table_path))

    # Update C-001 to PLATINUM and add C-003
    update_data = [
        ("C-001", "John", "Doe", "john.updated@example.com", "555-0100", "123 Main St", "Dallas", "TX", "75001", "US", date(2026, 1, 1), "PLATINUM"),
        ("C-003", "Bob", "Brown", "bob@example.com", "555-0300", "789 Pine St", "Houston", "TX", "77001", "US", date(2026, 3, 1), "GOLD"),
    ]
    df_update = spark.createDataFrame(update_data, cols)
    upsert_customers(spark, table_path, df_update)

    df_post = spark.read.format("delta").load(str(table_path))
    assert df_post.count() == 3

    c1 = df_post.filter(df_post.customer_id == "C-001").collect()[0]
    assert c1["loyalty_tier"] == "PLATINUM"
    assert c1["email"] == "john.updated@example.com"

    # Re-run same update -> Count must remain 3 (idempotent)
    upsert_customers(spark, table_path, df_update)
    assert spark.read.format("delta").load(str(table_path)).count() == 3


def test_delta_table_existence_check(spark, tmp_path):
    """Test DeltaTable.isDeltaTable identification on real and empty paths."""
    real_delta = tmp_path / "is_delta_true"
    spark.createDataFrame([(1, "A")], ["id", "val"]).write.format("delta").save(str(real_delta))
    assert DeltaTable.isDeltaTable(spark, str(real_delta)) is True

    non_delta = tmp_path / "not_delta"
    non_delta.mkdir()
    assert DeltaTable.isDeltaTable(spark, str(non_delta)) is False


def test_unity_catalog_registration_sql_generation():
    """Test generation of Unity Catalog external table registration SQL."""
    statements = generate_unity_catalog_registration_sql(
        catalog_name="retail_lakehouse",
        delta_root_uri="abfss://lakehouse@stlakehousedev.dfs.core.windows.net/delta",
    )
    assert len(statements) >= 32  # 1 catalog + 3 schemas + 8 bronze + 8 silver + 8 quarantine + 6 gold = 34 statements

    joined = "\n".join(statements)
    assert "CREATE CATALOG IF NOT EXISTS retail_lakehouse;" in joined
    assert "CREATE SCHEMA IF NOT EXISTS retail_lakehouse.bronze;" in joined
    assert "CREATE TABLE IF NOT EXISTS retail_lakehouse.bronze.customers USING DELTA LOCATION 'abfss://lakehouse@stlakehousedev.dfs.core.windows.net/delta/bronze/customers';" in joined
    assert "CREATE TABLE IF NOT EXISTS retail_lakehouse.silver.customers USING DELTA LOCATION 'abfss://lakehouse@stlakehousedev.dfs.core.windows.net/delta/silver/customers';" in joined
    assert "CREATE TABLE IF NOT EXISTS retail_lakehouse.silver.quarantine_customers USING DELTA LOCATION 'abfss://lakehouse@stlakehousedev.dfs.core.windows.net/delta/silver/quarantine/customers';" in joined
    assert "CREATE TABLE IF NOT EXISTS retail_lakehouse.gold.gold_daily_sales_performance USING DELTA LOCATION 'abfss://lakehouse@stlakehousedev.dfs.core.windows.net/delta/gold/gold_daily_sales_performance';" in joined


def test_delta_time_travel(spark, tmp_path):
    """Test Delta Lake Time Travel across multiple table versions."""
    table_path = tmp_path / "time_travel_test"

    # Version 0: 2 rows
    df_v0 = spark.createDataFrame([(1, "Item 1"), (2, "Item 2")], ["id", "name"])
    df_v0.write.format("delta").mode("overwrite").save(table_path.as_posix())

    # Version 1: +1 row
    df_v1 = spark.createDataFrame([(3, "Item 3")], ["id", "name"])
    df_v1.write.format("delta").mode("append").save(table_path.as_posix())

    # Query current state (Version 1)
    df_current = spark.read.format("delta").load(table_path.as_posix())
    assert df_current.count() == 3

    # Time travel back to Version 0
    df_past = query_time_travel_by_version(spark, table_path, version=0)
    assert df_past.count() == 2
    assert {r["id"] for r in df_past.collect()} == {1, 2}


def test_schema_enforcement_and_evolution(spark, tmp_path):
    """Test schema enforcement prevents unauthorized columns and mergeSchema allows evolution."""
    table_path = tmp_path / "schema_test"

    # 1. Verify schema enforcement blocks mismatch
    blocked, msg = demonstrate_schema_enforcement(spark, table_path)
    assert blocked is True
    assert "A schema mismatch detected when writing to the Delta table" in msg or "schema" in msg.lower()

    # 2. Verify controlled schema evolution
    evolved_df = demonstrate_schema_evolution(spark, table_path)
    assert "membership_tier" in evolved_df.columns
    assert evolved_df.count() == 3


def test_gold_layer_aggregations(spark, sample_landing_dir, tmp_path):
    """Test Gold aggregate generation derived from Silver tables."""
    bronze_root = tmp_path / "bronze_gold"
    silver_root = tmp_path / "silver_gold"
    quarantine_root = tmp_path / "quarantine_gold"
    gold_root = tmp_path / "gold_out"

    ingest_bronze_layer(spark, sample_landing_dir, bronze_root)
    process_silver_layer(spark, bronze_root, silver_root, quarantine_root)
    gold_counts = process_gold_layer(spark, silver_root, gold_root)

    assert len(gold_counts) == 6
    assert gold_counts["gold_daily_sales_performance"] > 0
    assert gold_counts["gold_monthly_revenue"] > 0
    assert gold_counts["gold_revenue_by_store_region"] > 0
    assert gold_counts["gold_category_revenue_performance"] > 0
    assert gold_counts["gold_customer_spending_summary"] > 0
    assert gold_counts["gold_return_refund_performance"] > 0

    # Verify metrics correctness on gold_daily_sales_performance
    daily_df = spark.read.format("delta").load(str(gold_root / "gold_daily_sales_performance"))
    assert "net_sales" in daily_df.columns
    assert "gross_profit" in daily_df.columns
    assert "returns_count" in daily_df.columns
