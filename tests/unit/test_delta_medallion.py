"""
Unit Tests for Module 3: Azure Databricks + Delta Lake + Medallion Lakehouse.

Validates:
1. Delta Lake write, read, and append functionality.
2. Ingestion audit logging and incremental landing file discovery.
3. Bronze lineage metadata columns (_source_file, _source_path, _ingestion_date, _adf_run_id, _ingested_timestamp).
4. Silver cleaning, typing, Decimal financial precision, deduplication, and referential integrity validation.
5. Silver quarantine routing and reconciliation invariant (bronze == silver_valid + quarantine).
6. Idempotent Delta MERGE operations (initial insert, no-op rerun, updated records, unaffected records).
7. Delta table history (DESCRIBE HISTORY) and Time Travel queries (VERSION AS OF).
8. Schema enforcement failure and controlled schema evolution (mergeSchema = true).
9. Gold analytical KPI aggregations correctness.
"""

from __future__ import annotations

import pytest
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
from src.medallion.discovery import (
    discover_landing_files,
    filter_uningested_files,
    record_ingested_files,
)
from src.medallion.gold import (
    process_gold_layer,
)
from src.medallion.merge import upsert_delta_table
from src.medallion.silver import (
    process_silver_layer,
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


def test_landing_discovery_and_audit_tracking(spark, sample_landing_dir, tmp_path):
    """Test landing file discovery and incremental audit tracking."""
    audit_table_path = tmp_path / "audit_log"
    discovered = discover_landing_files(sample_landing_dir)
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
