"""
Integration Test for End-to-End Dimensional Warehouse Pipeline (Module 4).

Executes the Silver ➔ Warehouse batch workflow:
1. Generates synthetic landing data.
2. Ingests through Medallion pipeline (Bronze ➔ Silver).
3. Executes Dimensional Warehouse Pipeline (Dimensions, SCD1, SCD2, Facts, Quality Gates, Reconciliation).
4. Verifies Delta persistence, point-in-time surrogate key linkages, and rerun idempotency.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql.functions import col

from src.config.settings import ScaleConfig
from src.data_generation.generate_retail_data import generate_all_datasets
from src.pipelines.delta_medallion_pipeline import run_delta_medallion_pipeline
from src.pipelines.dimensional_warehouse_pipeline import run_dimensional_warehouse_pipeline
from src.utils.spark import get_spark_session


def test_end_to_end_dimensional_warehouse_pipeline(tmp_path):
    """Test full execution of Dimensional Warehouse pipeline end-to-end."""
    landing_root = tmp_path / "landing"
    delta_root = tmp_path / "delta"
    silver_root = delta_root / "silver"
    warehouse_root = delta_root / "warehouse"

    # 1. Seed landing directory with test data
    raw_temp = tmp_path / "_raw_temp"
    scale = ScaleConfig(
        name="dim_integration_test",
        num_customers=50,
        num_products=20,
        num_stores=5,
        num_employees=10,
        num_orders=80,
        max_items_per_order=3,
        return_rate=0.15,
        seed=101,
    )
    generate_all_datasets(scale, raw_temp)

    date_str = "2026-08-31"
    run_id = "dim-integration-run-01"

    for file_path in raw_temp.glob("*"):
        if file_path.is_file():
            ds_name = file_path.stem.split(".")[0]
            dest = landing_root / "retail" / ds_name / f"ingestion_date={date_str}" / f"run_id={run_id}"
            dest.mkdir(parents=True, exist_ok=True)
            (dest / file_path.name).write_bytes(file_path.read_bytes())

    # 2. Run Module 3 Medallion Pipeline (creates Silver tables)
    medallion_result = run_delta_medallion_pipeline(
        landing_root=landing_root,
        delta_root=delta_root,
        scale_name="small",
    )
    assert medallion_result is not None

    # 3. Run Module 4 Dimensional Warehouse Pipeline
    wh_result = run_dimensional_warehouse_pipeline(
        silver_root=silver_root,
        warehouse_root=warehouse_root,
    )

    assert wh_result is not None
    assert wh_result["dimensions"]["dim_date"] > 0
    assert wh_result["dimensions"]["dim_product"] > 0
    assert wh_result["dimensions"]["dim_store"] > 0
    assert wh_result["dimensions"]["dim_employee"] > 0
    assert wh_result["dimensions"]["dim_customer"] > 0
    assert wh_result["facts"]["fact_sales"] > 0
    assert wh_result["facts"]["fact_returns"] >= 0
    assert wh_result["quality"]["passed_checks"] == wh_result["quality"]["total_checks"]
    assert wh_result["reconciliation"]["passed"] is True

    spark = get_spark_session()

    # 4. Verify Delta tables on disk via DeltaTable.isDeltaTable
    for dim in ["dim_date", "dim_product", "dim_store", "dim_employee", "dim_customer"]:
        assert DeltaTable.isDeltaTable(spark, str(warehouse_root / dim))

    for fact in ["fact_sales", "fact_returns", "quality_audit"]:
        assert DeltaTable.isDeltaTable(spark, str(warehouse_root / fact))

    # 5. Assert ZERO unexpected unknown keys (key 0) in fact_sales
    fact_sales_df = spark.read.format("delta").load(str(warehouse_root / "fact_sales"))
    assert fact_sales_df.filter(col("customer_key") == 0).count() == 0, "fact_sales must have 0 customer_key == 0"
    assert fact_sales_df.filter(col("product_key") == 0).count() == 0, "fact_sales must have 0 product_key == 0"
    assert fact_sales_df.filter(col("store_key") == 0).count() == 0, "fact_sales must have 0 store_key == 0"
    assert fact_sales_df.filter(col("order_date_key") == 0).count() == 0, "fact_sales must have 0 order_date_key == 0"

    # 6. Rerun pipeline with unchanged Silver data -> Must maintain identical row counts (idempotent MERGE)
    rerun_result = run_dimensional_warehouse_pipeline(
        silver_root=silver_root,
        warehouse_root=warehouse_root,
    )
    assert rerun_result["facts"]["fact_sales"] == wh_result["facts"]["fact_sales"]
    assert rerun_result["facts"]["fact_returns"] == wh_result["facts"]["fact_returns"]
    assert rerun_result["dimensions"]["dim_customer"] == wh_result["dimensions"]["dim_customer"]
    assert rerun_result["reconciliation"]["passed"] is True
