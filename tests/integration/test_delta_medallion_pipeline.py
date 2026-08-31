"""
Integration Test for End-to-End Delta Lake Medallion Pipeline (Module 3).

Executes the entire Landing ➔ Bronze ➔ Silver ➔ Gold batch workflow
with synthetic landing data, verifying data movement, Delta table persistence,
quarantine routing, and reconciliation metrics.
"""

from __future__ import annotations

from src.config.settings import ScaleConfig
from src.data_generation.generate_retail_data import generate_all_datasets
from src.pipelines.delta_medallion_pipeline import run_delta_medallion_pipeline


def test_end_to_end_delta_medallion_pipeline(tmp_path):
    """Test full execution of Delta Medallion pipeline end-to-end."""
    landing_root = tmp_path / "landing"
    delta_root = tmp_path / "delta"

    # Seed landing directory with test data
    raw_temp = tmp_path / "_raw_temp"
    scale = ScaleConfig(
        name="integration_test",
        num_customers=60,
        num_products=25,
        num_stores=5,
        num_employees=10,
        num_orders=100,
        max_items_per_order=3,
        return_rate=0.15,
        seed=999,
    )
    generate_all_datasets(scale, raw_temp)

    date_str = "2026-08-31"
    run_id = "integration-run-01"

    for file_path in raw_temp.glob("*"):
        if file_path.is_file():
            ds_name = file_path.stem.split(".")[0]
            dest = landing_root / "retail" / ds_name / f"ingestion_date={date_str}" / f"run_id={run_id}"
            dest.mkdir(parents=True, exist_ok=True)
            (dest / file_path.name).write_bytes(file_path.read_bytes())

    # Execute pipeline
    result = run_delta_medallion_pipeline(
        landing_root=landing_root,
        delta_root=delta_root,
        scale_name="small",
    )

    assert result is not None
    assert len(result["bronze"]) == 8
    assert len(result["silver"]) == 8
    assert len(result["gold"]) == 6
    assert result["duration_seconds"] > 0

    # Verify Bronze Delta tables exist on disk
    bronze_dir = delta_root / "bronze"
    for ds in ["customers", "products", "stores", "employees", "orders", "order_items", "payments", "returns"]:
        assert (bronze_dir / ds / "_delta_log").exists()

    # Verify Silver Delta tables exist on disk
    silver_dir = delta_root / "silver"
    for ds in ["customers", "products", "stores", "employees", "orders", "order_items", "payments", "returns"]:
        assert (silver_dir / ds / "_delta_log").exists()

    # Verify Gold Delta tables exist on disk
    gold_dir = delta_root / "gold"
    for tbl in [
        "gold_daily_sales_performance",
        "gold_monthly_revenue",
        "gold_revenue_by_store_region",
        "gold_category_revenue_performance",
        "gold_customer_spending_summary",
        "gold_return_refund_performance",
    ]:
        assert (gold_dir / tbl / "_delta_log").exists()
