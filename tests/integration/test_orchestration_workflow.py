"""
Integration Test for Lakeflow Jobs Orchestration Workflow (Module 5).

Executes the complete local orchestration DAG:
ADF Landing Validation ➔ Bronze Ingestion ➔ Silver Transformation ➔ Gold Analytics & Dimensional Warehouse ➔ Quality Gate ➔ Operational Audit.
"""

from __future__ import annotations

from delta.tables import DeltaTable

from src.config.settings import ScaleConfig
from src.data_generation.generate_retail_data import generate_all_datasets
from src.orchestration.models import RunContext
from src.orchestration.orchestrator import LakeflowLocalOrchestrator
from src.utils.spark import get_spark_session


def test_end_to_end_lakeflow_orchestration_success(tmp_path):
    """
    Test successful end-to-end execution of the Lakeflow Jobs orchestration DAG.
    """
    landing_root = tmp_path / "landing"
    delta_root = tmp_path / "delta"

    # 1. Seed landing directory with realistic batch
    raw_temp = tmp_path / "_raw_temp"
    scale = ScaleConfig(
        name="orch_integration_test",
        num_customers=40,
        num_products=15,
        num_stores=4,
        num_employees=8,
        num_orders=60,
        max_items_per_order=3,
        return_rate=0.10,
        seed=202,
    )
    generate_all_datasets(scale, raw_temp)

    date_str = "2026-08-31"
    run_id = "orch-integration-batch-01"

    for file_path in raw_temp.glob("*"):
        if file_path.is_file():
            ds_name = file_path.stem.split(".")[0]
            dest = landing_root / "retail" / ds_name / f"ingestion_date={date_str}" / f"run_id={run_id}"
            dest.mkdir(parents=True, exist_ok=True)
            (dest / file_path.name).write_bytes(file_path.read_bytes())

    # 2. Configure RunContext
    context = RunContext(
        environment="test",
        ingestion_date=date_str,
        adf_run_id=run_id,
        landing_root=landing_root,
        delta_root=delta_root,
    )

    # 3. Execute Orchestrator
    orchestrator = LakeflowLocalOrchestrator(context=context)
    audit = orchestrator.run()

    # 4. Verify Outcomes
    assert audit is not None
    assert audit.final_status == "SUCCESS"
    assert audit.landing_ready is True
    assert audit.discovered_dataset_count == 8
    assert audit.bronze_rows_ingested > 0
    assert audit.silver_valid_rows > 0
    assert audit.gold_tables_generated == 6
    assert audit.fact_sales_rows > 0
    assert audit.quality_status == "PASSED"

    # 5. Verify Operational Audit Table on disk
    spark = get_spark_session()
    audit_table_path = str(delta_root / "operations" / "job_run_audit")
    assert DeltaTable.isDeltaTable(spark, audit_table_path)

    audit_df = spark.read.format("delta").load(audit_table_path)
    assert audit_df.count() == 1
    row = audit_df.collect()[0]
    assert row["final_status"] == "SUCCESS"
    assert row["adf_run_id"] == run_id


def test_lakeflow_orchestration_failure_path_records_audit(tmp_path):
    """
    Test that an early failure (e.g. empty/missing landing batch) records a FAILED operational
    audit record with failure classification without crashing unhandled.
    """
    landing_root = tmp_path / "empty_landing"
    delta_root = tmp_path / "delta"

    landing_root.mkdir(parents=True, exist_ok=True)

    context = RunContext(
        environment="test",
        ingestion_date="2026-09-01",
        adf_run_id="non_existent_run",
        landing_root=landing_root,
        delta_root=delta_root,
    )

    orchestrator = LakeflowLocalOrchestrator(context=context)
    audit = orchestrator.run()

    assert audit is not None
    assert audit.final_status == "FAILED"
    assert audit.failure_task == "validate_landing_batch"
    assert audit.failure_classification is not None

    # Verify audit record is still persisted (run_if: ALL_DONE semantics)
    spark = get_spark_session()
    audit_table_path = str(delta_root / "operations" / "job_run_audit")
    assert DeltaTable.isDeltaTable(spark, audit_table_path)
    audit_df = spark.read.format("delta").load(audit_table_path)
    assert audit_df.count() == 1
    assert audit_df.collect()[0]["final_status"] == "FAILED"
