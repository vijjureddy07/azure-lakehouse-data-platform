"""
Integration test for complete end-to-end LocalBatchPipeline.
"""

import tempfile
from pathlib import Path

from src.pipelines.local_batch_pipeline import LocalBatchPipeline


def test_end_to_end_local_batch_pipeline():
    """Run full pipeline on small dataset and verify Parquet outputs and metric reconciliations."""
    with tempfile.TemporaryDirectory() as tmp_data_dir, tempfile.TemporaryDirectory() as tmp_output_dir:
        pipeline = LocalBatchPipeline(
            scale="small",
            data_dir=Path(tmp_data_dir),
            output_dir=Path(tmp_output_dir),
            skip_data_gen=False,
        )
        result = pipeline.run()

        assert result["status"] == "SUCCESS"
        assert len(result["metrics"]) == 8

        # Verify Parquet outputs exist
        out_path = Path(tmp_output_dir)
        assert (out_path / "cleaned" / "customers").exists()
        assert (out_path / "cleaned" / "orders").exists()
        assert (out_path / "quarantine" / "customers").exists()
        assert (out_path / "curated" / "curated_sales").exists()
        assert (out_path / "metrics" / "quality_summary").exists()

        # Idempotency test: rerun pipeline in same output directory
        rerun_pipeline = LocalBatchPipeline(
            scale="small",
            data_dir=Path(tmp_data_dir),
            output_dir=Path(tmp_output_dir),
            skip_data_gen=True,  # Test using existing raw data
        )
        rerun_result = rerun_pipeline.run()
        assert rerun_result["status"] == "SUCCESS"
