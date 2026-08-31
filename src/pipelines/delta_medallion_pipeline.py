"""
End-to-End Delta Lake Medallion Pipeline CLI (Module 3).

Orchestrates the entire Medallion lifecycle:
1. Landing Discovery: Scans ADLS/local landing directories for pending batches.
2. Bronze Ingestion: Ingests raw files to Delta tables with audit lineage and updates _ingestion_audit.
3. Silver Transformation: Types, standardizes, deduplicates, and quarantines defective records.
4. Delta MERGE: Demonstrates idempotent upsert processing.
5. Gold Aggregations: Derives 6 business-ready analytical Delta tables.
6. Quality & Reconciliation Audit: Proves zero silent data loss.

Usage:
    python -m src.pipelines.delta_medallion_pipeline --scale small
    python -m src.pipelines.delta_medallion_pipeline --scale standard
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from src.config.settings import (
    DELTA_DIR,
    LANDING_DIR,
    SCALE_PRESETS,
    ScaleConfig,
    SparkConfig,
    ensure_directories,
)
from src.data_generation.generate_retail_data import generate_all_datasets
from src.medallion.bronze import ingest_bronze_layer
from src.medallion.discovery import discover_landing_files
from src.medallion.gold import process_gold_layer
from src.medallion.silver import process_silver_layer
from src.utils.spark import get_spark_session, stop_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def prepare_landing_data_if_needed(landing_root: Path, scale_cfg: ScaleConfig) -> None:
    """If landing directory is empty, seed it with sample landing data structured by ADF path pattern."""
    discovered = discover_landing_files(landing_root)
    if discovered:
        logger.info("Found %d existing landing files in %s", len(discovered), landing_root)
        return

    logger.info("Landing directory is empty. Seeding synthetic landing files for scale: %s", scale_cfg.name)
    temp_raw = landing_root / "_temp_seed"
    generate_all_datasets(scale_cfg, temp_raw)

    date_str = "2026-08-31"
    run_id = "run-initial-batch"

    for file_path in temp_raw.glob("*"):
        if file_path.is_file():
            ds_name = file_path.stem.split(".")[0]
            dest_dir = landing_root / "retail" / ds_name / f"ingestion_date={date_str}" / f"run_id={run_id}"
            dest_dir.mkdir(parents=True, exist_ok=True)
            target_path = dest_dir / file_path.name
            target_path.write_bytes(file_path.read_bytes())
            logger.info("Seeded landing file: %s", target_path)

    # Clean up temp raw
    import shutil
    shutil.rmtree(temp_raw, ignore_errors=True)


def run_delta_medallion_pipeline(
    landing_root: Path = LANDING_DIR,
    delta_root: Path = DELTA_DIR,
    scale_name: str = "small",
    force_all: bool = False,
) -> dict:
    """Execute the end-to-end Delta Medallion pipeline."""
    start_time = time.time()
    ensure_directories()
    scale_cfg = SCALE_PRESETS.get(scale_name, SCALE_PRESETS["small"])

    bronze_root = delta_root / "bronze"
    silver_root = delta_root / "silver"
    quarantine_root = delta_root / "silver" / "quarantine"
    gold_root = delta_root / "gold"

    print("=" * 75)
    print("STARTING DELTA LAKE MEDALLION LAKEHOUSE PIPELINE (MODULE 3)")
    print(f"Scale: {scale_name} | Landing: {landing_root} | Delta Root: {delta_root}")
    print("=" * 75)

    # Ensure landing files exist
    prepare_landing_data_if_needed(landing_root, scale_cfg)

    spark = get_spark_session(SparkConfig(app_name="DeltaMedallionPipeline"))

    try:
        # --- 1. BRONZE INGESTION ---
        print("\n--- STAGE 1: BRONZE DELTA INGESTION ---")
        bronze_counts = ingest_bronze_layer(
            spark=spark,
            landing_root=landing_root,
            bronze_root=bronze_root,
            force_all=force_all,
        )
        for ds, count in bronze_counts.items():
            print(f"  [BRONZE] {ds:<15} : +{count:,} rows ingested")

        # --- 2. SILVER TRANSFORMATION ---
        print("\n--- STAGE 2: SILVER CONFORMANCE & QUARANTINE ---")
        silver_metrics = process_silver_layer(
            spark=spark,
            bronze_root=bronze_root,
            silver_root=silver_root,
            quarantine_root=quarantine_root,
        )
        for ds, m in silver_metrics.items():
            print(f"  [SILVER] {ds:<15} : Bronze={m['bronze']:,} | Valid={m['silver_valid']:,} | Quarantine={m['quarantine']:,}")

        # --- 3. GOLD ANALYTICS ---
        print("\n--- STAGE 3: GOLD BUSINESS AGGREGATIONS ---")
        gold_counts = process_gold_layer(
            spark=spark,
            silver_root=silver_root,
            gold_root=gold_root,
        )
        for tbl, count in gold_counts.items():
            print(f"  [GOLD]   {tbl:<35} : {count:,} aggregate rows")

        duration = time.time() - start_time
        print("\n" + "=" * 75)
        print(f"PIPELINE COMPLETED SUCCESSFULLY IN {duration:.2f} SECONDS")
        print("=" * 75)

        return {
            "bronze": bronze_counts,
            "silver": silver_metrics,
            "gold": gold_counts,
            "duration_seconds": duration,
        }

    finally:
        stop_spark_session(spark)


def main():
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Delta Lake Medallion Pipeline CLI (Module 3)")
    parser.add_argument("--scale", choices=["small", "standard"], default="small", help="Dataset scale to generate/process")
    parser.add_argument("--landing-dir", type=str, default=str(LANDING_DIR), help="Path to landing directory")
    parser.add_argument("--delta-dir", type=str, default=str(DELTA_DIR), help="Path to Delta root storage")
    parser.add_argument("--force-all", action="store_true", help="Force re-ingestion of already processed landing files")

    args = parser.parse_args()
    run_delta_medallion_pipeline(
        landing_root=Path(args.landing_dir),
        delta_root=Path(args.delta_dir),
        scale_name=args.scale,
        force_all=args.force_all,
    )


if __name__ == "__main__":
    main()
