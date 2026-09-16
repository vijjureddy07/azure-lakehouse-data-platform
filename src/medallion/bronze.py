"""
Bronze Medallion Ingestion Layer.

Ingests raw landing files (CSV / JSON) into Bronze Delta tables with minimal transformation,
attaching critical audit and lineage metadata columns:
- _source_file: Base filename
- _source_path: Full landing path
- _ingestion_date: ADF partition date
- _adf_run_id: Pipeline execution RunId
- _ingested_timestamp: Databricks processing timestamp (UTC)

Preserves raw source fidelity (zero aggressive type casting or row filtering) while
ensuring repeatable, incremental-safe execution.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

from src.medallion.discovery import (
    LandingFileInfo,
    discover_landing_files,
    filter_uningested_files,
    record_ingested_files,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

DATASETS = [
    "customers",
    "products",
    "stores",
    "employees",
    "orders",
    "order_items",
    "payments",
    "returns",
]


def ingest_file_to_bronze(
    spark: SparkSession,
    file_info: LandingFileInfo,
) -> DataFrame:
    """
    Read a single landing file as raw strings and append lineage metadata columns.

    Lineage columns appended:
    - _ingestion_id: Deterministic immutable source file identifier
    - _source_file: Base filename
    - _source_path: Full landing path
    - _ingestion_date: ADF partition date
    - _adf_run_id: Pipeline execution RunId
    - _ingested_timestamp: Databricks processing timestamp (UTC)
    """
    path_str = file_info.source_path

    if file_info.format == "json":
        # Standard JSON lines reading without multiLine option
        df = spark.read.json(path_str)
    else:
        # Raw CSV: preserve everything as string to prevent silent type truncation
        df = spark.read.option("header", "true").option("inferSchema", "false").csv(path_str)

    # Attach lineage metadata columns
    now_ts = datetime.now(timezone.utc)
    bronze_df = (
        df.withColumn("_ingestion_id", lit(file_info.ingestion_id))
        .withColumn("_source_file", lit(file_info.file_name))
        .withColumn("_source_path", lit(path_str))
        .withColumn("_ingestion_date", lit(file_info.ingestion_date))
        .withColumn("_adf_run_id", lit(file_info.adf_run_id))
        .withColumn("_ingested_timestamp", lit(now_ts))
    )

    return bronze_df


def ingest_bronze_layer(
    spark: SparkSession,
    landing_root: Path | str,
    bronze_root: Path | str,
    datasets: list[str] | None = None,
    force_all: bool = False,
    ingestion_date: str | None = None,
    adf_run_id: str | None = None,
) -> dict[str, int]:
    """
    Discover all pending landing files and ingest them into Bronze Delta tables.

    Achieves crash-safe idempotency via deterministic _ingestion_id. If a crash
    occurs after writing Bronze rows but before recording the audit log, a subsequent
    retry inspects the target Bronze Delta table, suppresses duplicate row appends,
    and idempotently repairs the missing audit log entry.
    """
    target_datasets = datasets or DATASETS
    bronze_root_str = str(bronze_root).rstrip("/")
    audit_table_path = f"{bronze_root_str}/_ingestion_audit"

    discovered = discover_landing_files(spark, landing_root, datasets=target_datasets)

    # Optional batch-level isolation filters
    if ingestion_date:
        discovered = [f for f in discovered if f.ingestion_date == ingestion_date]
    if adf_run_id:
        discovered = [f for f in discovered if f.adf_run_id == adf_run_id]

    if force_all:
        pending_files = discovered
    else:
        pending_files = filter_uningested_files(spark, discovered, audit_table_path)

    if not pending_files:
        logger.info("No new landing files to ingest into Bronze.")
        return {ds: 0 for ds in target_datasets}

    # Group pending files by dataset
    files_by_dataset: dict[str, list[LandingFileInfo]] = {}
    for f in pending_files:
        files_by_dataset.setdefault(f.dataset_name, []).append(f)

    ingested_counts: dict[str, int] = {}
    successfully_ingested_files: list[LandingFileInfo] = []

    for ds, file_list in files_by_dataset.items():
        ds_bronze_path = f"{bronze_root_str}/{ds}"
        total_rows = 0

        # Inspect target Bronze Delta table for existing ingestion IDs (crash-recovery check)
        existing_ingestion_ids: set[str] = set()
        if DeltaTable.isDeltaTable(spark, ds_bronze_path):
            existing_df = spark.read.format("delta").load(ds_bronze_path)
            if "_ingestion_id" in existing_df.columns:
                target_ids = [f.ingestion_id for f in file_list if f.ingestion_id]
                if target_ids:
                    matches = (
                        existing_df.filter(col("_ingestion_id").isin(target_ids))
                        .select("_ingestion_id")
                        .distinct()
                        .collect()
                    )
                    existing_ingestion_ids = {r["_ingestion_id"] for r in matches}

        for file_info in file_list:
            if file_info.ingestion_id in existing_ingestion_ids:
                logger.info(
                    "Bronze crash-recovery: rows for _ingestion_id=%s already exist in %s. "
                    "Skipping duplicate row append and queuing for audit repair.",
                    file_info.ingestion_id,
                    ds,
                )
                successfully_ingested_files.append(file_info)
                continue

            logger.info("Ingesting %s from %s into Bronze", ds, file_info.source_path)
            raw_bronze_df = ingest_file_to_bronze(spark, file_info)
            count = raw_bronze_df.count()

            # Append to Delta table
            raw_bronze_df.write.format("delta").mode("append").save(ds_bronze_path)
            total_rows += count
            successfully_ingested_files.append(file_info)

        ingested_counts[ds] = total_rows
        logger.info("Bronze table %s updated (+%d rows)", ds, total_rows)

    # Record in audit log via Delta MERGE
    if successfully_ingested_files:
        record_ingested_files(spark, successfully_ingested_files, audit_table_path, status="SUCCESS")

    return ingested_counts


def load_bronze_table(
    spark: SparkSession,
    bronze_root: Path | str,
    dataset_name: str,
) -> DataFrame:
    """Load a Bronze Delta table into a Spark DataFrame."""
    table_path = f"{str(bronze_root).rstrip('/')}/{dataset_name}"
    if not DeltaTable.isDeltaTable(spark, table_path):
        raise FileNotFoundError(f"Bronze Delta table not found at: {table_path}")
    return spark.read.format("delta").load(table_path)
