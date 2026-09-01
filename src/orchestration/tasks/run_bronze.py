"""
Bronze Layer Ingestion Task (Module 5 Lakeflow Jobs).

Invokes Module 3 Bronze ingestion to ingest immutable raw landing files
into Bronze Delta Lake tables with metadata enrichment and audit tracking.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from src.medallion.bronze import ingest_bronze_layer
from src.orchestration.models import RunContext, TaskValueStore

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def execute_bronze_task(
    spark: SparkSession,
    context: RunContext,
    task_values: TaskValueStore,
) -> dict:
    """
    Execute Bronze layer ingestion.

    Publishes Task Values:
        - bronze_rows_ingested (int)
        - datasets_processed (int)
    """
    logger.info("Executing Bronze Layer Ingestion for ADF Run: %s", context.adf_run_id)

    delta_root = str(context.delta_root).rstrip("/")
    landing_root = str(context.landing_root).rstrip("/")
    bronze_root = f"{delta_root}/bronze"

    bronze_results = ingest_bronze_layer(
        spark=spark,
        landing_root=landing_root,
        bronze_root=bronze_root,
        ingestion_date=context.ingestion_date,
        adf_run_id=context.adf_run_id,
    )

    total_rows = sum(bronze_results.values())
    datasets_count = len(bronze_results)

    task_values.set("bronze_ingestion", "bronze_rows_ingested", total_rows)
    task_values.set("bronze_ingestion", "datasets_processed", datasets_count)

    logger.info(
        "Bronze Layer Ingestion complete: Ingested %d rows across %d datasets",
        total_rows,
        datasets_count,
    )

    return {
        "bronze_rows_ingested": total_rows,
        "datasets_processed": datasets_count,
        "dataset_details": bronze_results,
    }
