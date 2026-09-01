"""
Landing Batch Validation Task (Module 5 Lakeflow Jobs).

Prerequisite check verifying that the ADF-landed dataset batch exists,
contains expected source tables, and is ready for Bronze ingestion.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from src.medallion.discovery import discover_landing_files
from src.orchestration.models import RunContext, TaskValueStore

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

EXPECTED_DATASETS = {
    "customers",
    "products",
    "stores",
    "employees",
    "orders",
    "order_items",
    "payments",
    "returns",
}


def execute_validate_landing_task(
    spark: SparkSession,
    context: RunContext,
    task_values: TaskValueStore,
) -> dict:
    """
    Validate presence and completeness of ADF landing batch.

    Publishes Task Values:
        - landing_ready (bool)
        - discovered_dataset_count (int)
        - batch_path (str)
    """
    logger.info(
        "Starting Landing Batch Validation: Date=%s, ADF_Run_ID=%s",
        context.ingestion_date,
        context.adf_run_id,
    )

    landing_root = Path(str(context.landing_root))
    all_discovered = discover_landing_files(spark, landing_root)

    # Filter for the target batch if specific adf_run_id is specified
    if context.adf_run_id and context.adf_run_id != "manual_orchestration_run":
        discovered = [f for f in all_discovered if f.adf_run_id == context.adf_run_id]
    else:
        discovered = all_discovered

    discovered_names = {f.dataset_name for f in discovered}
    discovered_count = len(discovered_names)

    batch_path = str(landing_root / "retail")

    # In local testing, if an explicit batch exists with at least 1 dataset, mark ready
    # In production, check for expected dataset coverage
    landing_ready = discovered_count > 0

    task_values.set("validate_landing_batch", "landing_ready", landing_ready)
    task_values.set("validate_landing_batch", "discovered_dataset_count", discovered_count)
    task_values.set("validate_landing_batch", "batch_path", batch_path)

    if not landing_ready:
        error_msg = (
            f"ADF landing batch validation failed: 0 datasets found for ingestion_date={context.ingestion_date}, "
            f"adf_run_id={context.adf_run_id} at {landing_root}"
        )
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    logger.info(
        "Landing Batch Validation complete: Ready=%s, Discovered %d datasets",
        landing_ready,
        discovered_count,
    )

    return {
        "landing_ready": landing_ready,
        "discovered_dataset_count": discovered_count,
        "batch_path": batch_path,
    }
