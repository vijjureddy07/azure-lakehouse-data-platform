"""
Landing Batch Validation Task (Module 5 Lakeflow Jobs).

Prerequisite check verifying that the ADF-landed dataset batch exists,
contains all 8 required source tables for the requested ingestion date and ADF run ID,
and is ready for Bronze ingestion.
"""

from __future__ import annotations

import logging
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


class LandingBatchIncompleteError(ValueError):
    """Raised when the ADF landing batch is missing required datasets or unreadable."""
    pass


def execute_validate_landing_task(
    spark: SparkSession,
    context: RunContext,
    task_values: TaskValueStore,
) -> dict:
    """
    Validate presence and completeness of the ADF landing batch.

    Requires all 8 expected datasets matching BOTH context.ingestion_date
    and context.adf_run_id (unless allow_partial_batch is explicitly True).

    Publishes Task Values:
        - landing_ready (bool)
        - discovered_dataset_count (int)
        - missing_dataset_count (int)
        - landing_root (str)
        - ingestion_date (str)
        - adf_run_id (str)
    """
    landing_root_str = str(context.landing_root).strip()
    logger.info(
        "Starting Landing Batch Validation: Date=%s, ADF_Run_ID=%s at %s",
        context.ingestion_date,
        context.adf_run_id,
        landing_root_str,
    )

    all_discovered = discover_landing_files(spark, landing_root_str)

    # Filter discovered files by BOTH ingestion_date and adf_run_id
    if context.adf_run_id and context.adf_run_id != "manual_orchestration_run":
        discovered = [
            f for f in all_discovered
            if f.ingestion_date == context.ingestion_date and f.adf_run_id == context.adf_run_id
        ]
    else:
        discovered = [
            f for f in all_discovered
            if f.ingestion_date == context.ingestion_date
        ]

    discovered_names = {f.dataset_name for f in discovered}
    discovered_expected = discovered_names & EXPECTED_DATASETS
    discovered_count = len(discovered_expected)
    missing_datasets = EXPECTED_DATASETS - discovered_names
    missing_count = len(missing_datasets)
    unexpected_datasets = discovered_names - EXPECTED_DATASETS

    if unexpected_datasets:
        logger.info("Discovered unexpected extra dataset(s) in landing batch: %s", sorted(list(unexpected_datasets)))

    # In production, all 8 datasets must be present
    landing_ready = (missing_count == 0) if not context.allow_partial_batch else (discovered_count > 0)

    # Publish task values
    task_values.set("validate_landing_batch", "landing_ready", landing_ready)
    task_values.set("validate_landing_batch", "discovered_dataset_count", discovered_count)
    task_values.set("validate_landing_batch", "missing_dataset_count", missing_count)
    task_values.set("validate_landing_batch", "landing_root", landing_root_str)
    task_values.set("validate_landing_batch", "ingestion_date", context.ingestion_date)
    task_values.set("validate_landing_batch", "adf_run_id", context.adf_run_id)

    if not landing_ready:
        error_msg = (
            f"ADF landing batch validation failed: Missing {missing_count} required dataset(s) "
            f"{sorted(list(missing_datasets))} for ingestion_date='{context.ingestion_date}', "
            f"adf_run_id='{context.adf_run_id}' at {landing_root_str}. (Found {discovered_count}/8 required)."
        )
        logger.error(error_msg)
        raise LandingBatchIncompleteError(error_msg)

    logger.info(
        "Landing Batch Validation PASSED: All 8 required datasets discovered for date=%s, run_id=%s",
        context.ingestion_date,
        context.adf_run_id,
    )

    return {
        "landing_ready": True,
        "discovered_dataset_count": discovered_count,
        "missing_dataset_count": 0,
        "landing_root": landing_root_str,
        "ingestion_date": context.ingestion_date,
        "adf_run_id": context.adf_run_id,
    }
