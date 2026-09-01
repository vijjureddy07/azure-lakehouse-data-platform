"""
Gold Layer Analytical Aggregations Task (Module 5 Lakeflow Jobs).

Invokes Module 3 Gold layer processing, generating aggregated business KPI
Delta tables from conformed Silver data.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from src.medallion.gold import process_gold_layer
from src.orchestration.models import RunContext, TaskValueStore

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def execute_gold_task(
    spark: SparkSession,
    context: RunContext,
    task_values: TaskValueStore,
) -> dict:
    """
    Execute Gold business analytical aggregations.

    Publishes Task Values:
        - gold_tables_generated (int)
    """
    logger.info("Executing Gold Layer Aggregations for run: %s", context.orchestration_run_id)

    delta_root = str(context.delta_root).rstrip("/")
    silver_root = f"{delta_root}/silver"
    gold_root = f"{delta_root}/gold"

    gold_results = process_gold_layer(
        spark=spark,
        silver_root=silver_root,
        gold_root=gold_root,
    )

    tables_count = len(gold_results)

    task_values.set("gold_analytics", "gold_tables_generated", tables_count)

    logger.info(
        "Gold Layer complete: Generated %d KPI tables",
        tables_count,
    )

    return {
        "gold_tables_generated": tables_count,
        "table_details": gold_results,
    }
