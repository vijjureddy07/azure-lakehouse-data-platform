"""
Silver Layer Transformation & Conformance Task (Module 5 Lakeflow Jobs).

Invokes Module 3 Silver transformation pipeline, enforcing typing, cleaning,
window deduplication, foreign-key anti-join quarantine, and mathematical reconciliation.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from src.medallion.silver import process_silver_layer
from src.orchestration.models import RunContext, TaskValueStore
from src.orchestration.reliability import QuarantineThresholdExceededError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def execute_silver_task(
    spark: SparkSession,
    context: RunContext,
    task_values: TaskValueStore,
) -> dict:
    """
    Execute Silver conformance, deduplication, quarantine, and reconciliation.

    Publishes Task Values:
        - silver_valid_rows (int)
        - silver_quarantine_rows (int)
        - reconciliation_passed (bool)
        - quarantine_rate (float)
        - quarantine_alert_triggered (bool)

    Raises:
        QuarantineThresholdExceededError: If quarantine rate exceeds configured threshold.
    """
    logger.info("Executing Silver Layer Conformance for run: %s", context.orchestration_run_id)

    delta_root = str(context.delta_root).rstrip("/")
    bronze_root = f"{delta_root}/bronze"
    silver_root = f"{delta_root}/silver"
    quarantine_root = f"{delta_root}/silver/quarantine"

    silver_result = process_silver_layer(
        spark=spark,
        bronze_root=bronze_root,
        silver_root=silver_root,
        quarantine_root=quarantine_root,
    )

    total_valid = sum(s["silver_valid"] for s in silver_result.values())
    total_quarantine = sum(s["quarantine"] for s in silver_result.values())
    total_processed = total_valid + total_quarantine

    quarantine_rate = (total_quarantine / total_processed) if total_processed > 0 else 0.0
    alert_triggered = quarantine_rate > context.quarantine_threshold_rate

    # Publish task values prior to evaluating gate
    task_values.set("silver_transformation", "silver_valid_rows", total_valid)
    task_values.set("silver_transformation", "silver_quarantine_rows", total_quarantine)
    task_values.set("silver_transformation", "reconciliation_passed", True)
    task_values.set("silver_transformation", "quarantine_rate", quarantine_rate)
    task_values.set("silver_transformation", "quarantine_alert_triggered", alert_triggered)

    if alert_triggered:
        error_msg = (
            f"Silver Quarantine Gate FAILED: Quarantine rate {quarantine_rate:.4f} ({quarantine_rate*100:.2f}%) "
            f"exceeded configured threshold {context.quarantine_threshold_rate:.4f} "
            f"({total_quarantine} quarantined rows / {total_processed} total processed). "
            f"Halting pipeline before Gold/Warehouse publication."
        )
        logger.error(error_msg)
        raise QuarantineThresholdExceededError(error_msg)

    logger.info(
        "Silver Layer complete: %d valid rows, %d quarantined rows (Reconciliation PASSED)",
        total_valid,
        total_quarantine,
    )

    return {
        "silver_valid_rows": total_valid,
        "silver_quarantine_rows": total_quarantine,
        "reconciliation_passed": True,
        "quarantine_rate": quarantine_rate,
        "quarantine_alert_triggered": False,
    }
