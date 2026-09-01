"""
Final Operational Quality Gate Task (Module 5 Lakeflow Jobs).

Verifies operational expectations across all upstream stages:
- Bronze ingestion succeeded
- Silver reconciliation passed
- Gold analytics completed
- Warehouse quality gates and reconciliation passed
- Zero critical failures
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from src.orchestration.models import RunContext, TaskValueStore

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class OperationalQualityGateError(RuntimeError):
    """Raised when the final operational quality gate detects upstream verification failure."""
    pass


def execute_final_quality_gate_task(
    spark: SparkSession,
    context: RunContext,
    task_values: TaskValueStore,
) -> dict:
    """
    Verify high-level operational quality criteria from upstream task values.

    Publishes Task Values:
        - final_quality_gate_passed (bool)
        - overall_quality_status (str)
    """
    logger.info("Executing Final Operational Quality Gate for run: %s", context.orchestration_run_id)

    bronze_rows = task_values.get("bronze_ingestion", "bronze_rows_ingested")
    silver_recon = task_values.get("silver_transformation", "reconciliation_passed")
    gold_tables = task_values.get("gold_analytics", "gold_tables_generated")
    warehouse_quality = task_values.get("dimensional_warehouse", "warehouse_quality_passed")
    fact_sales = task_values.get("dimensional_warehouse", "fact_sales_rows")

    violations = []

    if bronze_rows is None or bronze_rows < 0:
        violations.append("Bronze ingestion did not record valid processed row counts.")

    if silver_recon is not True:
        violations.append("Silver mathematical reconciliation was not verified.")

    if gold_tables is None or gold_tables <= 0:
        violations.append("Gold layer did not produce expected analytical tables.")

    if warehouse_quality is not True:
        violations.append("Dimensional warehouse quality gates failed or were not executed.")

    if fact_sales is None or fact_sales <= 0:
        violations.append("Fact sales table has 0 rows or was not generated.")

    passed = len(violations) == 0
    status_str = "PASSED" if passed else "FAILED"

    task_values.set("final_quality_gate", "final_quality_gate_passed", passed)
    task_values.set("final_quality_gate", "overall_quality_status", status_str)

    if not passed:
        error_msg = f"Final Operational Quality Gate FAILED with {len(violations)} violation(s): " + " | ".join(violations)
        logger.error(error_msg)
        raise OperationalQualityGateError(error_msg)

    logger.info("Final Operational Quality Gate PASSED successfully.")
    return {
        "final_quality_gate_passed": True,
        "overall_quality_status": "PASSED",
        "evaluated_criteria": [
            "Bronze Ingestion Succeeded",
            "Silver Reconciliation Verified",
            "Gold Aggregations Built",
            "Warehouse Star Schema Quality Verified",
        ],
    }
