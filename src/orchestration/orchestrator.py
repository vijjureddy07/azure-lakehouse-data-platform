"""
Lakeflow Jobs Local Orchestrator Engine (Module 5).

Executes the Lakeflow multi-task DAG locally, managing task state transitions,
dependency resolution, parameter injection, task value passing, intelligent retries,
failure classification, and operational audit persistence.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from src.config.settings import (
    DELTA_DIR,
    LANDING_DIR,
    SparkConfig,
    ensure_directories,
)
from src.orchestration.models import (
    FailureClassification,
    JobRunAudit,
    RunContext,
    TaskResult,
    TaskState,
    TaskValueStore,
)
from src.orchestration.reliability import RetryPolicy, classify_failure, execute_with_retry
from src.orchestration.tasks.final_quality_gate import execute_final_quality_gate_task
from src.orchestration.tasks.publish_run_summary import execute_publish_run_summary_task
from src.orchestration.tasks.run_bronze import execute_bronze_task
from src.orchestration.tasks.run_gold import execute_gold_task
from src.orchestration.tasks.run_silver import execute_silver_task
from src.orchestration.tasks.run_warehouse import execute_warehouse_task
from src.orchestration.tasks.validate_landing import execute_validate_landing_task
from src.utils.spark import get_spark_session, stop_spark_session

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class LakeflowLocalOrchestrator:
    """
    Local testable execution engine for Lakeflow Jobs multi-task DAG.
    """

    def __init__(self, context: RunContext | None = None) -> None:
        self.context = context or RunContext()
        if not self.context.landing_root:
            self.context.landing_root = LANDING_DIR
        if not self.context.delta_root:
            self.context.delta_root = DELTA_DIR

        self.task_values = TaskValueStore()
        self.task_results: dict[str, TaskResult] = {}

    def _execute_task_step(
        self,
        task_name: str,
        task_func,
        retry_policy: RetryPolicy,
    ) -> bool:
        """
        Execute a single task step with timing, retries, and state tracking.

        Returns:
            bool: True if task succeeded, False if task failed.
        """
        result_tracker = TaskResult(task_name=task_name, state=TaskState.RUNNING)
        result_tracker.started_at = datetime.now(timezone.utc)
        self.task_results[task_name] = result_tracker

        logger.info(">>> STARTING TASK: %s", task_name)
        start_t = time.time()

        try:
            _, retries_used, _ = execute_with_retry(task_func, task_name, retry_policy)
            result_tracker.state = TaskState.SUCCESS
            result_tracker.retry_count = retries_used
            result_tracker.completed_at = datetime.now(timezone.utc)
            result_tracker.duration_seconds = time.time() - start_t
            result_tracker.task_values = self.task_values.get_all(task_name)
            logger.info("<<< COMPLETED TASK: %s [SUCCESS in %.2fs]", task_name, result_tracker.duration_seconds)
            return True
        except Exception as exc:
            classification = classify_failure(exc)
            result_tracker.state = TaskState.FAILED
            result_tracker.completed_at = datetime.now(timezone.utc)
            result_tracker.duration_seconds = time.time() - start_t
            result_tracker.failure_classification = classification
            result_tracker.error_message = str(exc)
            logger.error("<<< FAILED TASK: %s [%s]: %s", task_name, classification.value, str(exc))
            return False

    def run(self, spark: SparkSession | None = None) -> JobRunAudit:
        """
        Execute the full Lakeflow Jobs DAG.

        DAG Flow:
            validate_landing_batch (retries: 2)
            ➔ bronze_ingestion (retries: 1)
            ➔ silver_transformation (retries: 0 for data quality)
            ➔ [gold_analytics, dimensional_warehouse] in parallel/sequence
            ➔ final_quality_gate
            ➔ publish_run_summary (run_if: ALL_DONE)
        """
        job_start_time = datetime.now(timezone.utc)
        ensure_directories()

        provided_spark = spark is not None
        active_spark = spark or get_spark_session(SparkConfig(app_name="LakeflowLocalOrchestrator"))

        failure_task: str | None = None
        failure_classification: str | None = None
        error_message: str | None = None
        overall_status = "SUCCESS"

        try:
            # Task 1: Validate Landing Batch
            t1_ok = self._execute_task_step(
                task_name="validate_landing_batch",
                task_func=lambda: execute_validate_landing_task(active_spark, self.context, self.task_values),
                retry_policy=RetryPolicy(max_retries=2),
            )
            if not t1_ok:
                raise RuntimeError(f"Task 'validate_landing_batch' failed: {self.task_results['validate_landing_batch'].error_message}")

            # Task 2: Bronze Ingestion
            t2_ok = self._execute_task_step(
                task_name="bronze_ingestion",
                task_func=lambda: execute_bronze_task(active_spark, self.context, self.task_values),
                retry_policy=RetryPolicy(max_retries=1),
            )
            if not t2_ok:
                raise RuntimeError(f"Task 'bronze_ingestion' failed: {self.task_results['bronze_ingestion'].error_message}")

            # Task 3: Silver Transformation & Reconciliation
            t3_ok = self._execute_task_step(
                task_name="silver_transformation",
                task_func=lambda: execute_silver_task(active_spark, self.context, self.task_values),
                retry_policy=RetryPolicy(max_retries=1, retryable_classifications={FailureClassification.TRANSIENT}),
            )
            if not t3_ok:
                raise RuntimeError(f"Task 'silver_transformation' failed: {self.task_results['silver_transformation'].error_message}")

            # Task 4A: Gold Analytics
            t4a_ok = self._execute_task_step(
                task_name="gold_analytics",
                task_func=lambda: execute_gold_task(active_spark, self.context, self.task_values),
                retry_policy=RetryPolicy(max_retries=1),
            )
            if not t4a_ok:
                raise RuntimeError(f"Task 'gold_analytics' failed: {self.task_results['gold_analytics'].error_message}")

            # Task 4B: Dimensional Warehouse (SCD1, SCD2, Facts, EDQ)
            t4b_ok = self._execute_task_step(
                task_name="dimensional_warehouse",
                task_func=lambda: execute_warehouse_task(active_spark, self.context, self.task_values),
                retry_policy=RetryPolicy(max_retries=1, retryable_classifications={FailureClassification.TRANSIENT}),
            )
            if not t4b_ok:
                raise RuntimeError(f"Task 'dimensional_warehouse' failed: {self.task_results['dimensional_warehouse'].error_message}")

            # Task 5: Final Operational Quality Gate
            t5_ok = self._execute_task_step(
                task_name="final_quality_gate",
                task_func=lambda: execute_final_quality_gate_task(active_spark, self.context, self.task_values),
                retry_policy=RetryPolicy(max_retries=0),
            )
            if not t5_ok:
                raise RuntimeError(f"Task 'final_quality_gate' failed: {self.task_results['final_quality_gate'].error_message}")

        except Exception as exc:
            overall_status = "FAILED"
            # Identify first failed task
            for tname, tres in self.task_results.items():
                if tres.state == TaskState.FAILED:
                    failure_task = tname
                    failure_classification = tres.failure_classification.value if tres.failure_classification else "UNKNOWN"
                    error_message = tres.error_message or str(exc)
                    break
            if not failure_task:
                failure_task = "orchestrator"
                failure_classification = classify_failure(exc).value
                error_message = str(exc)

            logger.error("Job execution failed at task '%s' [%s]: %s", failure_task, failure_classification, error_message)

        finally:
            # Task 6: Publish Run Summary (run_if: ALL_DONE)
            audit_record = execute_publish_run_summary_task(
                spark=active_spark,
                context=self.context,
                task_values=self.task_values,
                task_results=self.task_results,
                overall_status=overall_status,
                start_time=job_start_time,
                failure_task=failure_task,
                failure_classification=failure_classification,
                error_message=error_message,
            )

            if not provided_spark:
                stop_spark_session(active_spark)

        return audit_record
