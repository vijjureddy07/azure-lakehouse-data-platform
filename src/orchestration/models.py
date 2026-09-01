"""
Lakeflow Jobs Orchestration Domain Models & State Management (Module 5).

Defines core data structures for job execution, task state transitions,
failure classifications, cross-task communication, and operational audit records.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any


class TaskState(str, Enum):
    """Operational states for individual Lakeflow Jobs tasks."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class FailureClassification(str, Enum):
    """
    Taxonomy of pipeline and task failures.

    Distinguishes transient infrastructure errors (eligible for retry) from
    deterministic data-quality and configuration errors (ineligible for retry).
    """
    TRANSIENT = "TRANSIENT"            # Network blip, storage throttle, lock timeout (retryable)
    DATA_QUALITY = "DATA_QUALITY"      # Reconciliation failure, quality gate violation (non-retryable)
    CONFIGURATION = "CONFIGURATION"    # Missing required parameter, invalid path syntax (non-retryable)
    DEPENDENCY = "DEPENDENCY"          # Upstream task failed, skipping downstream (non-retryable)
    UNKNOWN = "UNKNOWN"                # Unclassified runtime exception


@dataclass
class RunContext:
    """
    Job-level configuration and execution context passed across Lakeflow Jobs DAG.
    """
    orchestration_run_id: str = field(default_factory=lambda: f"orch-run-{uuid.uuid4().hex[:8]}")
    databricks_job_id: str = "retail_lakehouse_lakeflow_job"
    databricks_job_run_id: str = field(default_factory=lambda: f"dbr-run-{uuid.uuid4().hex[:8]}")
    environment: str = "dev"
    ingestion_date: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    adf_run_id: str = field(default_factory=lambda: f"adf-{uuid.uuid4().hex[:8]}")
    storage_account_name: str = "stlakehousedev"
    container_name: str = "lakehouse"
    catalog_name: str = "retail_lakehouse"
    landing_root: Path | str = ""
    delta_root: Path | str = ""
    quarantine_threshold_rate: float = 0.20  # Max acceptable quarantine rate before alert (20%)
    scale_name: str = "small"
    allow_partial_batch: bool = False  # Set to True only in specific test scenarios


class TaskValueStore:
    """
    Thread-safe storage for small operational task values.

    Emulates Databricks Lakeflow cross-task communication:
    `dbutils.jobs.taskValues.set(key, value)` and `dbutils.jobs.taskValues.get(taskKey, key)`.
    """

    def __init__(self) -> None:
        self._store: dict[str, dict[str, Any]] = {}

    def set(self, task_name: str, key: str, value: Any) -> None:
        """Store a task-level operational value."""
        if task_name not in self._store:
            self._store[task_name] = {}
        self._store[task_name][key] = value

    def get(self, task_name: str, key: str, default: Any = None) -> Any:
        """Retrieve a task-level operational value."""
        return self._store.get(task_name, {}).get(key, default)

    def get_all(self, task_name: str) -> dict[str, Any]:
        """Retrieve all values set by a specific task."""
        return self._store.get(task_name, {}).copy()

    def as_dict(self) -> dict[str, dict[str, Any]]:
        """Export complete task values snapshot."""
        return {k: v.copy() for k, v in self._store.items()}


@dataclass
class TaskResult:
    """Execution outcome for an individual Lakeflow task."""
    task_name: str
    state: TaskState = TaskState.PENDING
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_seconds: float = 0.0
    retry_count: int = 0
    failure_classification: FailureClassification | None = None
    error_message: str | None = None
    task_values: dict[str, Any] = field(default_factory=dict)


@dataclass
class JobRunAudit:
    """
    Structured operational audit record persisted to delta/operations/job_run_audit.
    Grain: Exactly ONE row per Lakeflow Job run.
    """
    orchestration_run_id: str
    databricks_job_id: str
    databricks_job_run_id: str
    environment: str
    ingestion_date: str
    adf_run_id: str
    started_at: datetime
    completed_at: datetime
    final_status: str  # SUCCESS, FAILED
    duration_seconds: float
    landing_ready: bool | None = None
    discovered_dataset_count: int | None = None
    bronze_rows_ingested: int | None = None
    silver_valid_rows: int | None = None
    silver_quarantine_rows: int | None = None
    gold_tables_generated: int | None = None
    fact_sales_rows: int | None = None
    quality_status: str | None = None  # PASSED, FAILED, SKIPPED
    quarantine_rate: float | None = None
    quarantine_alert_triggered: bool | None = None
    failure_task: str | None = None
    failure_classification: str | None = None
    error_message: str | None = None
