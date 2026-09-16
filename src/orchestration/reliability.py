"""
Reliability, Retries, and Failure Classification Framework (Module 5).

Implements intelligent retry policies distinguishing transient infrastructure failures
from deterministic data-quality and configuration errors.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from src.medallion.discovery import LandingDiscoveryError, LandingPathError
from src.medallion.silver import ReconciliationError
from src.modeling.quality import WarehouseQualityGateError
from src.modeling.scd_type2 import SCD2TemporalOrderError
from src.orchestration.models import FailureClassification

logger = logging.getLogger(__name__)


class QuarantineThresholdExceededError(RuntimeError):
    """Raised when the Silver layer quarantine rate exceeds the configured operational threshold."""
    pass


def sanitize_failure_message(message: str) -> str:
    """Sanitize secrets, passwords, tokens, SAS keys, and file:/// links from error messages."""
    sanitized = re.sub(
        r"(sig|SharedAccessSignature|password|token|secret|account_key)=([^;&\s]+)",
        r"\1=REDACTED",
        message,
        flags=re.IGNORECASE,
    )
    sanitized = re.sub(r"dapi[a-f0-9]{32}", "dapi[REDACTED]", sanitized)
    sanitized = re.sub(r"eyJ[a-zA-Z0-9_\-]{20,}\.[a-zA-Z0-9_\-]{20,}", "[REDACTED_JWT]", sanitized)
    return sanitized


def classify_failure(exc: Exception) -> FailureClassification:
    """
    Classify an exception into an operational failure category.

    Key Architectural Rule:
    - DATA_QUALITY: Deterministic failures (e.g. Broken reconciliation, failed quality gates,
      quarantine threshold exceeded, out-of-order temporal intervals). Retrying with unchanged inputs
      will NOT fix the problem.
    - TRANSIENT: Temporary infrastructure/network/storage issues. Retrying may succeed.
    - CONFIGURATION: Missing parameters or malformed landing contract paths.
    - DEPENDENCY: Upstream task failure cascade or missing cloud dependencies.
    """
    if isinstance(
        exc,
        (
            WarehouseQualityGateError,
            ReconciliationError,
            SCD2TemporalOrderError,
            QuarantineThresholdExceededError,
        ),
    ):
        return FailureClassification.DATA_QUALITY

    if isinstance(exc, LandingPathError):
        return FailureClassification.CONFIGURATION

    if isinstance(exc, LandingDiscoveryError):
        if exc.__cause__ and isinstance(exc.__cause__, (TimeoutError, ConnectionError, OSError)):
            return FailureClassification.TRANSIENT
        return FailureClassification.DEPENDENCY

    if isinstance(exc, (FileNotFoundError, TimeoutError, ConnectionError, IOError, OSError)):
        return FailureClassification.TRANSIENT

    if isinstance(exc, (KeyError, ValueError)):
        return FailureClassification.CONFIGURATION

    return FailureClassification.UNKNOWN


@dataclass
class RetryPolicy:
    """
    Configurable retry policy for Lakeflow tasks.
    """
    max_retries: int = 1
    backoff_seconds: float = 0.05
    retryable_classifications: set[FailureClassification] = field(
        default_factory=lambda: {FailureClassification.TRANSIENT}
    )

    def is_retryable(self, exc: Exception) -> bool:
        """Determine if an exception is eligible for retry under this policy."""
        classification = classify_failure(exc)
        return classification in self.retryable_classifications

    def should_retry(self, exc: Exception, attempt: int = 0) -> bool:
        """Determine if an exception should be retried given current attempt count."""
        return self.is_retryable(exc) and attempt < self.max_retries


def execute_with_retry(
    task_func: Callable[[], Any],
    task_name: str,
    retry_policy: RetryPolicy,
) -> tuple[Any, int, FailureClassification | None]:
    """
    Execute a task function with policy-driven retry evaluation.

    Returns:
        tuple of (result, total_retries_attempted, final_failure_classification)

    Raises:
        Exception: If retries are exhausted or if the failure is non-retryable.
    """
    attempts = 0
    while True:
        try:
            result = task_func()
            return result, attempts, None
        except Exception as exc:
            classification = classify_failure(exc)
            logger.warning(
                "Task '%s' failed on attempt %d with %s [%s]: %s",
                task_name,
                attempts + 1,
                type(exc).__name__,
                classification.value,
                str(exc),
            )

            # Check if this failure type is permitted to retry
            if not retry_policy.is_retryable(exc):
                logger.info(
                    "Task '%s' failure classified as %s (NON-RETRYABLE). Aborting retries immediately.",
                    task_name,
                    classification.value,
                )
                raise exc

            # If retryable, check retry limit
            if attempts >= retry_policy.max_retries:
                logger.error(
                    "Task '%s' exhausted all %d retry attempt(s). Raising terminal error.",
                    task_name,
                    retry_policy.max_retries,
                )
                raise exc

            attempts += 1
            logger.info("Retrying task '%s' (Attempt %d/%d)...", task_name, attempts, retry_policy.max_retries)
            time.sleep(retry_policy.backoff_seconds)
