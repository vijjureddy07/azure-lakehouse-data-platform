"""
Data Quality Rules, Quarantine Handlers, and Metric Reconciliation Framework.

Core principles:
- Transparent and audit-ready: No silent dropping of corrupted or invalid records.
- Mutually exclusive or properly accounted routing into Valid vs Quarantine.
- Unified Quarantine schema capturing original raw payload, rejection reason, source dataset, and timestamp.
- Explicit metric reconciliation: source_count == valid_count + quarantine_count (or documented duplicate handling).
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.schemas.retail_schemas import QUALITY_METRICS_SCHEMA

logger = logging.getLogger(__name__)


@dataclass
class DatasetQualityMetric:
    """Dataclass holding validation counts for a specific dataset."""
    dataset_name: str
    source_row_count: int = 0
    valid_row_count: int = 0
    quarantine_row_count: int = 0
    duplicate_count: int = 0
    null_mandatory_count: int = 0
    referential_orphan_count: int = 0
    calculated_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_name": self.dataset_name,
            "source_row_count": self.source_row_count,
            "valid_row_count": self.valid_row_count,
            "quarantine_row_count": self.quarantine_row_count,
            "duplicate_count": self.duplicate_count,
            "null_mandatory_count": self.null_mandatory_count,
            "referential_orphan_count": self.referential_orphan_count,
            "calculated_at": self.calculated_at,
        }


def format_as_quarantine(
    df: DataFrame,
    record_id_col: str,
    source_dataset: str,
    rejection_reason_col: str,
) -> DataFrame:
    """
    Format rejected records into the standard QUARANTINE_SCHEMA.

    Converts all raw columns into a structured JSON string audit trail
    along with the specific rejection reason.
    """
    # Columns to serialize into raw_record
    cols_to_json = [c for c in df.columns if c not in [rejection_reason_col]]

    quarantine_df = (
        df.withColumn("record_id", F.col(record_id_col).cast("string"))
        .withColumn("source_dataset", F.lit(source_dataset))
        .withColumn("rejection_reason", F.col(rejection_reason_col))
        .withColumn("raw_record", F.to_json(F.struct([F.col(c) for c in cols_to_json])))
        .withColumn("ingested_at", F.current_timestamp())
        .select("record_id", "source_dataset", "rejection_reason", "raw_record", "ingested_at")
    )
    return quarantine_df


def find_orphans(
    child_df: DataFrame,
    parent_df: DataFrame,
    child_fk: str,
    parent_pk: str,
) -> DataFrame:
    """
    Identify orphan records in child_df that have no matching parent_pk in parent_df.
    Uses LEFT_ANTI join for high-performance Spark execution.
    """
    return child_df.join(parent_df, child_df[child_fk] == parent_df[parent_pk], "left_anti")


class DataQualityReconciliationError(ValueError):
    """Raised when source_row_count does not equal valid_row_count + quarantine_row_count."""


def validate_reconciliation(metrics_list: list[DatasetQualityMetric]) -> None:
    """
    Enforce the fundamental data quality reconciliation invariant:
    source_row_count == valid_row_count + quarantine_row_count

    Raises:
        DataQualityReconciliationError: If any dataset's counts do not reconcile.
    """
    for m in metrics_list:
        expected_source = m.valid_row_count + m.quarantine_row_count
        if m.source_row_count != expected_source:
            raise DataQualityReconciliationError(
                f"Data quality reconciliation failed for dataset '{m.dataset_name}': "
                f"source_row_count={m.source_row_count} != "
                f"valid_row_count={m.valid_row_count} + quarantine_row_count={m.quarantine_row_count} "
                f"(sum={expected_source})."
            )


def metrics_to_dataframe(spark: SparkSession, metrics_list: list[DatasetQualityMetric]) -> DataFrame:
    """Convert in-memory metrics list into a PySpark DataFrame adhering to QUALITY_METRICS_SCHEMA."""
    if not metrics_list:
        return spark.createDataFrame([], QUALITY_METRICS_SCHEMA)

    rows = [m.to_dict() for m in metrics_list]
    return spark.createDataFrame(rows, schema=QUALITY_METRICS_SCHEMA)
