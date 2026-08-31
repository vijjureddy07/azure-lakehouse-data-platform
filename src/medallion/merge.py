"""
Delta Lake MERGE / Upsert Operations.

Implements idempotent, ACID-compliant MERGE operations for Silver tables
using Delta Lake APIs (DeltaTable.merge):
- Matches records on business primary keys (e.g. customer_id, product_id, order_id).
- Updates existing records when attributes have changed.
- Inserts new incoming records that do not currently exist in the target table.
- Guarantees rerun idempotency (rerunning unchanged batches creates zero duplicate rows).
- Leaves unrelated target records completely unaffected.

Comparison:
- APPEND: Blindly appends rows, resulting in duplicate records if batches re-run.
- OVERWRITE: Replaces all partitions or the entire table, losing historical target state.
- MERGE: Granular row-level ACID upsert reconciling source increments with existing state.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from delta.tables import DeltaTable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def upsert_delta_table(
    spark: SparkSession,
    target_table_path: Path,
    source_df: DataFrame,
    primary_key: str,
    update_condition: str | None = None,
) -> None:
    """
    Perform an idempotent ACID MERGE (upsert) into a target Delta table.

    Args:
        spark: Active SparkSession.
        target_table_path: Path to existing target Delta table.
        source_df: DataFrame containing incoming updates and new inserts.
        primary_key: Name of the primary key column to match on.
        update_condition: Optional SQL condition for updating matched rows (e.g. 'source.updated_at > target.updated_at').
    """
    if not target_table_path.exists():
        logger.info("Target Delta table does not exist at %s. Initializing with source data.", target_table_path)
        source_df.write.format("delta").mode("overwrite").save(str(target_table_path))
        return

    delta_target = DeltaTable.forPath(spark, str(target_table_path))
    match_expr = f"target.{primary_key} = source.{primary_key}"

    merge_builder = delta_target.alias("target").merge(
        source_df.alias("source"),
        match_expr,
    )

    if update_condition:
        merge_builder = merge_builder.whenMatchedUpdateAll(condition=update_condition)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    merge_builder = merge_builder.whenNotMatchedInsertAll()
    merge_builder.execute()

    logger.info("Successfully executed Delta MERGE on %s matching on %s", target_table_path, primary_key)


def upsert_customers(
    spark: SparkSession,
    silver_customers_path: Path,
    incoming_customers_df: DataFrame,
) -> None:
    """Idempotent MERGE for Silver Customers dimension."""
    upsert_delta_table(
        spark=spark,
        target_table_path=silver_customers_path,
        source_df=incoming_customers_df,
        primary_key="customer_id",
    )


def upsert_products(
    spark: SparkSession,
    silver_products_path: Path,
    incoming_products_df: DataFrame,
) -> None:
    """Idempotent MERGE for Silver Products dimension."""
    upsert_delta_table(
        spark=spark,
        target_table_path=silver_products_path,
        source_df=incoming_products_df,
        primary_key="product_id",
    )


def upsert_orders(
    spark: SparkSession,
    silver_orders_path: Path,
    incoming_orders_df: DataFrame,
) -> None:
    """Idempotent MERGE for Silver Orders table."""
    upsert_delta_table(
        spark=spark,
        target_table_path=silver_orders_path,
        source_df=incoming_orders_df,
        primary_key="order_id",
    )
