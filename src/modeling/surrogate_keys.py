"""
Surrogate Key Generator & Key Allocation Utilities.

Provides deterministic, persistent integer surrogate key allocation for Kimball-style
dimensional warehouse models:
- Distinguishes business/natural keys from warehouse surrogate keys.
- Never uses non-deterministic functions like monotonically_increasing_id() for persistent keys.
- Retains existing surrogate key mappings for existing business keys.
- Deterministically assigns new surrogate keys (max_existing_key + row_number()) ordered by natural keys.
- Supports surrogate key 0 as the standardized UNKNOWN member convention.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.functions import max as spark_max
from pyspark.sql.types import IntegerType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def assign_surrogate_keys(
    existing_dim_df: DataFrame | None,
    incoming_df: DataFrame,
    natural_key: str,
    surrogate_key_name: str,
    order_by_cols: list[str] | None = None,
) -> DataFrame:
    """
    Deterministically assign integer surrogate keys to incoming dimension records.

    Preserves previously assigned surrogate keys for known natural keys and
    assigns new incrementing keys to newly observed natural keys.

    Args:
        existing_dim_df: Existing dimension DataFrame (or None if initial load).
        incoming_df: Incoming source DataFrame containing natural_key.
        natural_key: Business key column name (e.g. 'product_id', 'store_id').
        surrogate_key_name: Surrogate key column name (e.g. 'product_key', 'store_key').
        order_by_cols: Optional columns for deterministic ordering of new key assignment.

    Returns:
        DataFrame: Incoming records with surrogate_key_name populated as IntegerType.
    """
    order_cols = [col(c) for c in order_by_cols] if order_by_cols else [col(natural_key)]

    if existing_dim_df is None or surrogate_key_name not in existing_dim_df.columns:
        # Initial dimension load
        win = Window.orderBy(*order_cols)
        return incoming_df.withColumn(
            surrogate_key_name,
            row_number().over(win).cast(IntegerType()),
        )

    # Find highest existing surrogate key (ignoring 0 if used for unknown member)
    max_key_val = existing_dim_df.select(spark_max(col(surrogate_key_name))).collect()[0][0]
    max_key = int(max_key_val) if max_key_val is not None else 0

    # Separate incoming records into existing matches vs genuinely new keys
    existing_key_map = existing_dim_df.select(natural_key, surrogate_key_name).distinct()

    matched_incoming = incoming_df.join(
        existing_key_map,
        on=natural_key,
        how="inner",
    )

    new_incoming = incoming_df.join(
        existing_key_map,
        on=natural_key,
        how="left_anti",
    )

    # Assign new incrementing keys to new natural keys
    win = Window.orderBy(*order_cols)
    new_with_keys = new_incoming.withColumn(
        surrogate_key_name,
        (row_number().over(win) + lit(max_key)).cast(IntegerType()),
    )

    # Combine matched and new records
    return matched_incoming.unionByName(new_with_keys, allowMissingColumns=True)
