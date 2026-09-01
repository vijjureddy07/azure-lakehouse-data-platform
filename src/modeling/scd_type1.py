"""
Slowly Changing Dimension (SCD) Type 1 Processor.

Implements Kimball SCD Type 1 in-place overwrite semantics using Delta Lake MERGE:
- Natural Key: product_id
- Surrogate Key: product_key (immutable once assigned)
- Tracked Type-1 Attributes: product_name, product_sku, category, subcategory, cost_price, unit_price, is_active
- New Product: Assigned a new surrogate key and inserted.
- Changed Product Attributes: Overwrites existing attribute values in-place without altering the surrogate key.
- Unchanged Product: Left completely untouched (no-op).
- Rerun Idempotency: Repeated batch executions produce zero duplicate rows and zero key changes.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from delta.tables import DeltaTable

from src.modeling.surrogate_keys import assign_surrogate_keys

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def process_dim_product_scd1(
    spark: SparkSession,
    silver_products_df: DataFrame,
    dim_product_path: Path | str,
) -> DataFrame:
    """
    Process Silver products into Type 1 dim_product Delta table.

    Args:
        spark: Active SparkSession.
        silver_products_df: Cleaned Silver products DataFrame.
        dim_product_path: Path or ABFSS URI to dim_product Delta table.

    Returns:
        DataFrame: The current state of dim_product.
    """
    path_str = str(dim_product_path)
    existing_df: DataFrame | None = None
    if DeltaTable.isDeltaTable(spark, path_str):
        existing_df = spark.read.format("delta").load(path_str)

    incoming_clean = (
        silver_products_df
        .select(
            "product_id",
            "product_sku",
            "product_name",
            "category",
            "subcategory",
            "cost_price",
            "unit_price",
            "is_active",
        )
        .distinct()
    )

    incoming_with_keys = assign_surrogate_keys(
        existing_dim_df=existing_df,
        incoming_df=incoming_clean,
        natural_key="product_id",
        surrogate_key_name="product_key",
        order_by_cols=["product_id"],
    )

    if not DeltaTable.isDeltaTable(spark, path_str):
        incoming_with_keys.write.format("delta").mode("overwrite").save(path_str)
        logger.info("Initialized dim_product SCD1 table at %s with %d rows", path_str, incoming_with_keys.count())
    else:
        delta_tbl = DeltaTable.forPath(spark, path_str)
        delta_tbl.alias("target").merge(
            incoming_with_keys.alias("source"),
            "target.product_id = source.product_id",
        ).whenMatchedUpdate(
            condition="""
                NOT (target.product_name <=> source.product_name) OR
                NOT (target.product_sku <=> source.product_sku) OR
                NOT (target.category <=> source.category) OR
                NOT (target.subcategory <=> source.subcategory) OR
                NOT (target.cost_price <=> source.cost_price) OR
                NOT (target.unit_price <=> source.unit_price) OR
                NOT (target.is_active <=> source.is_active)
            """,
            set={
                "product_name": "source.product_name",
                "product_sku": "source.product_sku",
                "category": "source.category",
                "subcategory": "source.subcategory",
                "cost_price": "source.cost_price",
                "unit_price": "source.unit_price",
                "is_active": "source.is_active",
            },
        ).whenNotMatchedInsertAll().execute()
        logger.info("Successfully merged SCD1 updates into dim_product at %s", path_str)

    return spark.read.format("delta").load(path_str)
