"""
Product Data Transformations and Quality Validation.

Transformations:
- String trimming across descriptive columns
- Category & Subcategory title-casing normalization
- DecimalType casting for unit_price and cost_price (monetary precision)
- Boolean normalization for is_active
- Non-positive price rejection and duplicate product_id filtering
- Routing into Clean Products vs Standardized Quarantine
"""

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.quality.rules import DatasetQualityMetric, format_as_quarantine

logger = logging.getLogger(__name__)


def transform_products(raw_df: DataFrame) -> tuple[DataFrame, DataFrame, DatasetQualityMetric]:
    """
    Clean, validate, and type-cast product dataset.

    Returns:
        Tuple of (clean_products_df, quarantine_df, metric)
    """
    source_count = raw_df.count()
    logger.info("Transforming products: received %d raw records.", source_count)

    # 1. Base string normalization and monetary casting
    normalized_df = (
        raw_df.withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("product_sku", F.trim(F.col("product_sku")))
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category", F.initcap(F.trim(F.col("category"))))
        .withColumn("subcategory", F.initcap(F.trim(F.col("subcategory"))))
        .withColumn("parsed_unit_price", F.col("unit_price").cast(DecimalType(10, 2)))
        .withColumn("parsed_cost_price", F.col("cost_price").cast(DecimalType(10, 2)))
        .withColumn(
            "is_active_bool",
            F.when(
                F.lower(F.trim(F.col("is_active"))).isin("true", "t", "1", "yes", "y"),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .withColumn("description", F.trim(F.col("description")))
    )

    # 2. Duplicate detection on product_id
    window_spec = Window.partitionBy("product_id").orderBy(F.col("product_sku").asc_nulls_last())
    ranked_df = normalized_df.withColumn("row_num", F.row_number().over(window_spec))

    # 3. Defect classification
    classified_df = ranked_df.withColumn(
        "rejection_reason",
        F.when(
            (F.col("product_id").isNull()) | (F.col("product_id") == "") |
            (F.col("product_name").isNull()) | (F.col("product_name") == "") |
            (F.col("parsed_unit_price").isNull()),
            F.lit("NULL_MANDATORY_FIELD"),
        )
        .when(F.col("row_num") > 1, F.lit("DUPLICATE_PRODUCT_ID"))
        .when(F.col("parsed_unit_price") <= F.lit(0), F.lit("INVALID_PRICE_NON_POSITIVE"))
        .otherwise(F.lit(None))
    )

    # 4. Split into Clean vs Invalid
    clean_df = (
        classified_df.filter(F.col("rejection_reason").isNull())
        .withColumn("unit_price", F.col("parsed_unit_price"))
        .withColumn("cost_price", F.col("parsed_cost_price"))
        .withColumn("is_active", F.col("is_active_bool"))
        .select(
            "product_id",
            "product_sku",
            "product_name",
            "category",
            "subcategory",
            "unit_price",
            "cost_price",
            "is_active",
            "description",
        )
    )

    invalid_df = classified_df.filter(F.col("rejection_reason").isNotNull())
    quarantine_df = format_as_quarantine(
        invalid_df,
        record_id_col="product_id",
        source_dataset="products",
        rejection_reason_col="rejection_reason",
    )

    valid_count = clean_df.count()
    quarantine_count = quarantine_df.count()
    duplicate_count = classified_df.filter(F.col("rejection_reason") == "DUPLICATE_PRODUCT_ID").count()
    null_mand_count = classified_df.filter(F.col("rejection_reason") == "NULL_MANDATORY_FIELD").count()

    metric = DatasetQualityMetric(
        dataset_name="products",
        source_row_count=source_count,
        valid_row_count=valid_count,
        quarantine_row_count=quarantine_count,
        duplicate_count=duplicate_count,
        null_mandatory_count=null_mand_count,
        referential_orphan_count=0,
    )

    logger.info("Products transformation complete: %d valid, %d quarantined.", valid_count, quarantine_count)
    return clean_df, quarantine_df, metric
