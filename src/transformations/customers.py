"""
Customer Data Transformations and Quality Validation.

Transformations:
- Whitespace trimming across string columns
- Email normalization (lowercased) and regex format validation
- State/Country casing normalization
- Explicit DateType parsing for signup_date
- Duplicate primary key detection via window ranking
- Routing into Clean Customers vs Standardized Quarantine
"""

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.quality.rules import DatasetQualityMetric, format_as_quarantine

logger = logging.getLogger(__name__)

EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


def transform_customers(raw_df: DataFrame) -> tuple[DataFrame, DataFrame, DatasetQualityMetric]:
    """
    Clean, validate, and segment raw customer data.

    Returns:
        Tuple of (clean_customers_df, quarantine_df, metric)
    """
    source_count = raw_df.count()
    logger.info("Transforming customers: received %d raw records.", source_count)

    # 1. Base string normalization and date parsing
    normalized_df = (
        raw_df.withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("phone", F.trim(F.col("phone")))
        .withColumn("address", F.trim(F.col("address")))
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
        .withColumn("state", F.upper(F.trim(F.col("state"))))
        .withColumn("country", F.upper(F.trim(F.coalesce(F.col("country"), F.lit("US")))))
        .withColumn("postal_code", F.trim(F.col("postal_code")))
        .withColumn("parsed_signup_date", F.to_date(F.trim(F.col("signup_date")), "yyyy-MM-dd"))
        .withColumn("loyalty_tier", F.upper(F.coalesce(F.trim(F.col("loyalty_tier")), F.lit("STANDARD"))))
        .withColumn(
            "full_name",
            F.concat_ws(" ", F.col("first_name"), F.col("last_name")),
        )
    )

    # 2. Duplicate detection on customer_id
    window_spec = Window.partitionBy("customer_id").orderBy(
        F.col("parsed_signup_date").desc_nulls_last(),
        F.col("email").asc_nulls_last()
    )
    ranked_df = normalized_df.withColumn("row_num", F.row_number().over(window_spec))

    # 3. Defect classification
    classified_df = ranked_df.withColumn(
        "rejection_reason",
        F.when(
            (F.col("customer_id").isNull()) | (F.col("customer_id") == "") |
            (F.col("email").isNull()) | (F.col("email") == ""),
            F.lit("NULL_MANDATORY_FIELD"),
        )
        .when(F.col("row_num") > 1, F.lit("DUPLICATE_CUSTOMER_ID"))
        .when(~F.col("email").rlike(EMAIL_REGEX), F.lit("INVALID_EMAIL_FORMAT"))
        .when(
            F.col("parsed_signup_date").isNull() &
            F.col("signup_date").isNotNull() &
            (F.trim(F.col("signup_date")) != ""),
            F.lit("MALFORMED_SIGNUP_DATE"),
        )
        .otherwise(F.lit(None))
    )

    # 4. Split into Clean vs Invalid
    clean_df = (
        classified_df.filter(F.col("rejection_reason").isNull())
        .withColumn("signup_date", F.col("parsed_signup_date"))
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "full_name",
            "email",
            "phone",
            "address",
            "city",
            "state",
            "country",
            "postal_code",
            "signup_date",
            "loyalty_tier",
        )
    )

    invalid_df = classified_df.filter(F.col("rejection_reason").isNotNull())
    quarantine_df = format_as_quarantine(
        invalid_df,
        record_id_col="customer_id",
        source_dataset="customers",
        rejection_reason_col="rejection_reason",
    )

    # Counts & Metrics
    valid_count = clean_df.count()
    quarantine_count = quarantine_df.count()
    duplicate_count = classified_df.filter(F.col("rejection_reason") == "DUPLICATE_CUSTOMER_ID").count()
    null_mand_count = classified_df.filter(F.col("rejection_reason") == "NULL_MANDATORY_FIELD").count()

    metric = DatasetQualityMetric(
        dataset_name="customers",
        source_row_count=source_count,
        valid_row_count=valid_count,
        quarantine_row_count=quarantine_count,
        duplicate_count=duplicate_count,
        null_mandatory_count=null_mand_count,
        referential_orphan_count=0,
    )

    logger.info("Customers transformation complete: %d valid, %d quarantined.", valid_count, quarantine_count)
    return clean_df, quarantine_df, metric
