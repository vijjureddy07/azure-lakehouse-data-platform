"""
Slowly Changing Dimension (SCD) Type 2 Processor.

Implements Kimball SCD Type 2 history-preserving dimension tracking for dim_customer:
- Natural Key: customer_id
- Surrogate Key: customer_key (unique per version)
- Tracked Attributes: loyalty_tier, address, city, state, postal_code
- Deterministic Comparison: SHA-256 attribute_hash over tracked fields.
- Validity Intervals: Half-open interval convention [effective_from, effective_to)
- Current Record Invariant: is_current = True, effective_to = NULL
- Expired Record Invariant: is_current = False, effective_to = change_timestamp
- Version Tracking: version_number increments on each change (1, 2, 3, ...)
- Idempotency Guarantee: Re-running unchanged batches creates zero duplicate versions.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    lit,
    row_number,
    sha2,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    TimestampType,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

TRACKED_SCD2_COLS = ["loyalty_tier", "address", "city", "state", "postal_code"]


class SCD2TemporalOrderError(ValueError):
    """Raised when an SCD2 update is attempted with a timestamp on or before the active version's effective_from."""
    pass


def compute_customer_attribute_hash(df: DataFrame) -> DataFrame:
    """Compute SHA-256 attribute hash over tracked SCD Type 2 columns."""
    hash_expr = sha2(
        concat_ws(
            "||",
            *[coalesce(col(c).cast(StringType()), lit("<NULL>")) for c in TRACKED_SCD2_COLS],
        ),
        256,
    )
    return df.withColumn("attribute_hash", hash_expr)


DIM_CUSTOMER_COLS = [
    "customer_key",
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "address",
    "city",
    "state",
    "postal_code",
    "country",
    "signup_date",
    "loyalty_tier",
    "attribute_hash",
    "effective_from",
    "effective_to",
    "is_current",
    "version_number",
]


def process_dim_customer_scd2(
    spark: SparkSession,
    silver_customers_df: DataFrame,
    dim_customer_path: Path | str,
    batch_timestamp: datetime | None = None,
    initial_effective_from: datetime | None = None,
) -> DataFrame:
    """
    Process Silver customers into SCD Type 2 dim_customer Delta table.

    Args:
        spark: Active SparkSession.
        silver_customers_df: Conformed Silver customers DataFrame.
        dim_customer_path: Path or ABFSS URI to dim_customer Delta table.
        batch_timestamp: Effective timestamp for changes (defaults to UTC now).
        initial_effective_from: Explicit history start timestamp for initial warehouse load.
                                If omitted on initial load, falls back to batch_timestamp.

    Returns:
        DataFrame: Complete current state of dim_customer.
    """
    path_str = str(dim_customer_path)
    now_ts = batch_timestamp or datetime.now(timezone.utc)

    # Standardize incoming customer columns and compute comparison hash
    incoming_clean = (
        silver_customers_df
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "address",
            "city",
            "state",
            "postal_code",
            "country",
            "signup_date",
            "loyalty_tier",
        )
        .distinct()
    )
    incoming_hashed = compute_customer_attribute_hash(incoming_clean)

    # --- CASE 1: INITIAL DIMENSION LOAD ---
    if not DeltaTable.isDeltaTable(spark, path_str):
        # Initial load effective_from: initial_effective_from or batch_timestamp (never signup_date)
        eff_from_ts = initial_effective_from if initial_effective_from is not None else now_ts
        win = Window.orderBy("customer_id")
        initial_dim = (
            incoming_hashed
            .withColumn("customer_key", row_number().over(win).cast(IntegerType()))
            .withColumn(
                "effective_from",
                lit(eff_from_ts).cast(TimestampType()),
            )
            .withColumn("effective_to", lit(None).cast(TimestampType()))
            .withColumn("is_current", lit(True).cast(BooleanType()))
            .withColumn("version_number", lit(1).cast(IntegerType()))
            .select(*DIM_CUSTOMER_COLS)
        )

        initial_dim.write.format("delta").mode("overwrite").save(path_str)
        logger.info(
            "Initialized dim_customer SCD2 table at %s with %d rows (effective_from=%s)",
            path_str,
            initial_dim.count(),
            eff_from_ts,
        )
        return spark.read.format("delta").load(path_str)

    # --- CASE 2: INCREMENTAL / SCD2 MERGE PROCESSING ---
    existing_dim = spark.read.format("delta").load(path_str)
    max_key_val = existing_dim.select(spark_max(col("customer_key"))).collect()[0][0]
    max_existing_key = int(max_key_val) if max_key_val is not None else 0

    current_active = existing_dim.filter(col("is_current") == lit(True)).select(
        col("customer_id").alias("cur_customer_id"),
        col("attribute_hash").alias("cur_attribute_hash"),
        col("version_number").alias("prev_version"),
        col("effective_from").alias("cur_effective_from"),
    )

    # Join incoming with current active dimension records
    joined = incoming_hashed.join(
        current_active,
        incoming_hashed.customer_id == current_active.cur_customer_id,
        how="left",
    )

    # 1. Genuinely new customers (never seen before in dim_customer)
    new_customers_df = (
        joined
        .filter(col("cur_customer_id").isNull())
        .drop("cur_customer_id", "cur_attribute_hash", "prev_version", "cur_effective_from")
    )

    # 2. Changed customers (active record exists but tracked attribute_hash differs)
    changed_customers_df = (
        joined
        .filter(
            col("cur_customer_id").isNotNull()
            & (col("cur_attribute_hash") != col("attribute_hash"))
        )
        .drop("cur_customer_id", "cur_attribute_hash")
    )

    new_count = new_customers_df.count()
    changed_count = changed_customers_df.count()

    if new_count == 0 and changed_count == 0:
        logger.info("dim_customer SCD2: No new or changed records detected. Table is up to date.")
        return spark.read.format("delta").load(path_str)

    # Validation: Prevent out-of-order SCD2 mutations BEFORE modifying Delta table
    if changed_count > 0:
        invalid_orders = changed_customers_df.filter(
            col("cur_effective_from") >= lit(now_ts).cast(TimestampType())
        ).collect()
        if invalid_orders:
            bad_cid = invalid_orders[0]["customer_id"]
            bad_eff = invalid_orders[0]["cur_effective_from"]
            raise SCD2TemporalOrderError(
                f"Cannot apply out-of-order SCD2 change for customer '{bad_cid}': "
                f"change timestamp ({now_ts}) must be strictly later than active version "
                f"effective_from ({bad_eff})."
            )

    logger.info("dim_customer SCD2: Processing %d new customers and %d changed customers", new_count, changed_count)

    # Step A: Prepare and materialize new version rows BEFORE modifying the Delta table
    new_rows_list = []
    if new_count > 0:
        new_cust_win = Window.orderBy("customer_id")
        new_cust_rows = (
            new_customers_df
            .withColumn("customer_key", (row_number().over(new_cust_win) + lit(max_existing_key)).cast(IntegerType()))
            .withColumn(
                "effective_from",
                lit(now_ts).cast(TimestampType()),
            )
            .withColumn("effective_to", lit(None).cast(TimestampType()))
            .withColumn("is_current", lit(True).cast(BooleanType()))
            .withColumn("version_number", lit(1).cast(IntegerType()))
            .select(*DIM_CUSTOMER_COLS)
        )
        new_rows_list.append(new_cust_rows)

    if changed_count > 0:
        new_key_offset = max_existing_key + new_count
        changed_win = Window.orderBy("customer_id")
        changed_new_rows = (
            changed_customers_df
            .withColumn("customer_key", (row_number().over(changed_win) + lit(new_key_offset)).cast(IntegerType()))
            .withColumn("effective_from", lit(now_ts).cast(TimestampType()))
            .withColumn("effective_to", lit(None).cast(TimestampType()))
            .withColumn("is_current", lit(True).cast(BooleanType()))
            .withColumn("version_number", (col("prev_version") + lit(1)).cast(IntegerType()))
            .select(*DIM_CUSTOMER_COLS)
        )
        new_rows_list.append(changed_new_rows)

    materialized_rows = []
    if new_rows_list:
        combined_df = new_rows_list[0]
        for additional_df in new_rows_list[1:]:
            combined_df = combined_df.unionByName(additional_df)
        materialized_rows = combined_df.collect()

    # Step B: Expire changed current active records in Delta table
    if changed_count > 0:
        changed_ids = [row["customer_id"] for row in changed_customers_df.select("customer_id").distinct().collect()]
        changed_ids_df = spark.createDataFrame([(cid,) for cid in changed_ids], ["customer_id"])
        delta_tbl = DeltaTable.forPath(spark, path_str)
        delta_tbl.alias("target").merge(
            changed_ids_df.alias("source"),
            "target.customer_id = source.customer_id AND target.is_current = true",
        ).whenMatchedUpdate(
            set={
                "effective_to": f"cast('{now_ts.isoformat()}' as timestamp)",
                "is_current": "false",
            }
        ).execute()

    # Step C: Append materialized new rows
    if materialized_rows:
        target_schema = existing_dim.select(*DIM_CUSTOMER_COLS).schema
        insert_df = spark.createDataFrame(materialized_rows, schema=target_schema)
        insert_df.write.format("delta").mode("append").save(path_str)
        logger.info("dim_customer SCD2: Appended %d new version records to %s", len(materialized_rows), path_str)

    return spark.read.format("delta").load(path_str)
