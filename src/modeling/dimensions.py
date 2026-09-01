"""
Warehouse Dimension Builders.

Constructs core dimensional warehouse tables:
1. dim_date: Deterministic calendar dimension with rich temporal attributes and unknown member 0.
2. dim_store: Type 1 store dimension with persisted store_key surrogate allocation.
3. dim_employee: Type 1 employee dimension with store surrogate key linkage.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    concat,
    date_format,
    dayofmonth,
    dayofweek,
    dayofyear,
    explode,
    last_day,
    lit,
    month,
    quarter,
    weekofyear,
    when,
    year,
)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
)

from src.modeling.surrogate_keys import assign_surrogate_keys

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def build_dim_date(
    spark: SparkSession,
    start_date: str = "2020-01-01",
    end_date: str = "2030-12-31",
) -> DataFrame:
    """
    Generate a deterministic calendar dimension DataFrame.

    Generates integer date_key in yyyyMMdd format and derives comprehensive
    calendar attributes, including an unknown member record (date_key = 0).
    """
    # 1. Generate date sequence DataFrame
    date_seq_df = spark.sql(f"SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as dates")
    dates_df = date_seq_df.select(explode(col("dates")).alias("full_date"))

    calendar_df = (
        dates_df
        .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast(IntegerType()))
        .withColumn("full_date", col("full_date").cast(DateType()))
        .withColumn("day_of_week", dayofweek(col("full_date")).cast(IntegerType()))
        .withColumn("day_name", date_format(col("full_date"), "EEEE"))
        .withColumn("day_of_month", dayofmonth(col("full_date")).cast(IntegerType()))
        .withColumn("day_of_year", dayofyear(col("full_date")).cast(IntegerType()))
        .withColumn("week_of_year", weekofyear(col("full_date")).cast(IntegerType()))
        .withColumn("month", month(col("full_date")).cast(IntegerType()))
        .withColumn("month_name", date_format(col("full_date"), "MMMM"))
        .withColumn("quarter", quarter(col("full_date")).cast(IntegerType()))
        .withColumn("quarter_name", concat(lit("Q"), quarter(col("full_date")).cast(StringType())))
        .withColumn("year", year(col("full_date")).cast(IntegerType()))
        .withColumn(
            "is_weekend",
            when(dayofweek(col("full_date")).isin(1, 7), lit(True)).otherwise(lit(False)).cast(BooleanType()),
        )
        .withColumn(
            "is_month_end",
            when(last_day(col("full_date")) == col("full_date"), lit(True)).otherwise(lit(False)).cast(BooleanType()),
        )
        .select(
            "date_key",
            "full_date",
            "day_of_week",
            "day_name",
            "day_of_month",
            "day_of_year",
            "week_of_year",
            "month",
            "month_name",
            "quarter",
            "quarter_name",
            "year",
            "is_weekend",
            "is_month_end",
        )
    )

    # 2. Append unknown date record (key 0)
    unknown_schema = calendar_df.schema
    unknown_data = [(
        0, date(1900, 1, 1), 0, "Unknown", 0, 0, 0, 0, "Unknown", 0, "Unknown", 1900, False, False
    )]
    unknown_df = spark.createDataFrame(unknown_data, schema=unknown_schema)

    return unknown_df.unionByName(calendar_df)


def process_dim_date(
    spark: SparkSession,
    dim_date_path: Path | str,
    start_date: str = "2020-01-01",
    end_date: str = "2030-12-31",
) -> DataFrame:
    """Build and persist dim_date Delta table."""
    path_str = str(dim_date_path)
    df = build_dim_date(spark, start_date=start_date, end_date=end_date)
    df.write.format("delta").mode("overwrite").save(path_str)
    logger.info("Persisted dim_date with %d rows to %s", df.count(), path_str)
    return spark.read.format("delta").load(path_str)


def process_dim_store(
    spark: SparkSession,
    silver_stores_df: DataFrame,
    dim_store_path: Path | str,
) -> DataFrame:
    """
    Build and persist Type 1 dim_store Delta table.

    Assigns deterministic store_key surrogate keys and updates attributes in-place.
    """
    path_str = str(dim_store_path)
    existing_df: DataFrame | None = None
    if DeltaTable.isDeltaTable(spark, path_str):
        existing_df = spark.read.format("delta").load(path_str)

    incoming_clean = (
        silver_stores_df
        .select(
            "store_id",
            "store_name",
            "store_type",
            "region",
            "state",
            "country",
            "opened_date",
        )
        .distinct()
    )

    dim_with_keys = assign_surrogate_keys(
        existing_dim_df=existing_df,
        incoming_df=incoming_clean,
        natural_key="store_id",
        surrogate_key_name="store_key",
        order_by_cols=["store_id"],
    )

    if not DeltaTable.isDeltaTable(spark, path_str):
        dim_with_keys.write.format("delta").mode("overwrite").save(path_str)
    else:
        delta_tbl = DeltaTable.forPath(spark, path_str)
        delta_tbl.alias("t").merge(
            dim_with_keys.alias("s"),
            "t.store_id = s.store_id",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    logger.info("Processed dim_store at %s", path_str)
    return spark.read.format("delta").load(path_str)


def process_dim_employee(
    spark: SparkSession,
    silver_employees_df: DataFrame,
    dim_store_df: DataFrame,
    dim_employee_path: Path | str,
) -> DataFrame:
    """
    Build and persist Type 1 dim_employee Delta table linking store surrogate key.
    """
    path_str = str(dim_employee_path)
    existing_df: DataFrame | None = None
    if DeltaTable.isDeltaTable(spark, path_str):
        existing_df = spark.read.format("delta").load(path_str)

    store_lookup = dim_store_df.select("store_id", "store_key")

    incoming_clean = (
        silver_employees_df
        .join(store_lookup, on="store_id", how="left")
        .select(
            "employee_id",
            "store_key",
            "store_id",
            "first_name",
            "last_name",
            "email",
            "role",
            "hire_date",
            "is_active",
        )
        .distinct()
    )

    dim_with_keys = assign_surrogate_keys(
        existing_dim_df=existing_df,
        incoming_df=incoming_clean,
        natural_key="employee_id",
        surrogate_key_name="employee_key",
        order_by_cols=["employee_id"],
    )

    if not DeltaTable.isDeltaTable(spark, path_str):
        dim_with_keys.write.format("delta").mode("overwrite").save(path_str)
    else:
        delta_tbl = DeltaTable.forPath(spark, path_str)
        delta_tbl.alias("t").merge(
            dim_with_keys.alias("s"),
            "t.employee_id = s.employee_id",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    logger.info("Processed dim_employee at %s", path_str)
    return spark.read.format("delta").load(path_str)
