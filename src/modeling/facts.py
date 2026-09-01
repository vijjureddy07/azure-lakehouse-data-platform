"""
Warehouse Fact Table Processors.

Builds enterprise-grade Kimball fact tables:
1. fact_sales:
   - Grain: ONE ROW PER VALID SILVER ORDER ITEM.
   - Point-in-Time SCD2 Lookup: Resolves the exact historical customer_key valid at the order_timestamp.
   - Surrogate Key Resolution: Joins product_key, store_key, order_date_key (with unknown key 0 fallback).
   - Decimal Monetary Measures: gross_amount, discount_amount, net_amount, cost_amount, profit_amount.
   - Idempotency: Delta MERGE matching on order_item_id guarantees zero duplicate facts on reruns.

2. fact_returns:
   - Grain: ONE ROW PER VALID RETURN EVENT.
   - Measures: refund_amount with dimensional surrogate linkages.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from delta.tables import DeltaTable
from pyspark.sql.functions import (
    coalesce,
    col,
    lit,
    to_date,
)
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def build_fact_sales_dataframe(
    silver_order_items_df: DataFrame,
    silver_orders_df: DataFrame,
    dim_customer_df: DataFrame,
    dim_product_df: DataFrame,
    dim_store_df: DataFrame,
    dim_date_df: DataFrame,
) -> DataFrame:
    """
    Assemble fact_sales DataFrame with Point-in-Time SCD2 lookups and Decimal measures.

    Grain: ONE ROW PER VALID ORDER ITEM.
    """
    # 1. Join Silver order items with Silver orders to inherit order_timestamp and store_id
    items_orders_df = (
        silver_order_items_df.alias("item")
        .join(
            silver_orders_df.alias("ord"),
            on="order_id",
            how="inner",
        )
        .select(
            col("item.order_item_id"),
            col("item.order_id"),
            col("item.product_id"),
            col("ord.customer_id"),
            col("ord.store_id"),
            col("ord.order_timestamp"),
            col("ord.order_date"),
            col("ord.channel"),
            col("ord.order_status"),
            col("item.quantity"),
            col("item.unit_price"),
            col("item.discount_percent"),
            col("item.discount_amount"),
            col("item.net_amount"),
        )
    )

    # 2. Point-in-Time Historical Lookup for Customer SCD Type 2
    # Condition: order_timestamp in [effective_from, effective_to)
    cust_pit_condition = (
        (col("io.customer_id") == col("dc.customer_id"))
        & (col("io.order_timestamp") >= col("dc.effective_from"))
        & (
            (col("io.order_timestamp") < col("dc.effective_to"))
            | col("dc.effective_to").isNull()
        )
    )

    with_cust = items_orders_df.alias("io").join(
        dim_customer_df.alias("dc"),
        on=cust_pit_condition,
        how="left",
    ).select(
        col("io.*"),
        coalesce(col("dc.customer_key"), lit(0)).cast(IntegerType()).alias("customer_key"),
    )

    # 3. Product Dimension Lookup (SCD Type 1)
    with_prod = with_cust.alias("wc").join(
        dim_product_df.alias("dp"),
        col("wc.product_id") == col("dp.product_id"),
        how="left",
    ).select(
        col("wc.*"),
        coalesce(col("dp.product_key"), lit(0)).cast(IntegerType()).alias("product_key"),
        coalesce(col("dp.cost_price"), lit(0.00)).cast(DecimalType(10, 2)).alias("prod_cost_price"),
    )

    # 4. Store Dimension Lookup
    with_store = with_prod.alias("wp").join(
        dim_store_df.alias("ds"),
        col("wp.store_id") == col("ds.store_id"),
        how="left",
    ).select(
        col("wp.*"),
        coalesce(col("ds.store_key"), lit(0)).cast(IntegerType()).alias("store_key"),
    )

    # 5. Date Dimension Lookup
    date_lookup = dim_date_df.select(
        col("date_key").alias("dim_dt_key"),
        col("full_date").alias("dim_full_date"),
    )
    with_date = with_store.join(
        date_lookup,
        with_store.order_date == date_lookup.dim_full_date,
        how="left",
    ).withColumn(
        "order_date_key",
        coalesce(col("dim_dt_key"), lit(0)).cast(IntegerType()),
    ).drop("dim_dt_key", "dim_full_date")

    # 6. Derive Exact Decimal Financial Measures
    # gross_amount = quantity * unit_price
    # net_amount = gross_amount - discount_amount
    # cost_amount = quantity * prod_cost_price
    # profit_amount = net_amount - cost_amount
    fact_sales_df = (
        with_date
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2)))
        .withColumn(
            "gross_amount",
            (col("quantity") * col("unit_price")).cast(DecimalType(10, 2)),
        )
        .withColumn("discount_amount", col("discount_amount").cast(DecimalType(10, 2)))
        .withColumn("net_amount", col("net_amount").cast(DecimalType(10, 2)))
        .withColumn(
            "cost_amount",
            (col("quantity") * col("prod_cost_price")).cast(DecimalType(10, 2)),
        )
        .withColumn(
            "profit_amount",
            (col("net_amount") - (col("quantity") * col("prod_cost_price"))).cast(DecimalType(10, 2)),
        )
        .select(
            "order_item_id",
            "order_id",
            "customer_key",
            "product_key",
            "store_key",
            "order_date_key",
            "customer_id",
            "product_id",
            "store_id",
            "order_timestamp",
            "order_status",
            "channel",
            "quantity",
            "unit_price",
            "gross_amount",
            "discount_amount",
            "net_amount",
            "cost_amount",
            "profit_amount",
        )
    )

    return fact_sales_df


def process_fact_sales(
    spark: SparkSession,
    silver_order_items_df: DataFrame,
    silver_orders_df: DataFrame,
    dim_customer_df: DataFrame,
    dim_product_df: DataFrame,
    dim_store_df: DataFrame,
    dim_date_df: DataFrame,
    fact_sales_path: Path | str,
) -> DataFrame:
    """
    Build and persist fact_sales Delta table with idempotent Delta MERGE on order_item_id.
    """
    path_str = str(fact_sales_path)
    fact_df = build_fact_sales_dataframe(
        silver_order_items_df=silver_order_items_df,
        silver_orders_df=silver_orders_df,
        dim_customer_df=dim_customer_df,
        dim_product_df=dim_product_df,
        dim_store_df=dim_store_df,
        dim_date_df=dim_date_df,
    )

    if not DeltaTable.isDeltaTable(spark, path_str):
        fact_df.write.format("delta").mode("overwrite").save(path_str)
        logger.info("Initialized fact_sales table at %s with %d rows", path_str, fact_df.count())
    else:
        delta_tbl = DeltaTable.forPath(spark, path_str)
        delta_tbl.alias("target").merge(
            fact_df.alias("source"),
            "target.order_item_id = source.order_item_id",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info("Merged fact_sales at %s", path_str)

    return spark.read.format("delta").load(path_str)


def process_fact_returns(
    spark: SparkSession,
    silver_returns_df: DataFrame,
    fact_sales_df: DataFrame,
    dim_date_df: DataFrame,
    fact_returns_path: Path | str,
) -> DataFrame:
    """
    Build and persist fact_returns Delta table.

    Grain: ONE ROW PER VALID RETURN EVENT.
    """
    path_str = str(fact_returns_path)

    sales_lookup = fact_sales_df.select(
        "order_item_id",
        "customer_key",
        "product_key",
        "store_key",
        "order_id",
    )

    date_lookup = dim_date_df.select(
        col("date_key").alias("dim_ret_dt_key"),
        col("full_date").alias("dim_ret_full_date"),
    )

    fact_returns_df = (
        silver_returns_df.alias("ret")
        .join(sales_lookup.alias("sls"), on="order_item_id", how="left")
        .join(
            date_lookup,
            to_date(col("ret.return_timestamp")) == date_lookup.dim_ret_full_date,
            how="left",
        )
        .withColumn(
            "return_date_key",
            coalesce(col("dim_ret_dt_key"), lit(0)).cast(IntegerType()),
        )
        .withColumn("refund_amount", col("refund_amount").cast(DecimalType(10, 2)))
        .select(
            col("ret.return_id"),
            col("ret.order_item_id"),
            coalesce(col("sls.order_id"), lit("UNKNOWN")).alias("order_id"),
            coalesce(col("sls.customer_key"), lit(0)).cast(IntegerType()).alias("customer_key"),
            coalesce(col("sls.product_key"), lit(0)).cast(IntegerType()).alias("product_key"),
            coalesce(col("sls.store_key"), lit(0)).cast(IntegerType()).alias("store_key"),
            col("return_date_key"),
            col("ret.return_timestamp"),
            col("ret.return_reason"),
            col("ret.return_status"),
            col("refund_amount"),
        )
    )

    if not DeltaTable.isDeltaTable(spark, path_str):
        fact_returns_df.write.format("delta").mode("overwrite").save(path_str)
        logger.info("Initialized fact_returns table at %s with %d rows", path_str, fact_returns_df.count())
    else:
        delta_tbl = DeltaTable.forPath(spark, path_str)
        delta_tbl.alias("target").merge(
            fact_returns_df.alias("source"),
            "target.return_id = source.return_id",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info("Merged fact_returns at %s", path_str)

    return spark.read.format("delta").load(path_str)
