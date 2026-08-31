"""
Gold Medallion Analytical Aggregation Layer.

Builds business-ready, high-performance Delta aggregate tables derived strictly from
conformed Silver Delta tables (never directly from raw landing files).

Gold Analytical Tables:
1. gold_daily_sales_performance: Daily sales, discounts, gross/net revenue, profit, and returns.
2. gold_monthly_revenue: Monthly revenue cadence, total orders, units sold, and refunds.
3. gold_revenue_by_store_region: Store & regional revenue contribution and average order value.
4. gold_category_revenue_performance: Product category performance, sales volume, and return rates.
5. gold_customer_spending_summary: Customer lifetime value, order frequency, and recency.
6. gold_return_refund_performance: Return reason distribution, frequency, and financial impact.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    countDistinct,
    lit,
    max,
    min,
    month,
    round,
    sum,
    when,
    year,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def build_gold_daily_sales(
    orders_df: DataFrame,
    items_df: DataFrame,
    products_df: DataFrame,
    returns_df: DataFrame,
) -> DataFrame:
    """Aggregate daily sales performance metrics."""
    item_sales = (
        items_df
        .join(orders_df.select("order_id", "order_date", "order_status"), on="order_id", how="inner")
        .join(products_df.select("product_id", "cost_price"), on="product_id", how="inner")
        .withColumn("cost_amount", col("quantity") * col("cost_price"))
    )

    daily_sales = (
        item_sales
        .groupBy("order_date")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("quantity").alias("total_units_sold"),
            sum(col("quantity") * col("unit_price")).alias("gross_revenue"),
            sum("discount_amount").alias("total_discounts"),
            sum("net_amount").alias("net_sales"),
            sum("cost_amount").alias("total_cogs"),
        )
        .withColumn("gross_profit", col("net_sales") - col("total_cogs"))
    )

    returns_with_date = (
        returns_df
        .join(items_df.select("order_item_id", "order_id"), on="order_item_id", how="inner")
        .join(orders_df.select("order_id", "order_date"), on="order_id", how="inner")
        .groupBy("order_date")
        .agg(
            count("return_id").alias("returns_count"),
            sum("refund_amount").alias("total_refunded_amount"),
        )
    )

    result_df = (
        daily_sales
        .join(returns_with_date, on="order_date", how="left")
        .withColumn("returns_count", coalesce(col("returns_count"), lit(0)))
        .withColumn("total_refunded_amount", coalesce(col("total_refunded_amount"), lit(0.0)))
        .orderBy("order_date")
    )
    return result_df


def build_gold_monthly_revenue(
    orders_df: DataFrame,
    items_df: DataFrame,
    returns_df: DataFrame,
) -> DataFrame:
    """Aggregate monthly revenue performance."""
    item_orders = items_df.join(orders_df.select("order_id", "order_date"), on="order_id", how="inner")

    monthly_sales = (
        item_orders
        .withColumn("order_year", year(col("order_date")))
        .withColumn("order_month", month(col("order_date")))
        .groupBy("order_year", "order_month")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("quantity").alias("total_units_sold"),
            sum("net_amount").alias("total_net_revenue"),
        )
    )

    returns_with_month = (
        returns_df
        .join(items_df.select("order_item_id", "order_id"), on="order_item_id", how="inner")
        .join(orders_df.select("order_id", "order_date"), on="order_id", how="inner")
        .withColumn("return_year", year(col("order_date")))
        .withColumn("return_month", month(col("order_date")))
        .groupBy("return_year", "return_month")
        .agg(sum("refund_amount").alias("total_refunded_amount"))
    )

    result_df = (
        monthly_sales
        .join(
            returns_with_month,
            (monthly_sales.order_year == returns_with_month.return_year)
            & (monthly_sales.order_month == returns_with_month.return_month),
            how="left",
        )
        .select(
            col("order_year").alias("year"),
            col("order_month").alias("month"),
            col("total_orders"),
            col("total_units_sold"),
            col("total_net_revenue"),
            coalesce(col("total_refunded_amount"), lit(0.0)).alias("total_refunded_amount"),
        )
        .orderBy("year", "month")
    )
    return result_df


def build_gold_revenue_by_store_region(
    orders_df: DataFrame,
    items_df: DataFrame,
    stores_df: DataFrame,
) -> DataFrame:
    """Aggregate store and regional sales performance."""
    store_sales = (
        items_df
        .join(orders_df.select("order_id", "store_id"), on="order_id", how="inner")
        .groupBy("store_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("net_amount").alias("total_net_revenue"),
        )
    )

    result_df = (
        stores_df
        .join(store_sales, on="store_id", how="left")
        .select(
            col("store_id"),
            col("store_name"),
            col("store_type"),
            col("region"),
            col("state"),
            col("country"),
            coalesce(col("total_orders"), lit(0)).alias("total_orders"),
            coalesce(col("total_net_revenue"), lit(0.0)).alias("total_net_revenue"),
        )
        .withColumn(
            "avg_order_value",
            when(col("total_orders") > 0, round(col("total_net_revenue") / col("total_orders"), 2)).otherwise(lit(0.0)),
        )
        .orderBy(col("total_net_revenue").desc())
    )
    return result_df


def build_gold_category_performance(
    items_df: DataFrame,
    products_df: DataFrame,
    returns_df: DataFrame,
) -> DataFrame:
    """Aggregate product category and subcategory sales and return rates."""
    cat_sales = (
        items_df
        .join(products_df.select("product_id", "category", "subcategory"), on="product_id", how="inner")
        .groupBy("category", "subcategory")
        .agg(
            sum("quantity").alias("units_sold"),
            sum(col("quantity") * col("unit_price")).alias("gross_revenue"),
            sum("discount_amount").alias("total_discounts"),
            sum("net_amount").alias("net_revenue"),
        )
    )

    cat_returns = (
        returns_df
        .join(items_df.select("order_item_id", "product_id"), on="order_item_id", how="inner")
        .join(products_df.select("product_id", "category", "subcategory"), on="product_id", how="inner")
        .groupBy("category", "subcategory")
        .agg(
            count("return_id").alias("units_returned"),
            sum("refund_amount").alias("total_refunded_amount"),
        )
    )

    result_df = (
        cat_sales
        .join(cat_returns, on=["category", "subcategory"], how="left")
        .withColumn("units_returned", coalesce(col("units_returned"), lit(0)))
        .withColumn("total_refunded_amount", coalesce(col("total_refunded_amount"), lit(0.0)))
        .withColumn(
            "return_rate_pct",
            when(col("units_sold") > 0, round((col("units_returned") / col("units_sold")) * 100, 2)).otherwise(lit(0.0)),
        )
        .orderBy(col("net_revenue").desc())
    )
    return result_df


def build_gold_customer_spending(
    customers_df: DataFrame,
    orders_df: DataFrame,
    items_df: DataFrame,
) -> DataFrame:
    """Aggregate customer lifetime spending, order counts, and recency."""
    cust_orders = (
        items_df
        .join(orders_df.select("order_id", "customer_id", "order_date"), on="order_id", how="inner")
        .groupBy("customer_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("net_amount").alias("lifetime_spend"),
            min("order_date").alias("first_order_date"),
            max("order_date").alias("latest_order_date"),
        )
    )

    result_df = (
        customers_df
        .join(cust_orders, on="customer_id", how="left")
        .select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("email"),
            col("loyalty_tier"),
            coalesce(col("total_orders"), lit(0)).alias("total_orders"),
            coalesce(col("lifetime_spend"), lit(0.0)).alias("lifetime_spend"),
            col("first_order_date"),
            col("latest_order_date"),
        )
        .withColumn(
            "avg_order_value",
            when(col("total_orders") > 0, round(col("lifetime_spend") / col("total_orders"), 2)).otherwise(lit(0.0)),
        )
        .orderBy(col("lifetime_spend").desc())
    )
    return result_df


def build_gold_return_performance(returns_df: DataFrame) -> DataFrame:
    """Aggregate return reason frequency and financial impact."""
    result_df = (
        returns_df
        .groupBy("return_reason")
        .agg(
            count("return_id").alias("return_count"),
            sum("refund_amount").alias("total_refund_amount"),
            avg("refund_amount").alias("avg_refund_amount"),
        )
        .withColumn("avg_refund_amount", round(col("avg_refund_amount"), 2))
        .orderBy(col("return_count").desc())
    )
    return result_df


def process_gold_layer(
    spark: SparkSession,
    silver_root: Path,
    gold_root: Path,
) -> dict[str, int]:
    """
    Build and persist all Gold Delta aggregate tables from Silver Delta tables.
    """
    def load_s(table: str) -> DataFrame:
        return spark.read.format("delta").load(str(silver_root / table))

    customers = load_s("customers")
    products = load_s("products")
    stores = load_s("stores")
    orders = load_s("orders")
    items = load_s("order_items")
    returns = load_s("returns")

    counts = {}

    # 1. Daily Sales
    df_daily = build_gold_daily_sales(orders, items, products, returns)
    df_daily.write.format("delta").mode("overwrite").save(str(gold_root / "gold_daily_sales_performance"))
    counts["gold_daily_sales_performance"] = df_daily.count()

    # 2. Monthly Revenue
    df_monthly = build_gold_monthly_revenue(orders, items, returns)
    df_monthly.write.format("delta").mode("overwrite").save(str(gold_root / "gold_monthly_revenue"))
    counts["gold_monthly_revenue"] = df_monthly.count()

    # 3. Store Region Revenue
    df_store = build_gold_revenue_by_store_region(orders, items, stores)
    df_store.write.format("delta").mode("overwrite").save(str(gold_root / "gold_revenue_by_store_region"))
    counts["gold_revenue_by_store_region"] = df_store.count()

    # 4. Category Performance
    df_cat = build_gold_category_performance(items, products, returns)
    df_cat.write.format("delta").mode("overwrite").save(str(gold_root / "gold_category_revenue_performance"))
    counts["gold_category_revenue_performance"] = df_cat.count()

    # 5. Customer Spending Summary
    df_cust_spend = build_gold_customer_spending(customers, orders, items)
    df_cust_spend.write.format("delta").mode("overwrite").save(str(gold_root / "gold_customer_spending_summary"))
    counts["gold_customer_spending_summary"] = df_cust_spend.count()

    # 6. Return Refund Performance
    df_ret_perf = build_gold_return_performance(returns)
    df_ret_perf.write.format("delta").mode("overwrite").save(str(gold_root / "gold_return_refund_performance"))
    counts["gold_return_refund_performance"] = df_ret_perf.count()

    logger.info("Gold analytical aggregations successfully persisted to %s", gold_root)
    return counts
