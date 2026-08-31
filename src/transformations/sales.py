"""
Business Transformations, Analytical Enriched Curation, and Window Function Engine.

Curates an analytical omnichannel sales dataset combining:
- orders
- order_items
- products
- customers
- stores

Key Features:
- Exact DecimalType arithmetic for line-level financial derivations (gross, discount, net, profit)
- Temporal partitions (order_year, order_month) for optimized columnar Parquet layout
- Advanced Window Functions:
  1. Customer Order Sequence: ROW_NUMBER() per customer
  2. Customer Cumulative Spend: Running SUM(net_sales) per customer
  3. Days Since Previous Order: LAG(order_date) per customer
  4. Product Category Revenue Ranking: DENSE_RANK() per category
"""

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

logger = logging.getLogger(__name__)


def build_curated_sales(
    clean_orders_df: DataFrame,
    clean_order_items_df: DataFrame,
    clean_products_df: DataFrame,
    clean_customers_df: DataFrame,
    clean_stores_df: DataFrame,
) -> DataFrame:
    """
    Combine cleaned dimensional and transactional tables into an enriched, curated sales dataset.
    """
    logger.info("Building curated sales dataset...")

    # 1. Join order_items with orders (Inner join: we only analyze fulfilled items for validated orders)
    items_orders = clean_order_items_df.join(
        clean_orders_df.select(
            "order_id",
            "customer_id",
            "store_id",
            "employee_id",
            "order_timestamp",
            "order_date",
            "order_status",
            "channel",
        ),
        on="order_id",
        how="inner",
    )

    # 2. Join with Products (Inner join: items have validated product references)
    with_products = items_orders.join(
        clean_products_df.select(
            F.col("product_id"),
            F.col("product_sku"),
            F.col("product_name"),
            F.col("category"),
            F.col("subcategory"),
            F.col("cost_price"),
        ),
        on="product_id",
        how="inner",
    )

    # 3. Join with Customers (Inner join)
    with_customers = with_products.join(
        clean_customers_df.select(
            F.col("customer_id"),
            F.col("full_name").alias("customer_name"),
            F.col("email").alias("customer_email"),
            F.col("state").alias("customer_state"),
            F.col("loyalty_tier"),
        ),
        on="customer_id",
        how="inner",
    )

    # 4. Join with Stores (Inner join)
    with_stores = with_customers.join(
        clean_stores_df.select(
            F.col("store_id"),
            F.col("store_name"),
            F.col("store_type"),
            F.col("region").alias("store_region"),
            F.col("state").alias("store_state"),
        ),
        on="store_id",
        how="inner",
    )

    # 5. Financial metric derivations with exact Decimal precision
    # gross_sales = quantity * unit_price
    # discount_amount = gross_sales * discount_percent
    # net_sales = gross_sales - discount_amount
    financials_df = (
        with_stores.withColumn(
            "gross_sales",
            (F.col("quantity").cast(DecimalType(12, 2)) * F.col("unit_price")).cast(DecimalType(12, 2)),
        )
        .withColumn(
            "discount_amount",
            (F.col("gross_sales") * F.col("discount_percent")).cast(DecimalType(12, 2)),
        )
        .withColumn(
            "net_sales",
            (F.col("gross_sales") - F.col("discount_amount")).cast(DecimalType(12, 2)),
        )
        .withColumn(
            "cost_amount",
            (F.col("quantity").cast(DecimalType(12, 2)) * F.col("cost_price")).cast(DecimalType(12, 2)),
        )
        .withColumn(
            "gross_profit",
            (F.col("net_sales") - F.col("cost_amount")).cast(DecimalType(12, 2)),
        )
        .withColumn("order_year", F.year(F.col("order_date")).cast(IntegerType()))
        .withColumn("order_month", F.month(F.col("order_date")).cast(IntegerType()))
        .withColumn("order_day", F.dayofmonth(F.col("order_date")).cast(IntegerType()))
    )

    # 6. Order-Grain Window Functions
    # Calculate customer order sequence, purchase intervals, and cumulative spend at UNIQUE ORDER GRAIN
    # to avoid incorrect duplicate sequencing or partial intra-order spend accumulation on line items.
    order_grain_df = financials_df.groupBy(
        "customer_id", "order_id", "order_timestamp", "order_date"
    ).agg(
        F.sum("net_sales").alias("order_net_sales")
    )

    order_window = Window.partitionBy("customer_id").orderBy(
        F.col("order_timestamp").asc(),
        F.col("order_id").asc(),
    )
    order_running_window = (
        Window.partitionBy("customer_id")
        .orderBy(F.col("order_timestamp").asc(), F.col("order_id").asc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    order_metrics_df = (
        order_grain_df.withColumn("customer_order_sequence", F.row_number().over(order_window))
        .withColumn("prev_order_date", F.lag("order_date", 1).over(order_window))
        .withColumn("days_since_prior_order", F.datediff(F.col("order_date"), F.col("prev_order_date")))
        .withColumn(
            "customer_running_spend",
            F.sum("order_net_sales").over(order_running_window).cast(DecimalType(14, 2)),
        )
        .select(
            "customer_id",
            "order_id",
            "customer_order_sequence",
            "days_since_prior_order",
            "customer_running_spend",
        )
    )

    # 7. Category Product Ranking Window (DENSE_RANK per category)
    product_totals = financials_df.groupBy("category", "product_id").agg(
        F.sum("net_sales").alias("total_prod_cat_sales")
    )
    cat_rank_window = Window.partitionBy("category").orderBy(F.col("total_prod_cat_sales").desc())
    ranked_products = product_totals.withColumn("category_product_rank", F.dense_rank().over(cat_rank_window))

    # 8. Assemble Curated Dataset: Join order metrics and product rankings back onto line items
    with_order_metrics = financials_df.join(
        order_metrics_df,
        on=["customer_id", "order_id"],
        how="inner",
    )

    curated_df = with_order_metrics.join(
        ranked_products.select("category", "product_id", "category_product_rank"),
        on=["category", "product_id"],
        how="left",
    )

    logger.info("Curated sales dataset created successfully with %d line items.", curated_df.count())
    return curated_df
