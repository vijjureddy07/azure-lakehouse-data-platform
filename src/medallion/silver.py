"""
Silver Medallion Transformation & Conformance Layer.

Transforms raw Bronze Delta tables into conformed, strongly-typed, validated,
and deduplicated Silver Delta tables while routing defective records to Silver Quarantine.

Key Engineering Rules:
- Explicit type casting (DateType, TimestampType, DecimalType financial precision).
- String trimming, casing standardization, and email validation regex.
- Duplicate detection via Window ROW_NUMBER() routing duplicate occurrences to quarantine.
- Referential integrity validation via anti-joins against validated upstream dimension tables.
- Quarantine preservation: Defective rows are isolated with reason codes, never silently dropped.
- Mathematical Reconciliation: bronze_count == silver_valid_count + quarantine_count.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    lower,
    row_number,
    to_date,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

EMAIL_REGEX = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
STATUSES_ORDER = ["COMPLETED", "PENDING", "PROCESSING", "CANCELLED", "REFUNDED"]
STATUSES_PAYMENT = ["SUCCESS", "FAILED", "PENDING", "REFUNDED"]


def transform_silver_customers(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Transform Bronze customers into Silver Valid and Silver Quarantine."""
    df = (
        bronze_df
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("phone", trim(col("phone")))
        .withColumn("address", trim(col("address")))
        .withColumn("city", trim(col("city")))
        .withColumn("state", trim(col("state")))
        .withColumn("postal_code", trim(col("postal_code")))
        .withColumn("country", trim(col("country")))
        .withColumn("signup_date", to_date(col("signup_date")))
        .withColumn("loyalty_tier", trim(col("loyalty_tier")))
    )

    # Window ranking for duplicate detection
    window_spec = Window.partitionBy("customer_id").orderBy(col("_ingested_timestamp").desc_nulls_last())
    ranked_df = df.withColumn("_rn", row_number().over(window_spec))

    # Defect conditions
    null_pk = col("customer_id").isNull() | (trim(col("customer_id")) == "")
    is_duplicate = col("_rn") > 1
    null_names = col("first_name").isNull() | col("last_name").isNull()
    invalid_email = col("email").isNull() | (~col("email").rlike(EMAIL_REGEX))
    null_signup = col("signup_date").isNull()

    quarantine_cond = null_pk | is_duplicate | null_names | invalid_email | null_signup

    reason_col = (
        when(null_pk, "NULL_CUSTOMER_ID")
        .when(is_duplicate, "DUPLICATE_CUSTOMER_ID")
        .when(null_names, "NULL_MANDATORY_NAME")
        .when(invalid_email, "INVALID_EMAIL_FORMAT")
        .when(null_signup, "MALFORMED_SIGNUP_DATE")
        .otherwise("UNKNOWN_DEFECT")
    )

    quarantine_df = (
        ranked_df.filter(quarantine_cond)
        .withColumn("quarantine_reason", reason_col)
        .withColumn("_quarantined_at", current_timestamp())
        .drop("_rn")
    )
    valid_df = ranked_df.filter(~quarantine_cond).drop("_rn")

    return valid_df, quarantine_df


def transform_silver_products(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Transform Bronze products into Silver Valid and Silver Quarantine."""
    df = (
        bronze_df
        .withColumn("product_sku", trim(col("product_sku")))
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("category", trim(col("category")))
        .withColumn("subcategory", trim(col("subcategory")))
        .withColumn("cost_price", col("cost_price").cast(DecimalType(10, 2)))
        .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2)))
        .withColumn("is_active", col("is_active").cast(BooleanType()))
    )

    window_spec = Window.partitionBy("product_id").orderBy(col("_ingested_timestamp").desc_nulls_last())
    ranked_df = df.withColumn("_rn", row_number().over(window_spec))

    null_pk = col("product_id").isNull() | (trim(col("product_id")) == "")
    is_duplicate = col("_rn") > 1
    null_name = col("product_name").isNull() | (trim(col("product_name")) == "")
    invalid_price = col("unit_price").isNull() | (col("unit_price") <= 0)
    invalid_cost = col("cost_price").isNull() | (col("cost_price") <= 0)

    quarantine_cond = null_pk | is_duplicate | null_name | invalid_price | invalid_cost

    reason_col = (
        when(null_pk, "NULL_PRODUCT_ID")
        .when(is_duplicate, "DUPLICATE_PRODUCT_ID")
        .when(null_name, "NULL_MANDATORY_PRODUCT_INFO")
        .when(invalid_price, "INVALID_UNIT_PRICE")
        .when(invalid_cost, "INVALID_COST_PRICE")
        .otherwise("UNKNOWN_DEFECT")
    )

    quarantine_df = (
        ranked_df.filter(quarantine_cond)
        .withColumn("quarantine_reason", reason_col)
        .withColumn("_quarantined_at", current_timestamp())
        .drop("_rn")
    )
    valid_df = ranked_df.filter(~quarantine_cond).drop("_rn")

    return valid_df, quarantine_df


def transform_silver_stores(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Transform Bronze stores into Silver Valid and Silver Quarantine."""
    df = (
        bronze_df
        .withColumn("store_name", trim(col("store_name")))
        .withColumn("store_type", trim(col("store_type")))
        .withColumn("region", trim(col("region")))
        .withColumn("state", trim(col("state")))
        .withColumn("country", trim(col("country")))
        .withColumn("opened_date", to_date(col("opened_date")))
    )

    window_spec = Window.partitionBy("store_id").orderBy(col("_ingested_timestamp").desc_nulls_last())
    ranked_df = df.withColumn("_rn", row_number().over(window_spec))

    null_pk = col("store_id").isNull() | (trim(col("store_id")) == "")
    is_duplicate = col("_rn") > 1
    null_name = col("store_name").isNull() | (trim(col("store_name")) == "")
    null_opened = col("opened_date").isNull()

    quarantine_cond = null_pk | is_duplicate | null_name | null_opened

    reason_col = (
        when(null_pk, "NULL_STORE_ID")
        .when(is_duplicate, "DUPLICATE_STORE_ID")
        .when(null_name, "NULL_STORE_NAME")
        .when(null_opened, "MALFORMED_OPENED_DATE")
        .otherwise("UNKNOWN_DEFECT")
    )

    quarantine_df = (
        ranked_df.filter(quarantine_cond)
        .withColumn("quarantine_reason", reason_col)
        .withColumn("_quarantined_at", current_timestamp())
        .drop("_rn")
    )
    valid_df = ranked_df.filter(~quarantine_cond).drop("_rn")

    return valid_df, quarantine_df


def transform_silver_employees(bronze_df: DataFrame, valid_stores_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Transform Bronze employees and validate store referential integrity."""
    df = (
        bronze_df
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("role", trim(col("role")))
        .withColumn("hire_date", to_date(col("hire_date")))
        .withColumn("is_active", col("is_active").cast(BooleanType()))
    )

    window_spec = Window.partitionBy("employee_id").orderBy(col("_ingested_timestamp").desc_nulls_last())
    ranked_df = df.withColumn("_rn", row_number().over(window_spec))

    null_pk = col("employee_id").isNull() | (trim(col("employee_id")) == "")
    is_duplicate = col("_rn") > 1
    null_name = col("first_name").isNull() | col("last_name").isNull()
    invalid_email = col("email").isNull() | (~col("email").rlike(EMAIL_REGEX))
    null_hire = col("hire_date").isNull()

    basic_quarantine_cond = null_pk | is_duplicate | null_name | invalid_email | null_hire

    base_quarantine_df = (
        ranked_df.filter(basic_quarantine_cond)
        .withColumn(
            "quarantine_reason",
            when(null_pk, "NULL_EMPLOYEE_ID")
            .when(is_duplicate, "DUPLICATE_EMPLOYEE_ID")
            .when(null_name, "NULL_MANDATORY_NAME")
            .when(invalid_email, "INVALID_EMAIL_FORMAT")
            .when(null_hire, "MALFORMED_HIRE_DATE")
            .otherwise("UNKNOWN_DEFECT"),
        )
        .withColumn("_quarantined_at", current_timestamp())
        .drop("_rn")
    )

    candidate_valid_df = ranked_df.filter(~basic_quarantine_cond).drop("_rn")

    valid_store_ids = valid_stores_df.select("store_id").distinct()
    orphan_stores_df = candidate_valid_df.join(valid_store_ids, on="store_id", how="left_anti").withColumn(
        "quarantine_reason", lit("ORPHAN_STORE_FK")
    ).withColumn("_quarantined_at", current_timestamp())

    valid_df = candidate_valid_df.join(valid_store_ids, on="store_id", how="inner")
    quarantine_df = base_quarantine_df.unionByName(orphan_stores_df, allowMissingColumns=True)

    return valid_df, quarantine_df


def transform_silver_orders(
    bronze_df: DataFrame,
    valid_customers_df: DataFrame,
    valid_stores_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Transform Bronze orders, cast datatypes, and validate customer & store referential integrity."""
    df = (
        bronze_df
        .withColumn("order_timestamp", to_timestamp(col("order_timestamp")))
        .withColumn("order_date", to_date(col("order_timestamp")))
        .withColumn("order_status", trim(col("order_status")))
        .withColumn("channel", trim(col("channel")))
        .withColumn("shipping_cost", col("shipping_cost").cast(DecimalType(10, 2)))
        .withColumn("tax_amount", col("tax_amount").cast(DecimalType(10, 2)))
        .withColumn("order_subtotal", col("order_subtotal").cast(DecimalType(12, 2)))
        .withColumn("total_amount", col("total_amount").cast(DecimalType(12, 2)))
    )

    window_spec = Window.partitionBy("order_id").orderBy(col("_ingested_timestamp").desc_nulls_last())
    ranked_df = df.withColumn("_rn", row_number().over(window_spec))

    null_pk = col("order_id").isNull() | (trim(col("order_id")) == "")
    is_duplicate = col("_rn") > 1
    null_ts = col("order_timestamp").isNull()
    invalid_status = col("order_status").isNull() | (~col("order_status").isin(STATUSES_ORDER))

    basic_quarantine_cond = null_pk | is_duplicate | null_ts | invalid_status

    base_quarantine_df = (
        ranked_df.filter(basic_quarantine_cond)
        .withColumn(
            "quarantine_reason",
            when(null_pk, "NULL_ORDER_ID")
            .when(is_duplicate, "DUPLICATE_ORDER_ID")
            .when(null_ts, "MALFORMED_ORDER_TIMESTAMP")
            .when(invalid_status, "INVALID_ORDER_STATUS")
            .otherwise("UNKNOWN_DEFECT"),
        )
        .withColumn("_quarantined_at", current_timestamp())
        .drop("_rn")
    )

    candidate_df = ranked_df.filter(~basic_quarantine_cond).drop("_rn")

    valid_cust_ids = valid_customers_df.select("customer_id").distinct()
    valid_store_ids = valid_stores_df.select("store_id").distinct()

    orphan_cust_df = candidate_df.join(valid_cust_ids, on="customer_id", how="left_anti").withColumn(
        "quarantine_reason", lit("ORPHAN_CUSTOMER_FK")
    ).withColumn("_quarantined_at", current_timestamp())

    with_valid_cust_df = candidate_df.join(valid_cust_ids, on="customer_id", how="inner")

    orphan_store_df = with_valid_cust_df.join(valid_store_ids, on="store_id", how="left_anti").withColumn(
        "quarantine_reason", lit("ORPHAN_STORE_FK")
    ).withColumn("_quarantined_at", current_timestamp())

    valid_df = with_valid_cust_df.join(valid_store_ids, on="store_id", how="inner")
    quarantine_df = (
        base_quarantine_df
        .unionByName(orphan_cust_df, allowMissingColumns=True)
        .unionByName(orphan_store_df, allowMissingColumns=True)
    )

    return valid_df, quarantine_df


def transform_silver_order_items(
    bronze_df: DataFrame,
    valid_orders_df: DataFrame,
    valid_products_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Transform Bronze order_items with financial Decimal precision and referential validation."""
    df = (
        bronze_df
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2)))
        .withColumn("discount_percent", col("discount_percent").cast(DecimalType(5, 2)))
        .withColumn(
            "discount_amount",
            (col("quantity") * col("unit_price") * (col("discount_percent") / lit(100))).cast(DecimalType(10, 2)),
        )
        .withColumn(
            "net_amount",
            ((col("quantity") * col("unit_price")) - col("discount_amount")).cast(DecimalType(10, 2)),
        )
    )

    window_spec = Window.partitionBy("order_item_id").orderBy(col("_ingested_timestamp").desc_nulls_last())
    ranked_df = df.withColumn("_rn", row_number().over(window_spec))

    null_pk = col("order_item_id").isNull() | (trim(col("order_item_id")) == "")
    is_duplicate = col("_rn") > 1
    invalid_qty = col("quantity").isNull() | (col("quantity") <= 0)
    invalid_price = col("unit_price").isNull() | (col("unit_price") <= 0)

    basic_quarantine_cond = null_pk | is_duplicate | invalid_qty | invalid_price

    base_quarantine_df = (
        ranked_df.filter(basic_quarantine_cond)
        .withColumn(
            "quarantine_reason",
            when(null_pk, "NULL_ORDER_ITEM_ID")
            .when(is_duplicate, "DUPLICATE_ORDER_ITEM_ID")
            .when(invalid_qty, "INVALID_QUANTITY")
            .when(invalid_price, "INVALID_UNIT_PRICE")
            .otherwise("UNKNOWN_DEFECT"),
        )
        .withColumn("_quarantined_at", current_timestamp())
        .drop("_rn")
    )

    candidate_df = ranked_df.filter(~basic_quarantine_cond).drop("_rn")

    valid_order_ids = valid_orders_df.select("order_id").distinct()
    valid_prod_ids = valid_products_df.select("product_id").distinct()

    orphan_order_df = candidate_df.join(valid_order_ids, on="order_id", how="left_anti").withColumn(
        "quarantine_reason", lit("ORPHAN_ORDER_FK")
    ).withColumn("_quarantined_at", current_timestamp())

    with_valid_order_df = candidate_df.join(valid_order_ids, on="order_id", how="inner")

    orphan_prod_df = with_valid_order_df.join(valid_prod_ids, on="product_id", how="left_anti").withColumn(
        "quarantine_reason", lit("ORPHAN_PRODUCT_FK")
    ).withColumn("_quarantined_at", current_timestamp())

    valid_df = with_valid_order_df.join(valid_prod_ids, on="product_id", how="inner")
    quarantine_df = (
        base_quarantine_df
        .unionByName(orphan_order_df, allowMissingColumns=True)
        .unionByName(orphan_prod_df, allowMissingColumns=True)
    )

    return valid_df, quarantine_df


def transform_silver_payments(bronze_df: DataFrame, valid_orders_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Transform Bronze payments with Decimal precision and order referential validation."""
    df = (
        bronze_df
        .withColumn("payment_method", trim(col("payment_method")))
        .withColumn("payment_status", trim(col("payment_status")))
        .withColumn("payment_amount", col("payment_amount").cast(DecimalType(10, 2)))
        .withColumn("payment_timestamp", to_timestamp(col("payment_timestamp")))
    )

    window_spec = Window.partitionBy("payment_id").orderBy(col("_ingested_timestamp").desc_nulls_last())
    ranked_df = df.withColumn("_rn", row_number().over(window_spec))

    null_pk = col("payment_id").isNull() | (trim(col("payment_id")) == "")
    is_duplicate = col("_rn") > 1
    invalid_status = col("payment_status").isNull() | (~col("payment_status").isin(STATUSES_PAYMENT))
    invalid_amount = col("payment_amount").isNull() | (col("payment_amount") <= 0)
    null_ts = col("payment_timestamp").isNull()

    basic_quarantine_cond = null_pk | is_duplicate | invalid_status | invalid_amount | null_ts

    base_quarantine_df = (
        ranked_df.filter(basic_quarantine_cond)
        .withColumn(
            "quarantine_reason",
            when(null_pk, "NULL_PAYMENT_ID")
            .when(is_duplicate, "DUPLICATE_PAYMENT_ID")
            .when(invalid_status, "INVALID_PAYMENT_STATUS")
            .when(invalid_amount, "INVALID_PAYMENT_AMOUNT")
            .when(null_ts, "MALFORMED_PAYMENT_TIMESTAMP")
            .otherwise("UNKNOWN_DEFECT"),
        )
        .withColumn("_quarantined_at", current_timestamp())
        .drop("_rn")
    )

    candidate_df = ranked_df.filter(~basic_quarantine_cond).drop("_rn")
    valid_order_ids = valid_orders_df.select("order_id").distinct()

    orphan_order_df = candidate_df.join(valid_order_ids, on="order_id", how="left_anti").withColumn(
        "quarantine_reason", lit("ORPHAN_ORDER_FK")
    ).withColumn("_quarantined_at", current_timestamp())

    valid_df = candidate_df.join(valid_order_ids, on="order_id", how="inner")
    quarantine_df = base_quarantine_df.unionByName(orphan_order_df, allowMissingColumns=True)

    return valid_df, quarantine_df


def transform_silver_returns(bronze_df: DataFrame, valid_order_items_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Transform Bronze returns with Decimal precision and order_item referential validation."""
    df = (
        bronze_df
        .withColumn("return_reason", trim(col("return_reason")))
        .withColumn("return_status", trim(col("return_status")))
        .withColumn("refund_amount", col("refund_amount").cast(DecimalType(10, 2)))
        .withColumn("return_timestamp", to_timestamp(col("return_timestamp")))
    )

    window_spec = Window.partitionBy("return_id").orderBy(col("_ingested_timestamp").desc_nulls_last())
    ranked_df = df.withColumn("_rn", row_number().over(window_spec))

    null_pk = col("return_id").isNull() | (trim(col("return_id")) == "")
    is_duplicate = col("_rn") > 1
    invalid_amount = col("refund_amount").isNull() | (col("refund_amount") < 0)
    null_ts = col("return_timestamp").isNull()

    basic_quarantine_cond = null_pk | is_duplicate | invalid_amount | null_ts

    base_quarantine_df = (
        ranked_df.filter(basic_quarantine_cond)
        .withColumn(
            "quarantine_reason",
            when(null_pk, "NULL_RETURN_ID")
            .when(is_duplicate, "DUPLICATE_RETURN_ID")
            .when(invalid_amount, "INVALID_REFUND_AMOUNT")
            .when(null_ts, "MALFORMED_RETURN_TIMESTAMP")
            .otherwise("UNKNOWN_DEFECT"),
        )
        .withColumn("_quarantined_at", current_timestamp())
        .drop("_rn")
    )

    candidate_df = ranked_df.filter(~basic_quarantine_cond).drop("_rn")
    valid_item_ids = valid_order_items_df.select("order_item_id").distinct()

    orphan_item_df = candidate_df.join(valid_item_ids, on="order_item_id", how="left_anti").withColumn(
        "quarantine_reason", lit("ORPHAN_ORDER_ITEM_FK")
    ).withColumn("_quarantined_at", current_timestamp())

    valid_df = candidate_df.join(valid_item_ids, on="order_item_id", how="inner")
    quarantine_df = base_quarantine_df.unionByName(orphan_item_df, allowMissingColumns=True)

    return valid_df, quarantine_df


def process_silver_layer(
    spark: SparkSession,
    bronze_root: Path,
    silver_root: Path,
    quarantine_root: Path,
) -> dict[str, dict[str, int]]:
    """
    Transform all Bronze Delta tables into Silver Valid Delta tables and Silver Quarantine.
    """
    metrics: dict[str, dict[str, int]] = {}

    def load_b(ds: str) -> DataFrame:
        return spark.read.format("delta").load(str(bronze_root / ds))

    # 1. Independent Dimensions: customers, products, stores
    b_cust = load_b("customers")
    v_cust, q_cust = transform_silver_customers(b_cust)
    v_cust.write.format("delta").mode("overwrite").save(str(silver_root / "customers"))
    q_cust.write.format("delta").mode("overwrite").save(str(quarantine_root / "customers"))
    metrics["customers"] = {
        "bronze": b_cust.count(),
        "silver_valid": v_cust.count(),
        "quarantine": q_cust.count(),
    }

    b_prod = load_b("products")
    v_prod, q_prod = transform_silver_products(b_prod)
    v_prod.write.format("delta").mode("overwrite").save(str(silver_root / "products"))
    q_prod.write.format("delta").mode("overwrite").save(str(quarantine_root / "products"))
    metrics["products"] = {
        "bronze": b_prod.count(),
        "silver_valid": v_prod.count(),
        "quarantine": q_prod.count(),
    }

    b_stores = load_b("stores")
    v_stores, q_stores = transform_silver_stores(b_stores)
    v_stores.write.format("delta").mode("overwrite").save(str(silver_root / "stores"))
    q_stores.write.format("delta").mode("overwrite").save(str(quarantine_root / "stores"))
    metrics["stores"] = {
        "bronze": b_stores.count(),
        "silver_valid": v_stores.count(),
        "quarantine": q_stores.count(),
    }

    # 2. Dependent Dimension: employees
    b_emp = load_b("employees")
    v_emp, q_emp = transform_silver_employees(b_emp, v_stores)
    v_emp.write.format("delta").mode("overwrite").save(str(silver_root / "employees"))
    q_emp.write.format("delta").mode("overwrite").save(str(quarantine_root / "employees"))
    metrics["employees"] = {
        "bronze": b_emp.count(),
        "silver_valid": v_emp.count(),
        "quarantine": q_emp.count(),
    }

    # 3. Core Transactions: orders
    b_ord = load_b("orders")
    v_ord, q_ord = transform_silver_orders(b_ord, v_cust, v_stores)
    v_ord.write.format("delta").mode("overwrite").save(str(silver_root / "orders"))
    q_ord.write.format("delta").mode("overwrite").save(str(quarantine_root / "orders"))
    metrics["orders"] = {
        "bronze": b_ord.count(),
        "silver_valid": v_ord.count(),
        "quarantine": q_ord.count(),
    }

    # 4. Transaction Items: order_items
    b_items = load_b("order_items")
    v_items, q_items = transform_silver_order_items(b_items, v_ord, v_prod)
    v_items.write.format("delta").mode("overwrite").save(str(silver_root / "order_items"))
    q_items.write.format("delta").mode("overwrite").save(str(quarantine_root / "order_items"))
    metrics["order_items"] = {
        "bronze": b_items.count(),
        "silver_valid": v_items.count(),
        "quarantine": q_items.count(),
    }

    # 5. Financials: payments
    b_pay = load_b("payments")
    v_pay, q_pay = transform_silver_payments(b_pay, v_ord)
    v_pay.write.format("delta").mode("overwrite").save(str(silver_root / "payments"))
    q_pay.write.format("delta").mode("overwrite").save(str(quarantine_root / "payments"))
    metrics["payments"] = {
        "bronze": b_pay.count(),
        "silver_valid": v_pay.count(),
        "quarantine": q_pay.count(),
    }

    # 6. Post-Transaction: returns
    b_ret = load_b("returns")
    v_ret, q_ret = transform_silver_returns(b_ret, v_items)
    v_ret.write.format("delta").mode("overwrite").save(str(silver_root / "returns"))
    q_ret.write.format("delta").mode("overwrite").save(str(quarantine_root / "returns"))
    metrics["returns"] = {
        "bronze": b_ret.count(),
        "silver_valid": v_ret.count(),
        "quarantine": q_ret.count(),
    }

    logger.info("Silver transformation complete across all 8 datasets.")
    return metrics
