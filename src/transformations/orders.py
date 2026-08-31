"""
Order, Store, Employee, Order Item, Payment, and Return Transformations and Quality Validation.

Implements:
- Timestamp & Date parsing
- Monetary casting with DecimalType arithmetic
- Referential integrity verification (Foreign Key validation) via LEFT ANTI / LEFT OUTER joins
- Duplicate primary key elimination
- Status enum and business range validations
- Standardized quarantine routing and metric reconciliation
"""

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
)

from src.quality.rules import DatasetQualityMetric, format_as_quarantine

logger = logging.getLogger(__name__)

VALID_ORDER_STATUSES = ["COMPLETED", "PENDING", "CANCELLED", "REFUNDED"]
VALID_PAYMENT_STATUSES = ["SUCCESS", "FAILED", "PENDING"]


def transform_stores(raw_df: DataFrame) -> tuple[DataFrame, DataFrame, DatasetQualityMetric]:
    """Clean and validate store entities."""
    source_count = raw_df.count()
    logger.info("Transforming stores: received %d records.", source_count)

    normalized_df = (
        raw_df.withColumn("store_id", F.trim(F.col("store_id")))
        .withColumn("store_name", F.trim(F.col("store_name")))
        .withColumn("store_type", F.initcap(F.trim(F.col("store_type"))))
        .withColumn("region", F.initcap(F.trim(F.col("region"))))
        .withColumn("state", F.upper(F.trim(F.col("state"))))
        .withColumn("country", F.upper(F.trim(F.coalesce(F.col("country"), F.lit("US")))))
        .withColumn("parsed_opened_date", F.to_date(F.trim(F.col("opened_date")), "yyyy-MM-dd"))
    )

    window_spec = Window.partitionBy("store_id").orderBy(F.col("parsed_opened_date").desc_nulls_last())
    ranked_df = normalized_df.withColumn("row_num", F.row_number().over(window_spec))

    classified_df = ranked_df.withColumn(
        "rejection_reason",
        F.when(
            (F.col("store_id").isNull()) | (F.col("store_id") == ""),
            F.lit("NULL_MANDATORY_FIELD"),
        )
        .when(F.col("row_num") > 1, F.lit("DUPLICATE_STORE_ID"))
        .otherwise(F.lit(None))
    )

    clean_df = (
        classified_df.filter(F.col("rejection_reason").isNull())
        .withColumn("opened_date", F.col("parsed_opened_date"))
        .select("store_id", "store_name", "store_type", "region", "state", "country", "opened_date")
    )

    invalid_df = classified_df.filter(F.col("rejection_reason").isNotNull())
    quarantine_df = format_as_quarantine(invalid_df, "store_id", "stores", "rejection_reason")

    metric = DatasetQualityMetric(
        dataset_name="stores",
        source_row_count=source_count,
        valid_row_count=clean_df.count(),
        quarantine_row_count=quarantine_df.count(),
    )
    return clean_df, quarantine_df, metric


def transform_employees(
    raw_df: DataFrame,
    clean_stores_df: DataFrame,
) -> tuple[DataFrame, DataFrame, DatasetQualityMetric]:
    """Clean employees and validate store referential integrity."""
    source_count = raw_df.count()
    logger.info("Transforming employees: received %d records.", source_count)

    normalized_df = (
        raw_df.withColumn("employee_id", F.trim(F.col("employee_id")))
        .withColumn("store_id", F.trim(F.col("store_id")))
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("role", F.trim(F.col("role")))
        .withColumn("parsed_hire_date", F.to_date(F.trim(F.col("hire_date")), "yyyy-MM-dd"))
        .withColumn(
            "is_active_bool",
            F.when(F.lower(F.trim(F.col("is_active"))).isin("true", "t", "1"), F.lit(True)).otherwise(F.lit(False)),
        )
    )

    window_spec = Window.partitionBy("employee_id").orderBy(F.col("parsed_hire_date").desc_nulls_last())
    ranked_df = normalized_df.withColumn("row_num", F.row_number().over(window_spec))

    # Join with clean stores to check referential integrity
    joined_df = ranked_df.join(
        clean_stores_df.select(F.col("store_id").alias("ref_store_id")),
        ranked_df.store_id == F.col("ref_store_id"),
        "left",
    )

    classified_df = joined_df.withColumn(
        "rejection_reason",
        F.when(
            (F.col("employee_id").isNull()) | (F.col("employee_id") == ""),
            F.lit("NULL_MANDATORY_FIELD"),
        )
        .when(F.col("row_num") > 1, F.lit("DUPLICATE_EMPLOYEE_ID"))
        .when(F.col("ref_store_id").isNull(), F.lit("ORPHAN_STORE_FK"))
        .otherwise(F.lit(None))
    )

    clean_df = (
        classified_df.filter(F.col("rejection_reason").isNull())
        .withColumn("hire_date", F.col("parsed_hire_date"))
        .withColumn("is_active", F.col("is_active_bool"))
        .select("employee_id", "store_id", "first_name", "last_name", "email", "role", "hire_date", "is_active")
    )

    invalid_df = classified_df.filter(F.col("rejection_reason").isNotNull())
    quarantine_df = format_as_quarantine(invalid_df, "employee_id", "employees", "rejection_reason")

    metric = DatasetQualityMetric(
        dataset_name="employees",
        source_row_count=source_count,
        valid_row_count=clean_df.count(),
        quarantine_row_count=quarantine_df.count(),
        referential_orphan_count=classified_df.filter(F.col("rejection_reason") == "ORPHAN_STORE_FK").count(),
    )
    return clean_df, quarantine_df, metric


def transform_orders(
    raw_df: DataFrame,
    clean_customers_df: DataFrame,
    clean_stores_df: DataFrame,
    clean_employees_df: DataFrame,
) -> tuple[DataFrame, DataFrame, DatasetQualityMetric]:
    """
    Clean, validate, and check referential integrity for orders.
    Enforces customer, store, and employee foreign keys.
    """
    source_count = raw_df.count()
    logger.info("Transforming orders: received %d records.", source_count)

    normalized_df = (
        raw_df.withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("store_id", F.trim(F.col("store_id")))
        .withColumn("employee_id", F.trim(F.col("employee_id")))
        .withColumn("parsed_order_ts", F.to_timestamp(F.trim(F.col("order_timestamp")), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
        .withColumn("channel", F.upper(F.trim(F.col("channel"))))
        .withColumn("shipping_cost", F.col("shipping_cost").cast(DecimalType(10, 2)))
        .withColumn("tax_amount", F.col("tax_amount").cast(DecimalType(10, 2)))
        .withColumn("order_subtotal", F.col("order_subtotal").cast(DecimalType(12, 2)))
        .withColumn("total_amount", F.col("total_amount").cast(DecimalType(12, 2)))
    )

    # Window for duplicate order_id
    window_spec = Window.partitionBy("order_id").orderBy(F.col("parsed_order_ts").desc_nulls_last())
    ranked_df = normalized_df.withColumn("row_num", F.row_number().over(window_spec))

    # Referential checks
    with_cust = ranked_df.join(
        clean_customers_df.select(F.col("customer_id").alias("ref_cust_id")),
        ranked_df.customer_id == F.col("ref_cust_id"),
        "left",
    )
    with_store = with_cust.join(
        clean_stores_df.select(F.col("store_id").alias("ref_store_id")),
        with_cust.store_id == F.col("ref_store_id"),
        "left",
    )
    with_emp = with_store.join(
        clean_employees_df.select(F.col("employee_id").alias("ref_emp_id")),
        with_store.employee_id == F.col("ref_emp_id"),
        "left",
    )

    classified_df = with_emp.withColumn(
        "rejection_reason",
        F.when(
            (F.col("order_id").isNull()) | (F.col("order_id") == "") |
            (F.col("total_amount").isNull()),
            F.lit("NULL_MANDATORY_FIELD"),
        )
        .when(F.col("row_num") > 1, F.lit("DUPLICATE_ORDER_ID"))
        .when(
            F.col("parsed_order_ts").isNull() &
            F.col("order_timestamp").isNotNull() &
            (F.trim(F.col("order_timestamp")) != ""),
            F.lit("MALFORMED_ORDER_TIMESTAMP"),
        )
        .when(~F.col("order_status").isin(VALID_ORDER_STATUSES), F.lit("INVALID_ORDER_STATUS"))
        .when(F.col("ref_cust_id").isNull(), F.lit("ORPHAN_CUSTOMER_FK"))
        .when(F.col("ref_store_id").isNull(), F.lit("ORPHAN_STORE_FK"))
        .when(F.col("ref_emp_id").isNull(), F.lit("ORPHAN_EMPLOYEE_FK"))
        .otherwise(F.lit(None))
    )

    clean_df = (
        classified_df.filter(F.col("rejection_reason").isNull())
        .withColumn("order_timestamp", F.col("parsed_order_ts"))
        .withColumn("order_date", F.to_date(F.col("parsed_order_ts")))
        .select(
            "order_id",
            "customer_id",
            "store_id",
            "employee_id",
            "order_timestamp",
            "order_date",
            "order_status",
            "channel",
            "shipping_cost",
            "tax_amount",
            "order_subtotal",
            "total_amount",
        )
    )

    invalid_df = classified_df.filter(F.col("rejection_reason").isNotNull())
    quarantine_df = format_as_quarantine(invalid_df, "order_id", "orders", "rejection_reason")

    valid_count = clean_df.count()
    quarantine_count = quarantine_df.count()
    orphan_count = classified_df.filter(
        F.col("rejection_reason").isin("ORPHAN_CUSTOMER_FK", "ORPHAN_STORE_FK", "ORPHAN_EMPLOYEE_FK")
    ).count()

    metric = DatasetQualityMetric(
        dataset_name="orders",
        source_row_count=source_count,
        valid_row_count=valid_count,
        quarantine_row_count=quarantine_count,
        duplicate_count=classified_df.filter(F.col("rejection_reason") == "DUPLICATE_ORDER_ID").count(),
        null_mandatory_count=classified_df.filter(F.col("rejection_reason") == "NULL_MANDATORY_FIELD").count(),
        referential_orphan_count=orphan_count,
    )
    return clean_df, quarantine_df, metric


def transform_order_items(
    raw_df: DataFrame,
    clean_orders_df: DataFrame,
    clean_products_df: DataFrame,
) -> tuple[DataFrame, DataFrame, DatasetQualityMetric]:
    """
    Clean, validate, and check referential integrity for order items.
    Enforces product_id and order_id foreign keys, and positive quantities/prices.
    """
    source_count = raw_df.count()
    logger.info("Transforming order_items: received %d records.", source_count)

    normalized_df = (
        raw_df.withColumn("order_item_id", F.trim(F.col("order_item_id")))
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("parsed_quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("parsed_unit_price", F.col("unit_price").cast(DecimalType(10, 2)))
        .withColumn("parsed_discount_pct", F.col("discount_percent").cast(DecimalType(5, 2)))
    )

    window_spec = Window.partitionBy("order_item_id").orderBy(F.col("order_id").asc())
    ranked_df = normalized_df.withColumn("row_num", F.row_number().over(window_spec))

    # Referential joins
    with_order = ranked_df.join(
        clean_orders_df.select(F.col("order_id").alias("ref_order_id")),
        ranked_df.order_id == F.col("ref_order_id"),
        "left",
    )
    with_prod = with_order.join(
        clean_products_df.select(F.col("product_id").alias("ref_prod_id")),
        with_order.product_id == F.col("ref_prod_id"),
        "left",
    )

    classified_df = with_prod.withColumn(
        "rejection_reason",
        F.when(
            (F.col("order_item_id").isNull()) | (F.col("order_item_id") == "") |
            (F.col("parsed_quantity").isNull()) |
            (F.col("parsed_unit_price").isNull()),
            F.lit("NULL_MANDATORY_FIELD"),
        )
        .when(F.col("row_num") > 1, F.lit("DUPLICATE_ORDER_ITEM_ID"))
        .when(F.col("parsed_quantity") <= F.lit(0), F.lit("INVALID_QUANTITY_NON_POSITIVE"))
        .when(F.col("parsed_unit_price") <= F.lit(0), F.lit("INVALID_UNIT_PRICE_NON_POSITIVE"))
        .when(F.col("ref_order_id").isNull(), F.lit("ORPHAN_ORDER_FK"))
        .when(F.col("ref_prod_id").isNull(), F.lit("ORPHAN_PRODUCT_FK"))
        .otherwise(F.lit(None))
    )

    clean_df = (
        classified_df.filter(F.col("rejection_reason").isNull())
        .withColumn("quantity", F.col("parsed_quantity"))
        .withColumn("unit_price", F.col("parsed_unit_price"))
        .withColumn("discount_percent", F.coalesce(F.col("parsed_discount_pct"), F.lit(0.00).cast(DecimalType(5, 2))))
        .select("order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_percent")
    )

    invalid_df = classified_df.filter(F.col("rejection_reason").isNotNull())
    quarantine_df = format_as_quarantine(invalid_df, "order_item_id", "order_items", "rejection_reason")

    orphan_count = classified_df.filter(F.col("rejection_reason").isin("ORPHAN_ORDER_FK", "ORPHAN_PRODUCT_FK")).count()

    metric = DatasetQualityMetric(
        dataset_name="order_items",
        source_row_count=source_count,
        valid_row_count=clean_df.count(),
        quarantine_row_count=quarantine_df.count(),
        duplicate_count=classified_df.filter(F.col("rejection_reason") == "DUPLICATE_ORDER_ITEM_ID").count(),
        null_mandatory_count=classified_df.filter(F.col("rejection_reason") == "NULL_MANDATORY_FIELD").count(),
        referential_orphan_count=orphan_count,
    )
    return clean_df, quarantine_df, metric


def transform_payments(
    raw_df: DataFrame,
    clean_orders_df: DataFrame,
) -> tuple[DataFrame, DataFrame, DatasetQualityMetric]:
    """Clean payments and validate order references and payment reconciliation."""
    source_count = raw_df.count()
    logger.info("Transforming payments: received %d records.", source_count)

    normalized_df = (
        raw_df.withColumn("payment_id", F.trim(F.col("payment_id")))
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("parsed_pay_ts", F.to_timestamp(F.trim(F.col("payment_timestamp")), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("payment_method", F.upper(F.trim(F.col("payment_method"))))
        .withColumn("payment_status", F.upper(F.trim(F.col("payment_status"))))
        .withColumn("parsed_payment_amount", F.col("payment_amount").cast(DecimalType(12, 2)))
        .withColumn("transaction_reference", F.trim(F.col("transaction_reference")))
    )

    window_spec = Window.partitionBy("payment_id").orderBy(F.col("parsed_pay_ts").desc_nulls_last())
    ranked_df = normalized_df.withColumn("row_num", F.row_number().over(window_spec))

    # Join with clean orders
    joined_df = ranked_df.join(
        clean_orders_df.select(
            F.col("order_id").alias("ref_order_id"),
            F.col("total_amount").alias("order_total_amount"),
        ),
        ranked_df.order_id == F.col("ref_order_id"),
        "left",
    )

    classified_df = joined_df.withColumn(
        "rejection_reason",
        F.when(
            (F.col("payment_id").isNull()) | (F.col("payment_id") == "") |
            (F.col("parsed_payment_amount").isNull()),
            F.lit("NULL_MANDATORY_FIELD"),
        )
        .when(F.col("row_num") > 1, F.lit("DUPLICATE_PAYMENT_ID"))
        .when(~F.col("payment_status").isin(VALID_PAYMENT_STATUSES), F.lit("INVALID_PAYMENT_STATUS"))
        .when(F.col("ref_order_id").isNull(), F.lit("ORPHAN_ORDER_FK"))
        .when(
            (F.col("payment_status") == "SUCCESS") &
            (F.abs(F.col("parsed_payment_amount") - F.col("order_total_amount")) > F.lit(0.01)),
            F.lit("PAYMENT_AMOUNT_UNRECONCILED"),
        )
        .otherwise(F.lit(None))
    )

    clean_df = (
        classified_df.filter(F.col("rejection_reason").isNull())
        .withColumn("payment_timestamp", F.col("parsed_pay_ts"))
        .withColumn("payment_amount", F.col("parsed_payment_amount"))
        .select(
            "payment_id",
            "order_id",
            "payment_timestamp",
            "payment_method",
            "payment_status",
            "payment_amount",
            "transaction_reference",
        )
    )

    invalid_df = classified_df.filter(F.col("rejection_reason").isNotNull())
    quarantine_df = format_as_quarantine(invalid_df, "payment_id", "payments", "rejection_reason")

    metric = DatasetQualityMetric(
        dataset_name="payments",
        source_row_count=source_count,
        valid_row_count=clean_df.count(),
        quarantine_row_count=quarantine_df.count(),
        duplicate_count=classified_df.filter(F.col("rejection_reason") == "DUPLICATE_PAYMENT_ID").count(),
        null_mandatory_count=classified_df.filter(F.col("rejection_reason") == "NULL_MANDATORY_FIELD").count(),
        referential_orphan_count=classified_df.filter(F.col("rejection_reason") == "ORPHAN_ORDER_FK").count(),
    )
    return clean_df, quarantine_df, metric


def transform_returns(
    raw_df: DataFrame,
    clean_order_items_df: DataFrame,
) -> tuple[DataFrame, DataFrame, DatasetQualityMetric]:
    """Clean returns and validate order_item referential integrity."""
    source_count = raw_df.count()
    logger.info("Transforming returns: received %d records.", source_count)

    normalized_df = (
        raw_df.withColumn("return_id", F.trim(F.col("return_id")))
        .withColumn("order_item_id", F.trim(F.col("order_item_id")))
        .withColumn("parsed_ret_ts", F.to_timestamp(F.trim(F.col("return_timestamp")), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("return_reason", F.upper(F.trim(F.col("return_reason"))))
        .withColumn("parsed_refund_amount", F.col("refund_amount").cast(DecimalType(10, 2)))
        .withColumn("return_status", F.upper(F.trim(F.col("return_status"))))
    )

    window_spec = Window.partitionBy("return_id").orderBy(F.col("parsed_ret_ts").desc_nulls_last())
    ranked_df = normalized_df.withColumn("row_num", F.row_number().over(window_spec))

    # Join with clean order items
    joined_df = ranked_df.join(
        clean_order_items_df.select(F.col("order_item_id").alias("ref_item_id")),
        ranked_df.order_item_id == F.col("ref_item_id"),
        "left",
    )

    classified_df = joined_df.withColumn(
        "rejection_reason",
        F.when(
            (F.col("return_id").isNull()) | (F.col("return_id") == "") |
            (F.col("parsed_refund_amount").isNull()),
            F.lit("NULL_MANDATORY_FIELD"),
        )
        .when(F.col("row_num") > 1, F.lit("DUPLICATE_RETURN_ID"))
        .when(F.col("ref_item_id").isNull(), F.lit("ORPHAN_ORDER_ITEM_FK"))
        .otherwise(F.lit(None))
    )

    clean_df = (
        classified_df.filter(F.col("rejection_reason").isNull())
        .withColumn("return_timestamp", F.col("parsed_ret_ts"))
        .withColumn("refund_amount", F.col("parsed_refund_amount"))
        .select("return_id", "order_item_id", "return_timestamp", "return_reason", "refund_amount", "return_status")
    )

    invalid_df = classified_df.filter(F.col("rejection_reason").isNotNull())
    quarantine_df = format_as_quarantine(invalid_df, "return_id", "returns", "rejection_reason")

    metric = DatasetQualityMetric(
        dataset_name="returns",
        source_row_count=source_count,
        valid_row_count=clean_df.count(),
        quarantine_row_count=quarantine_df.count(),
        referential_orphan_count=classified_df.filter(F.col("rejection_reason") == "ORPHAN_ORDER_ITEM_FK").count(),
    )
    return clean_df, quarantine_df, metric
