"""
Warehouse Financial & Row Count Reconciliation.

Performs strict mathematical and financial verification between the conformed Silver layer
and the dimensional warehouse fact tables:
- Row Count Invariant: Eligible Silver order_items == fact_sales rows.
- Decimal Monetary Invariant: Total Silver net_amount == Total fact_sales net_amount.
- Gross Revenue Invariant: Total Silver (quantity * unit_price) == Total fact_sales gross_amount.
- Discount Invariant: Total Silver discount_amount == Total fact_sales discount_amount.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pyspark.sql.functions import col
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import DecimalType

from src.modeling.quality import WarehouseQualityGateError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def reconcile_warehouse_sales(
    silver_order_items_df: DataFrame,
    fact_sales_df: DataFrame,
    raise_on_failure: bool = True,
) -> dict[str, Any]:
    """
    Verify exact row-count and monetary Decimal reconciliation between Silver and Fact Sales.

    Raises:
        WarehouseQualityGateError: If any row-count or financial metric fails exact reconciliation.
    """
    # 1. Row Count Invariant
    silver_count = silver_order_items_df.count()
    fact_count = fact_sales_df.count()

    # 2. Financial Metrics Invariant (Exact Decimal sums)
    silver_metrics = (
        silver_order_items_df
        .select(
            spark_sum((col("quantity") * col("unit_price")).cast(DecimalType(14, 2))).alias("gross"),
            spark_sum(col("discount_amount").cast(DecimalType(14, 2))).alias("discount"),
            spark_sum(col("net_amount").cast(DecimalType(14, 2))).alias("net"),
        )
        .collect()[0]
    )

    fact_metrics = (
        fact_sales_df
        .select(
            spark_sum(col("gross_amount").cast(DecimalType(14, 2))).alias("gross"),
            spark_sum(col("discount_amount").cast(DecimalType(14, 2))).alias("discount"),
            spark_sum(col("net_amount").cast(DecimalType(14, 2))).alias("net"),
        )
        .collect()[0]
    )

    silver_gross: Decimal = silver_metrics["gross"] or Decimal("0.00")
    silver_discount: Decimal = silver_metrics["discount"] or Decimal("0.00")
    silver_net: Decimal = silver_metrics["net"] or Decimal("0.00")

    fact_gross: Decimal = fact_metrics["gross"] or Decimal("0.00")
    fact_discount: Decimal = fact_metrics["discount"] or Decimal("0.00")
    fact_net: Decimal = fact_metrics["net"] or Decimal("0.00")

    count_match = silver_count == fact_count
    gross_match = silver_gross == fact_gross
    discount_match = silver_discount == fact_discount
    net_match = silver_net == fact_net

    all_passed = count_match and gross_match and discount_match and net_match

    result: dict[str, Any] = {
        "passed": all_passed,
        "row_count": {
            "silver": silver_count,
            "fact_sales": fact_count,
            "match": count_match,
        },
        "gross_amount": {
            "silver": silver_gross,
            "fact_sales": fact_gross,
            "match": gross_match,
        },
        "discount_amount": {
            "silver": silver_discount,
            "fact_sales": fact_discount,
            "match": discount_match,
        },
        "net_amount": {
            "silver": silver_net,
            "fact_sales": fact_net,
            "match": net_match,
        },
    }

    if not all_passed and raise_on_failure:
        err_msg = (
            f"Warehouse Sales Reconciliation Failed:\n"
            f"  - Row Count: Silver={silver_count} vs Fact={fact_count} (Match={count_match})\n"
            f"  - Gross Revenue: Silver={silver_gross} vs Fact={fact_gross} (Match={gross_match})\n"
            f"  - Discounts: Silver={silver_discount} vs Fact={fact_discount} (Match={discount_match})\n"
            f"  - Net Sales: Silver={silver_net} vs Fact={fact_net} (Match={net_match})"
        )
        raise WarehouseQualityGateError(err_msg)

    logger.info(
        "Warehouse Sales Reconciliation Passed: %d rows | Gross=%s | Discount=%s | Net=%s",
        fact_count,
        fact_gross,
        fact_discount,
        fact_net,
    )
    return result
