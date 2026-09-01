"""
Dimensional Warehouse Processing Task (Module 5 Lakeflow Jobs).

Invokes Module 4 Kimball star schema modeling (dim_customer SCD2, dim_product SCD1,
dim_store, dim_employee, dim_date, fact_sales, fact_returns) and enterprise quality gates.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql.functions import col
from pyspark.sql.functions import min as spark_min

from src.modeling.dimensions import (
    process_dim_date,
    process_dim_employee,
    process_dim_store,
)
from src.modeling.facts import (
    process_fact_returns,
    process_fact_sales,
)
from src.modeling.quality import run_warehouse_quality_suite
from src.modeling.reconciliation import reconcile_warehouse_sales
from src.modeling.scd_type1 import process_dim_product_scd1
from src.modeling.scd_type2 import process_dim_customer_scd2
from src.orchestration.models import RunContext, TaskValueStore

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def execute_warehouse_task(
    spark: SparkSession,
    context: RunContext,
    task_values: TaskValueStore,
) -> dict:
    """
    Execute Dimensional Warehouse dimensional modeling, fact building, and quality suite.

    Publishes Task Values:
        - fact_sales_rows (int)
        - fact_returns_rows (int)
        - warehouse_quality_passed (bool)
    """
    logger.info("Executing Dimensional Warehouse Modeling for run: %s", context.orchestration_run_id)

    delta_root = str(context.delta_root).rstrip("/")
    silver_str = f"{delta_root}/silver"
    wh_str = f"{delta_root}/warehouse"

    # 1. Load Silver sources
    silver_cust = spark.read.format("delta").load(f"{silver_str}/customers")
    silver_prod = spark.read.format("delta").load(f"{silver_str}/products")
    silver_stor = spark.read.format("delta").load(f"{silver_str}/stores")
    silver_empl = spark.read.format("delta").load(f"{silver_str}/employees")
    silver_ord = spark.read.format("delta").load(f"{silver_str}/orders")
    silver_items = spark.read.format("delta").load(f"{silver_str}/order_items")
    silver_ret = spark.read.format("delta").load(f"{silver_str}/returns")

    # 2. Derive history start for initial SCD2 load
    min_order_ts = silver_ord.select(spark_min(col("order_timestamp"))).collect()[0][0]

    # 3. Process Dimensions
    dim_date_df = process_dim_date(spark, f"{wh_str}/dim_date")
    dim_prod_df = process_dim_product_scd1(spark, silver_prod, f"{wh_str}/dim_product")
    dim_stor_df = process_dim_store(spark, silver_stor, f"{wh_str}/dim_store")
    process_dim_employee(spark, silver_empl, dim_stor_df, f"{wh_str}/dim_employee")
    dim_cust_df = process_dim_customer_scd2(
        spark,
        silver_cust,
        f"{wh_str}/dim_customer",
        initial_effective_from=min_order_ts,
    )

    # 4. Process Facts
    fact_sales_df = process_fact_sales(
        spark=spark,
        silver_order_items_df=silver_items,
        silver_orders_df=silver_ord,
        dim_customer_df=dim_cust_df,
        dim_product_df=dim_prod_df,
        dim_store_df=dim_stor_df,
        dim_date_df=dim_date_df,
        fact_sales_path=f"{wh_str}/fact_sales",
    )
    fact_returns_df = process_fact_returns(
        spark=spark,
        silver_returns_df=silver_ret,
        fact_sales_df=fact_sales_df,
        dim_date_df=dim_date_df,
        fact_returns_path=f"{wh_str}/fact_returns",
    )

    # 5. Quality Gate Execution
    quality_results = run_warehouse_quality_suite(
        spark=spark,
        dim_customer_df=dim_cust_df,
        dim_product_df=dim_prod_df,
        dim_store_df=dim_stor_df,
        dim_date_df=dim_date_df,
        fact_sales_df=fact_sales_df,
        fact_returns_df=fact_returns_df,
        quality_audit_path=f"{wh_str}/quality_audit",
        allow_unknown_keys=False,
        raise_on_failure=True,
    )

    # 6. Warehouse Sales Reconciliation
    reconcile_warehouse_sales(
        silver_order_items_df=silver_items,
        fact_sales_df=fact_sales_df,
        raise_on_failure=True,
    )

    fact_sales_count = fact_sales_df.count()
    fact_returns_count = fact_returns_df.count()
    quality_passed = all(r.passed for r in quality_results)

    task_values.set("dimensional_warehouse", "fact_sales_rows", fact_sales_count)
    task_values.set("dimensional_warehouse", "fact_returns_rows", fact_returns_count)
    task_values.set("dimensional_warehouse", "warehouse_quality_passed", quality_passed)

    logger.info(
        "Dimensional Warehouse complete: %d fact sales, %d fact returns (Quality PASSED)",
        fact_sales_count,
        fact_returns_count,
    )

    return {
        "fact_sales_rows": fact_sales_count,
        "fact_returns_rows": fact_returns_count,
        "warehouse_quality_passed": quality_passed,
        "total_quality_checks": len(quality_results),
    }
