"""
Dimensional Warehouse Pipeline CLI (Module 4).

Orchestrates the Kimball Star Schema warehouse lifecycle:
1. Load Silver Conformed Sources (customers, products, stores, employees, orders, order_items, returns).
2. Build/Update Dimension Tables:
   - dim_date: Generated calendar with temporal keys and unknown record 0.
   - dim_product: SCD Type 1 in-place attribute updates via Delta MERGE.
   - dim_store: Type 1 store dimension with store_key surrogate allocation.
   - dim_employee: Type 1 employee dimension with store linkages.
   - dim_customer: SCD Type 2 history tracking with half-open validity intervals and SHA-256 attribute hashing.
3. Build/Update Fact Tables:
   - fact_sales: Grain = 1 row per valid order item with Point-in-Time SCD2 customer lookup and Decimal measures.
   - fact_returns: Grain = 1 row per return event.
4. Enterprise Data Quality Gates:
   - Completeness, uniqueness, referential integrity, SCD2 invariants, measure validity.
   - Persists execution audit log to delta/warehouse/quality_audit.
   - Critical failures raise WarehouseQualityGateError.
5. Exact Warehouse Reconciliation:
   - Verifies 100% row-count and Decimal monetary reconciliation against Silver sources.

Usage:
    python -m src.pipelines.dimensional_warehouse_pipeline --scale small
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from src.config.settings import (
    SILVER_DIR,
    WAREHOUSE_DIR,
    SparkConfig,
    ensure_directories,
)
from src.modeling.dimensions import (
    process_dim_date,
    process_dim_employee,
    process_dim_store,
)
from src.modeling.facts import (
    process_fact_returns,
    process_fact_sales,
)
from src.modeling.quality import (
    run_warehouse_quality_suite,
)
from src.modeling.reconciliation import reconcile_warehouse_sales
from src.modeling.scd_type1 import process_dim_product_scd1
from src.modeling.scd_type2 import process_dim_customer_scd2
from src.utils.spark import get_spark_session, stop_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def run_dimensional_warehouse_pipeline(
    silver_root: Path | str = SILVER_DIR,
    warehouse_root: Path | str = WAREHOUSE_DIR,
) -> dict:
    """Execute the end-to-end Dimensional Warehouse pipeline."""
    start_time = time.time()
    ensure_directories()

    silver_str = str(silver_root).rstrip("/")
    wh_str = str(warehouse_root).rstrip("/")

    print("=" * 80)
    print("STARTING DIMENSIONAL WAREHOUSE PIPELINE (MODULE 4)")
    print(f"Silver Source: {silver_str} | Warehouse Root: {wh_str}")
    print("=" * 80)

    spark = get_spark_session(SparkConfig(app_name="DimensionalWarehousePipeline"))

    try:
        # --- 1. LOAD CONFORMED SILVER SOURCES ---
        print("\n--- STAGE 1: LOADING SILVER CONFORMED TABLES ---")
        silver_cust = spark.read.format("delta").load(f"{silver_str}/customers")
        silver_prod = spark.read.format("delta").load(f"{silver_str}/products")
        silver_stor = spark.read.format("delta").load(f"{silver_str}/stores")
        silver_empl = spark.read.format("delta").load(f"{silver_str}/employees")
        silver_ord = spark.read.format("delta").load(f"{silver_str}/orders")
        silver_items = spark.read.format("delta").load(f"{silver_str}/order_items")
        silver_ret = spark.read.format("delta").load(f"{silver_str}/returns")

        print(f"  Loaded Silver Customers  : {silver_cust.count():,} rows")
        print(f"  Loaded Silver Products   : {silver_prod.count():,} rows")
        print(f"  Loaded Silver Stores     : {silver_stor.count():,} rows")
        print(f"  Loaded Silver Employees  : {silver_empl.count():,} rows")
        print(f"  Loaded Silver Orders     : {silver_ord.count():,} rows")
        print(f"  Loaded Silver Order Items: {silver_items.count():,} rows")
        print(f"  Loaded Silver Returns    : {silver_ret.count():,} rows")

        # --- 2. BUILD DIMENSIONS ---
        print("\n--- STAGE 2: PROCESSING DIMENSION TABLES ---")
        dim_date_df = process_dim_date(spark, f"{wh_str}/dim_date")
        dim_prod_df = process_dim_product_scd1(spark, silver_prod, f"{wh_str}/dim_product")
        dim_stor_df = process_dim_store(spark, silver_stor, f"{wh_str}/dim_store")
        dim_empl_df = process_dim_employee(spark, silver_empl, dim_stor_df, f"{wh_str}/dim_employee")
        dim_cust_df = process_dim_customer_scd2(spark, silver_cust, f"{wh_str}/dim_customer")

        print(f"  [DIM] dim_date     : {dim_date_df.count():,} rows")
        print(f"  [DIM] dim_product  : {dim_prod_df.count():,} rows (SCD Type 1)")
        print(f"  [DIM] dim_store    : {dim_stor_df.count():,} rows")
        print(f"  [DIM] dim_employee : {dim_empl_df.count():,} rows")
        print(f"  [DIM] dim_customer : {dim_cust_df.count():,} rows (SCD Type 2)")

        # --- 3. BUILD FACT TABLES ---
        print("\n--- STAGE 3: PROCESSING FACT TABLES (POINT-IN-TIME LOOKUPS) ---")
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

        print(f"  [FACT] fact_sales   : {fact_sales_df.count():,} rows (Grain: 1 row / order_item)")
        print(f"  [FACT] fact_returns : {fact_returns_df.count():,} rows (Grain: 1 row / return)")

        # --- 4. ENTERPRISE DATA QUALITY GATES ---
        print("\n--- STAGE 4: ENTERPRISE DATA QUALITY GATES ---")
        quality_results = run_warehouse_quality_suite(
            spark=spark,
            dim_customer_df=dim_cust_df,
            dim_product_df=dim_prod_df,
            dim_store_df=dim_stor_df,
            dim_date_df=dim_date_df,
            fact_sales_df=fact_sales_df,
            fact_returns_df=fact_returns_df,
            quality_audit_path=f"{wh_str}/quality_audit",
            raise_on_failure=True,
        )
        passed_checks = sum(1 for r in quality_results if r.passed)
        print(f"  Quality Gates Passed: {passed_checks}/{len(quality_results)} checks (0 Critical Failures)")

        # --- 5. EXACT WAREHOUSE RECONCILIATION ---
        print("\n--- STAGE 5: WAREHOUSE RECONCILIATION ---")
        recon_result = reconcile_warehouse_sales(
            silver_order_items_df=silver_items,
            fact_sales_df=fact_sales_df,
            raise_on_failure=True,
        )
        print(f"  Row Count Match    : {recon_result['row_count']['silver']:,} == {recon_result['row_count']['fact_sales']:,} (100%)")
        print(f"  Gross Sales Match  : ${recon_result['gross_amount']['silver']:,} == ${recon_result['gross_amount']['fact_sales']:,}")
        print(f"  Net Sales Match    : ${recon_result['net_amount']['silver']:,} == ${recon_result['net_amount']['fact_sales']:,}")

        duration = time.time() - start_time
        print("\n" + "=" * 80)
        print(f"DIMENSIONAL WAREHOUSE PIPELINE COMPLETED IN {duration:.2f} SECONDS")
        print("=" * 80)

        return {
            "dimensions": {
                "dim_date": dim_date_df.count(),
                "dim_product": dim_prod_df.count(),
                "dim_store": dim_stor_df.count(),
                "dim_employee": dim_empl_df.count(),
                "dim_customer": dim_cust_df.count(),
            },
            "facts": {
                "fact_sales": fact_sales_df.count(),
                "fact_returns": fact_returns_df.count(),
            },
            "quality": {
                "total_checks": len(quality_results),
                "passed_checks": passed_checks,
            },
            "reconciliation": recon_result,
            "duration_seconds": duration,
        }

    finally:
        stop_spark_session(spark)


def main():
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Dimensional Warehouse Pipeline CLI (Module 4)")
    parser.add_argument("--silver-dir", type=str, default=str(SILVER_DIR), help="Path to Silver Delta tables")
    parser.add_argument("--warehouse-dir", type=str, default=str(WAREHOUSE_DIR), help="Path to Warehouse Delta tables")

    args = parser.parse_args()
    run_dimensional_warehouse_pipeline(
        silver_root=args.silver_dir,
        warehouse_root=args.warehouse_dir,
    )


if __name__ == "__main__":
    main()
