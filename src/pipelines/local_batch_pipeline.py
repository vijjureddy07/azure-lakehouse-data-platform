"""
Local PySpark Batch Pipeline Runner (Module 1).

Orchestrates:
1. Synthetic Data Generation (or loading existing raw files)
2. Schema-Enforced Ingestion (CSV & JSON)
3. Cleaning, Quality Rule Validations, and Quarantine Routing
4. Idempotent Parquet Writes for Cleaned & Quarantine Tables
5. Curated Sales Dataset Construction with Window Functions
6. Partitioned Parquet Output (order_year, order_month)
7. Spark SQL Analytical Views Execution
8. Quality Reconciliation Metrics Generation
9. Clean Spark Session Shutdown
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.config.settings import (
    OUTPUT_DIR,
    RAW_DATA_DIR,
    SQL_DIR,
    SparkConfig,
    ensure_directories,
)
from src.data_generation.generate_retail_data import generate_retail_dataset
from src.quality.rules import (
    DatasetQualityMetric,
    metrics_to_dataframe,
    validate_reconciliation,
)
from src.schemas.retail_schemas import (
    RAW_CUSTOMERS_SCHEMA,
    RAW_EMPLOYEES_SCHEMA,
    RAW_ORDER_ITEMS_SCHEMA,
    RAW_ORDERS_SCHEMA,
    RAW_PAYMENTS_SCHEMA,
    RAW_PRODUCTS_SCHEMA,
    RAW_RETURNS_SCHEMA,
    RAW_STORES_SCHEMA,
)
from src.transformations.customers import transform_customers
from src.transformations.orders import (
    transform_employees,
    transform_order_items,
    transform_orders,
    transform_payments,
    transform_returns,
    transform_stores,
)
from src.transformations.products import transform_products
from src.transformations.sales import build_curated_sales
from src.utils.spark import get_spark_session, stop_spark_session

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("LocalBatchPipeline")


class LocalBatchPipeline:
    """End-to-end Local PySpark Batch Pipeline."""

    def __init__(
        self,
        scale: str = "small",
        data_dir: Path = RAW_DATA_DIR,
        output_dir: Path = OUTPUT_DIR,
        skip_data_gen: bool = False,
    ):
        self.scale = scale.lower()
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.skip_data_gen = skip_data_gen
        self.cleaned_dir = output_dir / "cleaned"
        self.quarantine_dir = output_dir / "quarantine"
        self.curated_dir = output_dir / "curated"
        self.metrics_dir = output_dir / "metrics"
        self.spark: SparkSession = None
        self.metrics: list[DatasetQualityMetric] = []

    def run(self) -> dict[str, Any]:
        """Execute all pipeline stages sequentially."""
        start_time = time.time()
        logger.info("=" * 70)
        logger.info("STARTING AZURE LAKEHOUSE LOCAL BATCH PIPELINE (MODULE 1)")
        logger.info("Scale: %s | Data Dir: %s | Output Dir: %s", self.scale, self.data_dir, self.output_dir)
        logger.info("=" * 70)

        ensure_directories()

        # Stage 1: Data Generation
        if not self.skip_data_gen:
            logger.info("\n--- STAGE 1: SYNTHETIC DATA GENERATION ---")
            gen_counts = generate_retail_dataset(scale=self.scale, output_dir=self.data_dir)
            logger.info("Generated source records: %s", gen_counts)
        else:
            logger.info("\n--- STAGE 1: SKIPPING DATA GENERATION (Using existing files) ---")

        # Stage 2: Initialize Spark
        logger.info("\n--- STAGE 2: INITIALIZING LOCAL SPARK SESSION ---")
        spark_cfg = SparkConfig(app_name=f"AzureLakehouse_Module1_{self.scale}")
        self.spark = get_spark_session(spark_cfg)
        logger.info("SparkSession active: Spark %s (Master: %s)", self.spark.version, spark_cfg.master)

        try:
            # Stage 3: Ingest Raw Files with Explicit Schemas
            logger.info("\n--- STAGE 3: SCHEMA-ENFORCED RAW INGESTION ---")
            raw_dfs = self._ingest_raw_data()

            # Stage 4: Transformations, Quality Checks, and Quarantine
            logger.info("\n--- STAGE 4: CLEANING, TRANSFORMATIONS & DATA QUALITY ENFORCEMENT ---")
            clean_dfs, quarantine_dfs = self._process_transformations(raw_dfs)

            # Stage 5: Write Cleaned and Quarantine Datasets to Parquet (Idempotent Overwrite)
            logger.info("\n--- STAGE 5: PERSISTING CLEANED & QUARANTINE PARQUET DATASETS ---")
            self._write_cleaned_and_quarantine(clean_dfs, quarantine_dfs)

            # Stage 6 & 7: Curation and Window Transformations
            logger.info("\n--- STAGE 6 & 7: ANALYTICAL CURATION & PARTITIONED PARQUET EXPORT ---")
            curated_sales_df = build_curated_sales(
                clean_orders_df=clean_dfs["orders"],
                clean_order_items_df=clean_dfs["order_items"],
                clean_products_df=clean_dfs["products"],
                clean_customers_df=clean_dfs["customers"],
                clean_stores_df=clean_dfs["stores"],
            )
            # Write partitioned by order_year and order_month
            curated_path = str(self.curated_dir / "curated_sales")
            logger.info("Writing curated_sales to Parquet (Partitioned by order_year, order_month): %s", curated_path)
            curated_sales_df.write.mode("overwrite").partitionBy("order_year", "order_month").parquet(curated_path)

            # Stage 8: Spark SQL Analytics
            logger.info("\n--- STAGE 8: REGISTERING SPARK SQL VIEWS & EXECUTING ANALYTICS ---")
            self._execute_spark_sql_analytics(clean_dfs, curated_sales_df)

            # Stage 9: Quality Metrics Consolidation & Reconciliation Validation
            logger.info("\n--- STAGE 9: DATA QUALITY AUDIT RECONCILIATION ---")
            validate_reconciliation(self.metrics)
            logger.info("Reconciliation validation passed for all %d datasets.", len(self.metrics))
            metrics_df = metrics_to_dataframe(self.spark, self.metrics)
            metrics_path = str(self.metrics_dir / "quality_summary")
            metrics_df.write.mode("overwrite").parquet(metrics_path)
            logger.info("Data Quality Metrics Summary:")
            metrics_df.show(truncate=False)

            elapsed = round(time.time() - start_time, 2)
            logger.info("\n" + "=" * 70)
            logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY in %.2f seconds", elapsed)
            logger.info("=" * 70)

            return {
                "status": "SUCCESS",
                "scale": self.scale,
                "elapsed_seconds": elapsed,
                "metrics": [m.to_dict() for m in self.metrics],
            }

        finally:
            logger.info("Stopping Spark session...")
            stop_spark_session(self.spark)

    def _ingest_raw_data(self) -> dict[str, DataFrame]:
        """Read CSV and JSON files using explicit schemas."""
        csv_options = {"header": "true", "mode": "PERMISSIVE"}

        customers_raw = self.spark.read.options(**csv_options).schema(RAW_CUSTOMERS_SCHEMA).csv(str(self.data_dir / "customers.csv"))
        products_raw = self.spark.read.options(**csv_options).schema(RAW_PRODUCTS_SCHEMA).csv(str(self.data_dir / "products.csv"))
        stores_raw = self.spark.read.options(**csv_options).schema(RAW_STORES_SCHEMA).csv(str(self.data_dir / "stores.csv"))
        employees_raw = self.spark.read.options(**csv_options).schema(RAW_EMPLOYEES_SCHEMA).csv(str(self.data_dir / "employees.csv"))
        orders_raw = self.spark.read.options(**csv_options).schema(RAW_ORDERS_SCHEMA).csv(str(self.data_dir / "orders.csv"))
        order_items_raw = self.spark.read.options(**csv_options).schema(RAW_ORDER_ITEMS_SCHEMA).csv(str(self.data_dir / "order_items.csv"))
        returns_raw = self.spark.read.options(**csv_options).schema(RAW_RETURNS_SCHEMA).csv(str(self.data_dir / "returns.csv"))

        # Read JSON (JSON Lines format)
        payments_raw = self.spark.read.schema(RAW_PAYMENTS_SCHEMA).json(str(self.data_dir / "payments.json"))

        logger.info("Ingested 8 raw source datasets successfully.")
        return {
            "customers": customers_raw,
            "products": products_raw,
            "stores": stores_raw,
            "employees": employees_raw,
            "orders": orders_raw,
            "order_items": order_items_raw,
            "payments": payments_raw,
            "returns": returns_raw,
        }

    def _process_transformations(self, raw_dfs: dict[str, DataFrame]) -> tuple[dict[str, DataFrame], dict[str, DataFrame]]:
        """Run transformations, referential integrity checks, and quarantine routing."""
        clean_dfs: dict[str, DataFrame] = {}
        quarantine_dfs: dict[str, DataFrame] = {}
        self.metrics.clear()

        # 1. Stores (Independent entity)
        clean_stores, quar_stores, m_stores = transform_stores(raw_dfs["stores"])
        clean_dfs["stores"] = clean_stores
        quarantine_dfs["stores"] = quar_stores
        self.metrics.append(m_stores)

        # 2. Employees (Depends on stores)
        clean_employees, quar_employees, m_employees = transform_employees(raw_dfs["employees"], clean_stores)
        clean_dfs["employees"] = clean_employees
        quarantine_dfs["employees"] = quar_employees
        self.metrics.append(m_employees)

        # 3. Customers (Independent entity)
        clean_cust, quar_cust, m_cust = transform_customers(raw_dfs["customers"])
        clean_dfs["customers"] = clean_cust
        quarantine_dfs["customers"] = quar_cust
        self.metrics.append(m_cust)

        # 4. Products (Independent entity)
        clean_prod, quar_prod, m_prod = transform_products(raw_dfs["products"])
        clean_dfs["products"] = clean_prod
        quarantine_dfs["products"] = quar_prod
        self.metrics.append(m_prod)

        # 5. Orders (Depends on customers, stores, employees)
        clean_orders, quar_orders, m_orders = transform_orders(
            raw_dfs["orders"],
            clean_customers_df=clean_cust,
            clean_stores_df=clean_stores,
            clean_employees_df=clean_employees,
        )
        clean_dfs["orders"] = clean_orders
        quarantine_dfs["orders"] = quar_orders
        self.metrics.append(m_orders)

        # 6. Order Items (Depends on orders, products)
        clean_items, quar_items, m_items = transform_order_items(
            raw_dfs["order_items"],
            clean_orders_df=clean_orders,
            clean_products_df=clean_prod,
        )
        clean_dfs["order_items"] = clean_items
        quarantine_dfs["order_items"] = quar_items
        self.metrics.append(m_items)

        # 7. Payments (Depends on orders)
        clean_pay, quar_pay, m_pay = transform_payments(raw_dfs["payments"], clean_orders)
        clean_dfs["payments"] = clean_pay
        quarantine_dfs["payments"] = quar_pay
        self.metrics.append(m_pay)

        # 8. Returns (Depends on order_items)
        clean_ret, quar_ret, m_ret = transform_returns(raw_dfs["returns"], clean_items)
        clean_dfs["returns"] = clean_ret
        quarantine_dfs["returns"] = quar_ret
        self.metrics.append(m_ret)

        return clean_dfs, quarantine_dfs

    def _write_cleaned_and_quarantine(
        self,
        clean_dfs: dict[str, DataFrame],
        quarantine_dfs: dict[str, DataFrame],
    ) -> None:
        """Write cleaned and quarantine datasets to Parquet."""
        for name, df in clean_dfs.items():
            path = str(self.cleaned_dir / name)
            df.write.mode("overwrite").parquet(path)
            logger.info("Persisted clean dataset [%s] -> %s", name, path)

        for name, df in quarantine_dfs.items():
            path = str(self.quarantine_dir / name)
            df.write.mode("overwrite").parquet(path)
            logger.info("Persisted quarantine dataset [%s] -> %s", name, path)

    def _execute_spark_sql_analytics(
        self,
        clean_dfs: dict[str, DataFrame],
        curated_sales_df: DataFrame,
    ) -> None:
        """Register views and execute Spark SQL queries."""
        # Register views
        curated_sales_df.createOrReplaceTempView("v_curated_sales")
        for name, df in clean_dfs.items():
            df.createOrReplaceTempView(f"v_{name}")

        sql_files = sorted(SQL_DIR.glob("*.sql"))
        if not sql_files:
            logger.warning("No SQL files found in %s", SQL_DIR)
            return

        for sql_file in sql_files:
            logger.info("\n>>> Executing Spark SQL: %s", sql_file.name)
            with open(sql_file, "r", encoding="utf-8") as f:
                query = f.read()

            try:
                result_df = self.spark.sql(query)
                row_count = result_df.count()
                logger.info("SQL Query [%s] executed successfully (%d rows returned):", sql_file.name, row_count)
                result_df.show(5, truncate=False)
            except Exception as e:
                logger.error("Error executing SQL file %s: %s", sql_file.name, e)
                raise


def run_pipeline(scale: str = "small", skip_data_gen: bool = False) -> dict[str, Any]:
    """Helper runner function."""
    pipeline = LocalBatchPipeline(scale=scale, skip_data_gen=skip_data_gen)
    return pipeline.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Azure Lakehouse Local Batch Pipeline (Module 1)")
    parser.add_argument("--scale", choices=["small", "standard"], default="small", help="Dataset scale preset")
    parser.add_argument("--skip-data-gen", action="store_true", help="Skip synthetic data generation step")
    args = parser.parse_args()

    result = run_pipeline(scale=args.scale, skip_data_gen=args.skip_data_gen)
    if result["status"] != "SUCCESS":
        sys.exit(1)
