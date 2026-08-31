"""
Unity Catalog Registration & DDL Generator.

Generates and executes ANSI SQL statements to register external Delta tables in Unity Catalog
across the 3-level namespace (<catalog_name>.<schema_name>.<table_name>) using governed
ABFSS external location paths.

Logical Namespace Structure:
- <catalog>.bronze.<dataset> (8 raw Bronze tables)
- <catalog>.silver.<dataset> (8 conformed Silver tables)
- <catalog>.silver.quarantine_<dataset> (8 Silver quality quarantine tables)
- <catalog>.gold.<table_name> (6 business aggregate Gold tables)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

DATASETS = [
    "customers",
    "products",
    "stores",
    "employees",
    "orders",
    "order_items",
    "payments",
    "returns",
]

GOLD_TABLES = [
    "gold_daily_sales_performance",
    "gold_monthly_revenue",
    "gold_revenue_by_store_region",
    "gold_category_revenue_performance",
    "gold_customer_spending_summary",
    "gold_return_refund_performance",
]


def generate_unity_catalog_registration_sql(
    catalog_name: str,
    delta_root_uri: str,
) -> list[str]:
    """
    Generate list of idempotent DDL statements registering all Medallion Delta tables in Unity Catalog.

    Args:
        catalog_name: Name of the Unity Catalog (e.g., 'retail_lakehouse').
        delta_root_uri: Base URI for Delta storage (e.g., 'abfss://lakehouse@stlakehousedev.dfs.core.windows.net/delta').

    Returns:
        list[str]: Executable SQL DDL statements.
    """
    root = delta_root_uri.rstrip("/")
    statements: list[str] = [
        f"CREATE CATALOG IF NOT EXISTS {catalog_name};",
        f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze;",
        f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver;",
        f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold;",
    ]

    # Bronze tables
    for ds in DATASETS:
        loc = f"{root}/bronze/{ds}"
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.{ds} USING DELTA LOCATION '{loc}';"
        )

    # Silver tables
    for ds in DATASETS:
        loc = f"{root}/silver/{ds}"
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.silver.{ds} USING DELTA LOCATION '{loc}';"
        )

    # Silver Quarantine tables
    for ds in DATASETS:
        loc = f"{root}/silver/quarantine/{ds}"
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.silver.quarantine_{ds} USING DELTA LOCATION '{loc}';"
        )

    # Gold tables
    for tbl in GOLD_TABLES:
        loc = f"{root}/gold/{tbl}"
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.gold.{tbl} USING DELTA LOCATION '{loc}';"
        )

    return statements


def register_medallion_tables_in_catalog(
    spark: SparkSession,
    catalog_name: str,
    delta_root_uri: str,
) -> list[str]:
    """
    Execute registration SQL in Spark to register all external Delta tables into Unity Catalog.

    Returns:
        list[str]: The executed SQL DDL statements.
    """
    statements = generate_unity_catalog_registration_sql(catalog_name, delta_root_uri)
    for sql_stmt in statements:
        logger.info("Executing Unity Catalog registration: %s", sql_stmt)
        spark.sql(sql_stmt)
    return statements
