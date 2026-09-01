"""
Unity Catalog Registration & DDL Generator.

Generates and executes ANSI SQL statements to register external Delta tables in Unity Catalog
across the 3-level namespace (<catalog_name>.<schema_name>.<table_name>) using governed
ABFSS external location paths.

Supports independent, layer-specific registration:
- Bronze: <catalog>.bronze.<dataset> (8 raw Bronze tables)
- Silver: <catalog>.silver.<dataset> (8 conformed Silver tables) + <catalog>.silver.quarantine_<dataset> (8 Silver quarantine tables)
- Gold: <catalog>.gold.<table_name> (6 business aggregate Gold tables)

This layer separation ensures notebooks do not attempt to register tables before their
corresponding Delta transaction logs physically exist on storage.
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
    layers: list[str] | None = None,
) -> list[str]:
    """
    Generate list of idempotent DDL statements registering Medallion Delta tables in Unity Catalog.

    Args:
        catalog_name: Name of the Unity Catalog (e.g., 'retail_lakehouse').
        delta_root_uri: Base URI for Delta storage (e.g., 'abfss://lakehouse@stlakehousedev.dfs.core.windows.net/delta').
        layers: Optional list of layers to register (e.g., ['bronze'], ['silver'], ['gold']).
                Defaults to ['bronze', 'silver', 'gold'].

    Returns:
        list[str]: Executable SQL DDL statements strictly for the requested layers.
    """
    root = delta_root_uri.rstrip("/")
    target_layers = [lyr.lower() for lyr in layers] if layers else ["bronze", "silver", "gold"]

    statements: list[str] = [
        f"CREATE CATALOG IF NOT EXISTS {catalog_name};",
    ]

    # Bronze Layer Registration
    if "bronze" in target_layers:
        statements.append(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze;")
        for ds in DATASETS:
            loc = f"{root}/bronze/{ds}"
            statements.append(
                f"CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.{ds} USING DELTA LOCATION '{loc}';"
            )

    # Silver Layer Registration (Conformed Tables + Quarantine Tables)
    if "silver" in target_layers:
        statements.append(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver;")
        for ds in DATASETS:
            loc = f"{root}/silver/{ds}"
            statements.append(
                f"CREATE TABLE IF NOT EXISTS {catalog_name}.silver.{ds} USING DELTA LOCATION '{loc}';"
            )
        for ds in DATASETS:
            loc = f"{root}/silver/quarantine/{ds}"
            statements.append(
                f"CREATE TABLE IF NOT EXISTS {catalog_name}.silver.quarantine_{ds} USING DELTA LOCATION '{loc}';"
            )

    # Gold Layer Registration
    if "gold" in target_layers:
        statements.append(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold;")
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
    layers: list[str] | None = None,
) -> list[str]:
    """
    Execute registration SQL in Spark to register external Delta tables into Unity Catalog
    for the specified layers.

    Args:
        spark: Active SparkSession.
        catalog_name: Name of the Unity Catalog.
        delta_root_uri: Base URI for Delta storage.
        layers: Optional list of layers to register (['bronze'], ['silver'], ['gold']).

    Returns:
        list[str]: The executed SQL DDL statements.
    """
    statements = generate_unity_catalog_registration_sql(
        catalog_name=catalog_name,
        delta_root_uri=delta_root_uri,
        layers=layers,
    )
    for sql_stmt in statements:
        logger.info("Executing Unity Catalog registration: %s", sql_stmt)
        spark.sql(sql_stmt)
    return statements


def register_bronze_tables(
    spark: SparkSession,
    catalog_name: str,
    delta_root_uri: str,
) -> list[str]:
    """Register only Bronze Delta tables in Unity Catalog."""
    return register_medallion_tables_in_catalog(
        spark=spark,
        catalog_name=catalog_name,
        delta_root_uri=delta_root_uri,
        layers=["bronze"],
    )


def register_silver_tables(
    spark: SparkSession,
    catalog_name: str,
    delta_root_uri: str,
) -> list[str]:
    """Register only Silver conformed and quarantine Delta tables in Unity Catalog."""
    return register_medallion_tables_in_catalog(
        spark=spark,
        catalog_name=catalog_name,
        delta_root_uri=delta_root_uri,
        layers=["silver"],
    )


def register_gold_tables(
    spark: SparkSession,
    catalog_name: str,
    delta_root_uri: str,
) -> list[str]:
    """Register only Gold analytical Delta tables in Unity Catalog."""
    return register_medallion_tables_in_catalog(
        spark=spark,
        catalog_name=catalog_name,
        delta_root_uri=delta_root_uri,
        layers=["gold"],
    )
