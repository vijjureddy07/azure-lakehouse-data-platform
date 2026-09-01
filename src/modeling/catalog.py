"""
Unity Catalog Registration for Dimensional Warehouse Schema.

Generates and executes ANSI SQL statements to register external Delta tables in Unity Catalog
under the 3-level namespace (<catalog_name>.warehouse.<table_name>).

Registered Warehouse Tables:
- Dimensions: dim_date, dim_product, dim_store, dim_employee, dim_customer
- Facts: fact_sales, fact_returns
- Audit: quality_audit
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

WAREHOUSE_TABLES = [
    "dim_date",
    "dim_product",
    "dim_store",
    "dim_employee",
    "dim_customer",
    "fact_sales",
    "fact_returns",
    "quality_audit",
]


def generate_warehouse_registration_sql(
    catalog_name: str,
    delta_root_uri: str,
    tables: list[str] | None = None,
) -> list[str]:
    """
    Generate list of idempotent DDL statements registering Warehouse Delta tables in Unity Catalog.

    Args:
        catalog_name: Name of the Unity Catalog (e.g. 'retail_lakehouse').
        delta_root_uri: Base URI for Delta storage (e.g. 'abfss://lakehouse@stlakehousedev.dfs.core.windows.net/delta').
        tables: Optional subset of tables to register.

    Returns:
        list[str]: Executable SQL DDL statements.
    """
    root = delta_root_uri.rstrip("/")
    target_tables = tables or WAREHOUSE_TABLES

    statements: list[str] = [
        f"CREATE CATALOG IF NOT EXISTS {catalog_name};",
        f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.warehouse COMMENT 'Kimball Star Schema Dimensional Warehouse';",
    ]

    for tbl in target_tables:
        loc = f"{root}/warehouse/{tbl}"
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.warehouse.{tbl} USING DELTA LOCATION '{loc}';"
        )

    return statements


def register_warehouse_tables(
    spark: SparkSession,
    catalog_name: str,
    delta_root_uri: str,
    tables: list[str] | None = None,
) -> list[str]:
    """
    Execute registration SQL in Spark to register Warehouse external Delta tables in Unity Catalog.
    """
    statements = generate_warehouse_registration_sql(
        catalog_name=catalog_name,
        delta_root_uri=delta_root_uri,
        tables=tables,
    )
    for sql_stmt in statements:
        logger.info("Executing Unity Catalog warehouse registration: %s", sql_stmt)
        spark.sql(sql_stmt)
    return statements
