"""
Module 4: Dimensional Modeling, Slowly Changing Dimensions, and Enterprise Quality Gates.
"""

from src.modeling.catalog import (
    generate_warehouse_registration_sql,
    register_warehouse_tables,
)
from src.modeling.dimensions import (
    build_dim_date,
    process_dim_date,
    process_dim_employee,
    process_dim_store,
)
from src.modeling.facts import (
    build_fact_sales_dataframe,
    process_fact_returns,
    process_fact_sales,
)
from src.modeling.quality import (
    QualityCheckResult,
    WarehouseQualityGateError,
    run_warehouse_quality_suite,
)
from src.modeling.reconciliation import reconcile_warehouse_sales
from src.modeling.scd_type1 import process_dim_product_scd1
from src.modeling.scd_type2 import process_dim_customer_scd2
from src.modeling.surrogate_keys import assign_surrogate_keys

__all__ = [
    "QualityCheckResult",
    "WarehouseQualityGateError",
    "assign_surrogate_keys",
    "build_dim_date",
    "build_fact_sales_dataframe",
    "generate_warehouse_registration_sql",
    "process_dim_customer_scd2",
    "process_dim_date",
    "process_dim_employee",
    "process_dim_product_scd1",
    "process_dim_store",
    "process_fact_returns",
    "process_fact_sales",
    "reconcile_warehouse_sales",
    "register_warehouse_tables",
    "run_warehouse_quality_suite",
]
