"""
Enterprise Warehouse Data Quality Gates & Audit Framework.

Implements automated, multi-dimensional data quality verification for Kimball star schemas:
1. COMPLETENESS: Ensures surrogate and natural keys are non-null.
2. UNIQUENESS: Verifies primary key grain uniqueness for dimensions and facts.
3. REFERENTIAL INTEGRITY: Confirms fact foreign keys resolve to dimension surrogate keys.
4. SCD2 INVARIANTS:
   - Exactly one current record per business key.
   - Current records have effective_to IS NULL.
   - Inactive records have effective_to IS NOT NULL and effective_from < effective_to.
   - Non-overlapping validity intervals per business key.
5. MEASURE VALIDITY: Confirms non-negative pricing, positive quantities, and arithmetic integrity.
6. QUALITY AUDIT SINK: Persists all check executions to delta/warehouse/quality_audit.
7. CRITICAL GATE ENFORCEMENT: Explicitly raises WarehouseQualityGateError on critical failures.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql.functions import col, lag, lit, when
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class WarehouseQualityGateError(RuntimeError):
    """Raised when one or more CRITICAL quality gate checks fail in Module 4."""
    pass


@dataclass
class QualityCheckResult:
    """Individual quality check outcome."""
    check_name: str
    table_name: str
    check_type: str  # COMPLETENESS, UNIQUENESS, REFERENTIAL_INTEGRITY, SCD2_INVARIANT, MEASURE_VALIDITY, RECONCILIATION
    severity: str    # CRITICAL, WARNING
    passed: bool
    observed_value: str
    expected_value: str
    failed_row_count: int
    execution_timestamp: datetime


QUALITY_AUDIT_SCHEMA = StructType([
    StructField("check_name", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("check_type", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("passed", BooleanType(), False),
    StructField("observed_value", StringType(), True),
    StructField("expected_value", StringType(), True),
    StructField("failed_row_count", IntegerType(), False),
    StructField("execution_timestamp", TimestampType(), False),
])


def check_completeness(
    df: DataFrame,
    table_name: str,
    required_cols: list[str],
    severity: str = "CRITICAL",
) -> list[QualityCheckResult]:
    """Verify that required columns do not contain null values."""
    results: list[QualityCheckResult] = []
    now = datetime.now(timezone.utc)

    for c in required_cols:
        null_count = df.filter(col(c).isNull()).count()
        passed = null_count == 0
        results.append(
            QualityCheckResult(
                check_name=f"completeness_{table_name}_{c}",
                table_name=table_name,
                check_type="COMPLETENESS",
                severity=severity,
                passed=passed,
                observed_value=f"{null_count} nulls",
                expected_value="0 nulls",
                failed_row_count=null_count,
                execution_timestamp=now,
            )
        )
    return results


def check_uniqueness(
    df: DataFrame,
    table_name: str,
    grain_cols: list[str],
    severity: str = "CRITICAL",
) -> QualityCheckResult:
    """Verify uniqueness across the specified grain/key columns."""
    now = datetime.now(timezone.utc)
    total_rows = df.count()
    distinct_rows = df.select(*grain_cols).distinct().count()
    duplicate_count = total_rows - distinct_rows
    passed = duplicate_count == 0

    return QualityCheckResult(
        check_name=f"uniqueness_{table_name}_{'_'.join(grain_cols)}",
        table_name=table_name,
        check_type="UNIQUENESS",
        severity=severity,
        passed=passed,
        observed_value=f"{duplicate_count} duplicate keys",
        expected_value="0 duplicate keys",
        failed_row_count=duplicate_count,
        execution_timestamp=now,
    )


def check_referential_integrity(
    fact_df: DataFrame,
    fact_table_name: str,
    fk_col: str,
    dim_df: DataFrame,
    dim_pk_col: str,
    allow_unknown_zero: bool = True,
    severity: str = "CRITICAL",
) -> QualityCheckResult:
    """Verify that fact foreign keys resolve to dimension surrogate keys."""
    now = datetime.now(timezone.utc)
    eval_fact = fact_df.filter(col(fk_col) != 0) if allow_unknown_zero else fact_df

    dim_keys = dim_df.select(col(dim_pk_col).alias("dim_key")).distinct()
    orphans_df = eval_fact.join(dim_keys, eval_fact[fk_col] == dim_keys["dim_key"], how="left_anti")
    orphan_count = orphans_df.count()
    passed = orphan_count == 0

    return QualityCheckResult(
        check_name=f"referential_integrity_{fact_table_name}_{fk_col}",
        table_name=fact_table_name,
        check_type="REFERENTIAL_INTEGRITY",
        severity=severity,
        passed=passed,
        observed_value=f"{orphan_count} orphan FKs",
        expected_value="0 orphan FKs",
        failed_row_count=orphan_count,
        execution_timestamp=now,
    )


def check_unknown_member_usage(
    fact_df: DataFrame,
    table_name: str,
    key_cols: list[str],
    allow_unknown_keys: bool = False,
    severity: str = "CRITICAL",
) -> list[QualityCheckResult]:
    """
    Verify whether fact records contain surrogate key 0 (Unknown Member).

    In normal production runs (allow_unknown_keys=False), unexpected resolution to
    surrogate key 0 indicates missing dimension attribution and triggers a CRITICAL gate failure.
    For simulated late-arriving dimension workflows, allow_unknown_keys=True records a passed check
    with severity=WARNING.
    """
    results: list[QualityCheckResult] = []
    now = datetime.now(timezone.utc)

    for k in key_cols:
        unknown_count = fact_df.filter(col(k) == 0).count()
        passed = (unknown_count == 0) or allow_unknown_keys
        results.append(
            QualityCheckResult(
                check_name=f"unknown_member_usage_{table_name}_{k}",
                table_name=table_name,
                check_type="REFERENTIAL_INTEGRITY",
                severity=severity if not allow_unknown_keys else "WARNING",
                passed=passed,
                observed_value=f"{unknown_count} rows with key 0",
                expected_value="0 rows with key 0" if not allow_unknown_keys else "key 0 permitted for late-arriving simulation",
                failed_row_count=unknown_count if not allow_unknown_keys else 0,
                execution_timestamp=now,
            )
        )
    return results


def check_scd2_invariants(
    dim_customer_df: DataFrame,
    severity: str = "CRITICAL",
) -> list[QualityCheckResult]:
    """
    Verify Kimball SCD Type 2 structural invariants:
    - Exactly one active (is_current = True) record per customer business key.
    - Active records have effective_to IS NULL.
    - Inactive records have effective_to IS NOT NULL and effective_from < effective_to.
    - Non-overlapping validity intervals per customer.
    """
    results: list[QualityCheckResult] = []
    now = datetime.now(timezone.utc)

    # Invariant 1: Exactly 1 current record per customer_id (fails on 0 or >1)
    cust_current_counts = (
        dim_customer_df
        .groupBy("customer_id")
        .agg(
            spark_sum(when(col("is_current") == lit(True), 1).otherwise(0)).alias("cur_count")
        )
    )
    invalid_custs = cust_current_counts.filter(col("cur_count") != 1)
    invalid_count = invalid_custs.count()
    results.append(
        QualityCheckResult(
            check_name="scd2_exactly_one_current_record_per_customer",
            table_name="dim_customer",
            check_type="SCD2_INVARIANT",
            severity=severity,
            passed=invalid_count == 0,
            observed_value=f"{invalid_count} customers without exactly 1 current record",
            expected_value="0 customers without exactly 1 current record",
            failed_row_count=invalid_count,
            execution_timestamp=now,
        )
    )

    # Invariant 2: Current records must have effective_to IS NULL
    invalid_current_effective = dim_customer_df.filter(
        (col("is_current") == lit(True)) & col("effective_to").isNotNull()
    ).count()
    results.append(
        QualityCheckResult(
            check_name="scd2_current_record_null_effective_to",
            table_name="dim_customer",
            check_type="SCD2_INVARIANT",
            severity=severity,
            passed=invalid_current_effective == 0,
            observed_value=f"{invalid_current_effective} current records with non-null effective_to",
            expected_value="0 current records with non-null effective_to",
            failed_row_count=invalid_current_effective,
            execution_timestamp=now,
        )
    )

    # Invariant 3: Expired records must have effective_from < effective_to
    invalid_expired_order = dim_customer_df.filter(
        (col("is_current") == lit(False))
        & (col("effective_to").isNull() | (col("effective_from") >= col("effective_to")))
    ).count()
    results.append(
        QualityCheckResult(
            check_name="scd2_expired_record_chronological_validity",
            table_name="dim_customer",
            check_type="SCD2_INVARIANT",
            severity=severity,
            passed=invalid_expired_order == 0,
            observed_value=f"{invalid_expired_order} expired records with inverted/null interval",
            expected_value="0 expired records with inverted/null interval",
            failed_row_count=invalid_expired_order,
            execution_timestamp=now,
        )
    )

    # Invariant 4: No overlapping validity intervals per customer
    win = Window.partitionBy("customer_id").orderBy("effective_from")
    overlap_df = (
        dim_customer_df
        .withColumn("prev_effective_to", lag(col("effective_to")).over(win))
        .filter(col("prev_effective_to").isNotNull() & (col("effective_from") < col("prev_effective_to")))
    )
    overlap_count = overlap_df.count()
    results.append(
        QualityCheckResult(
            check_name="scd2_non_overlapping_intervals",
            table_name="dim_customer",
            check_type="SCD2_INVARIANT",
            severity=severity,
            passed=overlap_count == 0,
            observed_value=f"{overlap_count} overlapping intervals",
            expected_value="0 overlapping intervals",
            failed_row_count=overlap_count,
            execution_timestamp=now,
        )
    )

    return results


def check_measure_validity(
    fact_sales_df: DataFrame,
    severity: str = "CRITICAL",
) -> list[QualityCheckResult]:
    """Verify arithmetic consistency and validity of numerical measures in fact_sales."""
    results: list[QualityCheckResult] = []
    now = datetime.now(timezone.utc)

    # 1. Positive quantities
    invalid_qty = fact_sales_df.filter(col("quantity").isNull() | (col("quantity") <= 0)).count()
    results.append(
        QualityCheckResult(
            check_name="measure_fact_sales_positive_quantity",
            table_name="fact_sales",
            check_type="MEASURE_VALIDITY",
            severity=severity,
            passed=invalid_qty == 0,
            observed_value=f"{invalid_qty} invalid quantities",
            expected_value="0 invalid quantities",
            failed_row_count=invalid_qty,
            execution_timestamp=now,
        )
    )

    # 2. Arithmetic equality: net_amount == gross_amount - discount_amount
    arithmetic_err = fact_sales_df.filter(
        col("net_amount") != (col("gross_amount") - col("discount_amount"))
    ).count()
    results.append(
        QualityCheckResult(
            check_name="measure_fact_sales_net_amount_arithmetic",
            table_name="fact_sales",
            check_type="MEASURE_VALIDITY",
            severity=severity,
            passed=arithmetic_err == 0,
            observed_value=f"{arithmetic_err} arithmetic mismatches",
            expected_value="0 arithmetic mismatches",
            failed_row_count=arithmetic_err,
            execution_timestamp=now,
        )
    )

    return results


def persist_quality_audit(
    spark: SparkSession,
    results: list[QualityCheckResult],
    quality_audit_path: Path | str,
) -> None:
    """Persist quality check results to the quality_audit Delta table."""
    path_str = str(quality_audit_path)
    records = [
        (
            r.check_name,
            r.table_name,
            r.check_type,
            r.severity,
            r.passed,
            r.observed_value,
            r.expected_value,
            r.failed_row_count,
            r.execution_timestamp,
        )
        for r in results
    ]
    df = spark.createDataFrame(records, schema=QUALITY_AUDIT_SCHEMA)
    df.write.format("delta").mode("append").save(path_str)
    logger.info("Persisted %d quality check results to %s", len(records), path_str)


def run_warehouse_quality_suite(
    spark: SparkSession,
    dim_customer_df: DataFrame,
    dim_product_df: DataFrame,
    dim_store_df: DataFrame,
    dim_date_df: DataFrame,
    fact_sales_df: DataFrame,
    fact_returns_df: DataFrame,
    quality_audit_path: Path | str | None = None,
    allow_unknown_keys: bool = False,
    raise_on_failure: bool = True,
) -> list[QualityCheckResult]:
    """
    Execute full enterprise quality gate suite across all warehouse dimensions and facts.

    Args:
        spark: Active SparkSession.
        dim_customer_df: Customer dimension DataFrame.
        dim_product_df: Product dimension DataFrame.
        dim_store_df: Store dimension DataFrame.
        dim_date_df: Date dimension DataFrame.
        fact_sales_df: Sales fact DataFrame.
        fact_returns_df: Returns fact DataFrame.
        quality_audit_path: Optional path for persisting audit results in Delta.
        allow_unknown_keys: If True, permits surrogate key 0 in fact tables for late-arriving testing.
        raise_on_failure: If True, raises WarehouseQualityGateError on any CRITICAL failure.

    Raises:
        WarehouseQualityGateError: If any CRITICAL quality gate check fails.
    """
    all_results: list[QualityCheckResult] = []

    # 1. Dimension Completeness & Uniqueness
    all_results.extend(check_completeness(dim_product_df, "dim_product", ["product_key", "product_id", "product_name"]))
    all_results.append(check_uniqueness(dim_product_df, "dim_product", ["product_key"]))
    all_results.append(check_uniqueness(dim_product_df, "dim_product", ["product_id"]))

    all_results.extend(check_completeness(dim_store_df, "dim_store", ["store_key", "store_id", "store_name"]))
    all_results.append(check_uniqueness(dim_store_df, "dim_store", ["store_key"]))
    all_results.append(check_uniqueness(dim_store_df, "dim_store", ["store_id"]))

    all_results.extend(check_completeness(dim_date_df, "dim_date", ["date_key", "full_date", "year", "month"]))
    all_results.append(check_uniqueness(dim_date_df, "dim_date", ["date_key"]))

    all_results.extend(check_completeness(dim_customer_df, "dim_customer", ["customer_key", "customer_id", "effective_from", "is_current"]))
    all_results.append(check_uniqueness(dim_customer_df, "dim_customer", ["customer_key"]))

    # 2. SCD Type 2 Invariants
    all_results.extend(check_scd2_invariants(dim_customer_df))

    # 3. Fact Table Grain & Completeness
    all_results.extend(check_completeness(fact_sales_df, "fact_sales", ["order_item_id", "customer_key", "product_key", "store_key", "order_date_key", "net_amount"]))
    all_results.append(check_uniqueness(fact_sales_df, "fact_sales", ["order_item_id"]))

    all_results.extend(check_completeness(fact_returns_df, "fact_returns", ["return_id", "order_item_id", "refund_amount"]))
    all_results.append(check_uniqueness(fact_returns_df, "fact_returns", ["return_id"]))

    # 4. Referential Integrity
    all_results.append(check_referential_integrity(fact_sales_df, "fact_sales", "product_key", dim_product_df, "product_key"))
    all_results.append(check_referential_integrity(fact_sales_df, "fact_sales", "store_key", dim_store_df, "store_key"))
    all_results.append(check_referential_integrity(fact_sales_df, "fact_sales", "customer_key", dim_customer_df, "customer_key"))
    all_results.append(check_referential_integrity(fact_sales_df, "fact_sales", "order_date_key", dim_date_df, "date_key"))

    # 5. Unknown Member (Key 0) Usage Policy Checks
    all_results.extend(
        check_unknown_member_usage(
            fact_sales_df,
            "fact_sales",
            ["customer_key", "product_key", "store_key", "order_date_key"],
            allow_unknown_keys=allow_unknown_keys,
            severity="CRITICAL",
        )
    )
    all_results.extend(
        check_unknown_member_usage(
            fact_returns_df,
            "fact_returns",
            ["customer_key", "product_key", "store_key", "return_date_key"],
            allow_unknown_keys=allow_unknown_keys,
            severity="CRITICAL",
        )
    )

    # 6. Measure Validity
    all_results.extend(check_measure_validity(fact_sales_df))

    # Persist audit if path provided
    if quality_audit_path:
        persist_quality_audit(spark, all_results, quality_audit_path)

    # Check for critical failures
    critical_failures = [r for r in all_results if not r.passed and r.severity == "CRITICAL"]
    if critical_failures and raise_on_failure:
        failure_msgs = "\n".join([f"  - [{r.check_name}] Observed: {r.observed_value} | Expected: {r.expected_value}" for r in critical_failures])
        raise WarehouseQualityGateError(
            f"Warehouse Quality Gate failed with {len(critical_failures)} CRITICAL violation(s):\n{failure_msgs}"
        )

    logger.info("Warehouse Quality Gate completed: %d checks evaluated, %d passed", len(all_results), sum(1 for r in all_results if r.passed))
    return all_results
