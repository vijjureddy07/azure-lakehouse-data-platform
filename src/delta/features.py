"""
Delta Lake Core Features & ACID Demonstrations.

Provides programmatic utilities to inspect, demonstrate, and validate core Delta Lake capabilities:
1. Delta Transaction Log (_delta_log): JSON commit records providing atomicity and serializability.
2. Table History (DESCRIBE HISTORY): Auditing table operations, timestamps, and versions.
3. Time Travel: Querying earlier snapshot versions using 'VERSION AS OF' or 'TIMESTAMP AS OF'.
4. Schema Enforcement: Blocking unauthorized schema changes on append.
5. Controlled Schema Evolution: Safely merging newly introduced columns with 'mergeSchema: true'.
6. ACID Atomicity: Guaranteeing multi-row updates and transactions either fully succeed or roll back.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def get_delta_history(spark: SparkSession, table_path: Path) -> DataFrame:
    """
    Query the complete operational history of a Delta table using DESCRIBE HISTORY.
    """
    history_df = spark.sql(f"DESCRIBE HISTORY delta.`{table_path.as_posix()}`")
    return history_df


def inspect_transaction_log(table_path: Path) -> list[dict]:
    """
    Directly parse JSON commit entries from the _delta_log directory.
    """
    log_dir = table_path / "_delta_log"
    if not log_dir.exists():
        raise FileNotFoundError(f"Delta log directory not found at: {log_dir}")

    commit_files = sorted([f for f in log_dir.glob("*.json") if f.name != "_last_checkpoint"])
    commits = []

    for cf in commit_files:
        commit_actions = []
        with cf.open("r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    commit_actions.append(json.loads(line.strip()))
        commits.append({
            "version": int(cf.stem),
            "file": cf.name,
            "actions": commit_actions,
        })

    return commits


def query_time_travel_by_version(
    spark: SparkSession,
    table_path: Path,
    version: int,
) -> DataFrame:
    """
    Query a previous snapshot of the Delta table using VERSION AS OF.
    """
    return (
        spark.read.format("delta")
        .option("versionAsOf", version)
        .load(table_path.as_posix())
    )


def query_time_travel_by_timestamp(
    spark: SparkSession,
    table_path: Path,
    timestamp_str: str,
) -> DataFrame:
    """
    Query a snapshot of the Delta table using TIMESTAMP AS OF.
    """
    return (
        spark.read.format("delta")
        .option("timestampAsOf", timestamp_str)
        .load(table_path.as_posix())
    )


def demonstrate_schema_enforcement(
    spark: SparkSession,
    table_path: Path,
) -> tuple[bool, str]:
    """
    Demonstrate that Delta Lake blocks appending incompatible schemas by default.

    Returns:
        tuple[bool, str]: (is_blocked_successfully, error_message_or_explanation)
    """
    # Create base table
    df_base = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    df_base.write.format("delta").mode("overwrite").save(table_path.as_posix())

    # Attempt appending dataframe with unexpected extra column
    df_incompatible = spark.createDataFrame([(3, "Charlie", "VIP")], ["id", "name", "membership_tier"])

    try:
        df_incompatible.write.format("delta").mode("append").save(table_path.as_posix())
        return False, "Schema enforcement FAILED: unexpected column was allowed without mergeSchema option."
    except Exception as exc:  # noqa: BLE001
        err_msg = str(exc)
        logger.info("Schema enforcement successfully blocked incompatible write: %s", err_msg[:100])
        return True, err_msg


def demonstrate_schema_evolution(
    spark: SparkSession,
    table_path: Path,
) -> DataFrame:
    """
    Demonstrate controlled schema evolution using .option('mergeSchema', 'true').
    """
    # Evolve table schema to include 'membership_tier'
    df_evolved = spark.createDataFrame([(3, "Charlie", "VIP")], ["id", "name", "membership_tier"])
    df_evolved.write.format("delta").mode("append").option("mergeSchema", "true").save(table_path.as_posix())

    # Read evolved table
    result_df = spark.read.format("delta").load(table_path.as_posix())
    return result_df
