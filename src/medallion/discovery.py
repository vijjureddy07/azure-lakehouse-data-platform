"""
Landing File Discovery & Ingestion Audit Tracker.

Provides incremental-safe discovery of landing files conforming to the Module 2 ADF layout:
landing/retail/<dataset_name>/ingestion_date=<yyyy-MM-dd>/run_id=<run_id>/<file_name>

Supports both local filesystem paths and cloud ABFSS storage locations.
Maintains an immutable Delta-based ingestion audit log (_ingestion_audit) to track
processed source paths, timestamps, file hashes, and ADF execution IDs, guaranteeing
that rerun cycles do not duplicate data in Bronze.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from delta.tables import DeltaTable
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

INGESTION_AUDIT_SCHEMA = StructType([
    StructField("source_path", StringType(), False),
    StructField("dataset_name", StringType(), False),
    StructField("ingestion_date", StringType(), False),
    StructField("adf_run_id", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("file_sha256", StringType(), True),  # Nullable: computed locally, None for cloud paths
    StructField("status", StringType(), False),
    StructField("ingested_at", TimestampType(), False),
])

LANDING_PATH_PATTERN = re.compile(
    r".*/retail/(?P<dataset>[^/]+)/ingestion_date=(?P<date>\d{4}-\d{2}-\d{2})/run_id=(?P<run_id>[^/]+)/(?P<filename>[^/]+)$"
)


@dataclass
class LandingFileInfo:
    """Represents a discovered landing file with extracted lineage metadata."""
    dataset_name: str
    ingestion_date: str
    adf_run_id: str
    file_name: str
    source_path: str
    file_sha256: str | None  # 64-character SHA-256 for local files, None for cloud paths where remote calculation is skipped
    format: str  # 'csv' or 'json'


def compute_local_file_sha256(path: Path) -> str:
    """Compute SHA-256 hash of a local landing file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def parse_landing_path(path_str: str) -> tuple[str, str, str, str, str]:
    """
    Extract dataset_name, ingestion_date, run_id, file_name, format from a landing path string.
    """
    match = LANDING_PATH_PATTERN.match(path_str)
    if match:
        ds = match.group("dataset")
        ingestion_date = match.group("date")
        run_id = match.group("run_id")
        filename = match.group("filename")
    else:
        # Fallback for flat mock/test paths
        filename = path_str.split("/")[-1]
        stem = filename.split(".")[0]
        ds = stem
        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        run_id = "local-batch"

    fmt = "json" if filename.lower().endswith((".json", ".jsonl")) else "csv"
    return ds, ingestion_date, run_id, filename, fmt


def discover_landing_files(
    spark: SparkSession,
    landing_root: Path | str,
    datasets: list[str] | None = None,
) -> list[LandingFileInfo]:
    """
    Scan landing location (supporting both local paths and cloud ABFSS URIs)
    and extract metadata for all landing files.
    """
    discovered: list[LandingFileInfo] = []
    root_str = str(landing_root).rstrip("/")

    is_local_path = False
    try:
        p = Path(root_str)
        if "://" not in root_str and p.exists():
            is_local_path = True
    except Exception:  # noqa: BLE001
        is_local_path = False

    if is_local_path:
        local_root = Path(root_str)
        for file_path in local_root.rglob("*"):
            if not file_path.is_file() or file_path.name.startswith((".", "_")):
                continue

            str_path = file_path.as_posix()
            ds, ingestion_date, run_id, filename, fmt = parse_landing_path(str_path)

            if datasets and ds not in datasets:
                continue

            sha256 = compute_local_file_sha256(file_path)
            discovered.append(
                LandingFileInfo(
                    dataset_name=ds,
                    ingestion_date=ingestion_date,
                    adf_run_id=run_id,
                    file_name=filename,
                    source_path=str_path,
                    file_sha256=sha256,
                    format=fmt,
                )
            )
    else:
        # Cloud / Remote path discovery via Hadoop FileSystem / Spark
        try:
            hadoop_conf = spark._jsc.hadoopConfiguration()
            jvm_path = spark._jvm.org.apache.hadoop.fs.Path(root_str)
            fs = jvm_path.getFileSystem(hadoop_conf)

            if fs.exists(jvm_path):
                file_statuses = fs.listStatus(jvm_path)
                # Recursively discover files if needed
                queue = list(file_statuses)
                while queue:
                    status = queue.pop(0)
                    if status.isDirectory():
                        for sub_status in fs.listStatus(status.getPath()):
                            queue.append(sub_status)
                    else:
                        file_uri = status.getPath().toString()
                        fname = status.getPath().getName()
                        if fname.startswith((".", "_")):
                            continue

                        ds, ingestion_date, run_id, filename, fmt = parse_landing_path(file_uri)
                        if datasets and ds not in datasets:
                            continue

                        discovered.append(
                            LandingFileInfo(
                                dataset_name=ds,
                                ingestion_date=ingestion_date,
                                adf_run_id=run_id,
                                file_name=filename,
                                source_path=file_uri,
                                file_sha256=None,  # Nullable: cloud ingestion identity uses immutable source path
                                format=fmt,
                            )
                        )
        except Exception as e:  # noqa: BLE001
            logger.warning("Cloud path scan encountered exception for %s: %s", root_str, e)

    logger.info("Discovered %d landing files in %s", len(discovered), root_str)
    return discovered


def get_ingested_paths(spark: SparkSession, audit_table_path: Path | str) -> set[str]:
    """Retrieve set of already ingested source paths from Delta audit log."""
    path_str = str(audit_table_path)
    if not DeltaTable.isDeltaTable(spark, path_str):
        return set()

    try:
        audit_df = spark.read.format("delta").load(path_str)
        rows = audit_df.filter(audit_df.status == "SUCCESS").select("source_path").collect()
        return {r["source_path"] for r in rows}
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not load ingestion audit table at %s: %s", path_str, e)
        return set()


def filter_uningested_files(
    spark: SparkSession,
    discovered_files: list[LandingFileInfo],
    audit_table_path: Path | str,
) -> list[LandingFileInfo]:
    """Filter out files that have already been recorded in the Delta audit log."""
    already_ingested = get_ingested_paths(spark, audit_table_path)
    pending = [f for f in discovered_files if f.source_path not in already_ingested]
    logger.info(
        "Filtered landing files: %d total, %d already ingested, %d pending ingestion",
        len(discovered_files),
        len(discovered_files) - len(pending),
        len(pending),
    )
    return pending


def record_ingested_files(
    spark: SparkSession,
    ingested_files: list[LandingFileInfo],
    audit_table_path: Path | str,
    status: str = "SUCCESS",
) -> None:
    """Append newly ingested landing files into the Delta audit log table."""
    if not ingested_files:
        return

    path_str = str(audit_table_path)
    now = datetime.now(timezone.utc)
    records = [
        (
            f.source_path,
            f.dataset_name,
            f.ingestion_date,
            f.adf_run_id,
            f.file_name,
            f.file_sha256,
            status,
            now,
        )
        for f in ingested_files
    ]

    audit_df = spark.createDataFrame(records, schema=INGESTION_AUDIT_SCHEMA)
    audit_df.write.format("delta").mode("append").save(path_str)
    logger.info("Recorded %d entries in ingestion audit log at %s", len(records), path_str)
