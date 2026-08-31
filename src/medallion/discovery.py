"""
Landing File Discovery & Ingestion Audit Tracker.

Provides incremental-safe discovery of landing files conforming to the Module 2 ADF layout:
landing/retail/<dataset_name>/ingestion_date=<yyyy-MM-dd>/run_id=<run_id>/<file_name>

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
    StructField("file_sha256", StringType(), False),
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
    file_path: Path
    file_sha256: str
    format: str  # 'csv' or 'json'


def compute_file_sha256(path: Path) -> str:
    """Compute SHA-256 hash of a local landing file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def discover_landing_files(
    landing_root: Path,
    datasets: list[str] | None = None,
) -> list[LandingFileInfo]:
    """
    Recursively scan landing directory and extract metadata for all landing files.

    Expected directory structure:
    <landing_root>/retail/<dataset>/ingestion_date=<yyyy-MM-dd>/run_id=<run_id>/<filename>
    or fallback direct search if flat folder.
    """
    discovered: list[LandingFileInfo] = []
    if not landing_root.exists():
        logger.warning("Landing root directory does not exist: %s", landing_root)
        return discovered

    for file_path in landing_root.rglob("*"):
        if not file_path.is_file() or file_path.name.startswith((".", "_")):
            continue

        str_path = file_path.as_posix()
        match = LANDING_PATH_PATTERN.match(str_path)

        if match:
            ds = match.group("dataset")
            ingestion_date = match.group("date")
            run_id = match.group("run_id")
            filename = match.group("filename")
        else:
            # Fallback for flat mock/test directories
            filename = file_path.name
            stem = file_path.stem.split(".")[0]
            ds = stem
            ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            run_id = "local-batch"

        if datasets and ds not in datasets:
            continue

        fmt = "json" if file_path.suffix.lower() == ".json" else "csv"
        sha256 = compute_file_sha256(file_path)

        discovered.append(
            LandingFileInfo(
                dataset_name=ds,
                ingestion_date=ingestion_date,
                adf_run_id=run_id,
                file_name=filename,
                file_path=file_path,
                file_sha256=sha256,
                format=fmt,
            )
        )

    logger.info("Discovered %d landing files in %s", len(discovered), landing_root)
    return discovered


def get_ingested_paths(spark: SparkSession, audit_table_path: Path) -> set[str]:
    """Retrieve set of already ingested source paths from Delta audit log."""
    if not audit_table_path.exists():
        return set()

    try:
        audit_df = spark.read.format("delta").load(str(audit_table_path))
        rows = audit_df.filter(audit_df.status == "SUCCESS").select("source_path").collect()
        return {r["source_path"] for r in rows}
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not load ingestion audit table at %s: %s", audit_table_path, e)
        return set()


def filter_uningested_files(
    spark: SparkSession,
    discovered_files: list[LandingFileInfo],
    audit_table_path: Path,
) -> list[LandingFileInfo]:
    """Filter out files that have already been recorded in the Delta audit log."""
    already_ingested = get_ingested_paths(spark, audit_table_path)
    pending = [f for f in discovered_files if f.file_path.as_posix() not in already_ingested]
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
    audit_table_path: Path,
    status: str = "SUCCESS",
) -> None:
    """Append newly ingested landing files into the Delta audit log table."""
    if not ingested_files:
        return

    now = datetime.now(timezone.utc)
    records = [
        (
            f.file_path.as_posix(),
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
    audit_df.write.format("delta").mode("append").save(str(audit_table_path))
    logger.info("Recorded %d entries in ingestion audit log at %s", len(records), audit_table_path)
