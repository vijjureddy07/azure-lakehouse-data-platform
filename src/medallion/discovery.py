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

class LandingDiscoveryError(RuntimeError):
    """Raised when cloud landing file discovery fails due to filesystem, auth, or configuration errors."""
    pass


class LandingPathError(ValueError):
    """Raised when a landing path violates the ADF partition contract."""
    pass


LandingPathContractError = LandingPathError  # Backward-compatibility alias


def sanitize_error_message(message: str) -> str:
    """Sanitize sensitive credentials, SAS tokens, and keys from error messages."""
    sanitized = re.sub(r"(sig|SharedAccessSignature|password|token|secret|account_key)=([^;&\s]+)", r"\1=REDACTED", message, flags=re.IGNORECASE)
    sanitized = re.sub(r"dapi[a-f0-9]{32}", "dapi[REDACTED]", sanitized)
    sanitized = re.sub(r"eyJ[a-zA-Z0-9_\-]{20,}\.[a-zA-Z0-9_\-]{20,}", "[REDACTED_JWT]", sanitized)
    return sanitized


def compute_ingestion_id(
    dataset_name: str,
    source_path: str,
    file_sha256: str | None = None,
) -> str:
    """
    Derive deterministic, immutable ingestion ID for a source file.

    Formula: sha256(dataset_name + normalized_source_path + file_sha256)
    """
    normalized_path = source_path.strip().replace("\\", "/").lower()
    payload = f"{dataset_name.strip().lower()}|{normalized_path}|{file_sha256 or ''}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


INGESTION_AUDIT_SCHEMA = StructType([
    StructField("ingestion_id", StringType(), False),
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

VALID_EXTENSIONS = (".csv", ".json", ".jsonl", ".parquet")


@dataclass
class LandingFileInfo:
    """Represents a discovered landing file with extracted lineage metadata."""
    dataset_name: str
    ingestion_date: str
    adf_run_id: str
    file_name: str
    source_path: str
    file_sha256: str | None  # 64-character SHA-256 for local files, None for cloud paths
    format: str  # 'csv' or 'json'
    ingestion_id: str = ""

    def __post_init__(self) -> None:
        if not self.ingestion_id:
            self.ingestion_id = compute_ingestion_id(
                self.dataset_name, self.source_path, self.file_sha256
            )


def compute_local_file_sha256(path: Path) -> str:
    """Compute SHA-256 hash of a local landing file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def parse_landing_path(
    path_str: str,
    strict: bool = True,
) -> tuple[str, str, str, str, str]:
    """
    Extract dataset_name, ingestion_date, run_id, file_name, format from a landing path string.

    Args:
        path_str: Path string to parse.
        strict: If True, enforce ADF landing contract:
                landing/retail/<dataset>/ingestion_date=<YYYY-MM-DD>/run_id=<RUN_ID>/<filename>
                If False, fall back to filename-derived defaults for test fixtures.

    Returns:
        tuple: (dataset_name, ingestion_date, run_id, filename, format)

    Raises:
        LandingPathError: If path is malformed and strict=True.
    """
    normalized = path_str.replace("\\", "/")
    match = LANDING_PATH_PATTERN.match(normalized)

    if match:
        ds = match.group("dataset").strip()
        ingestion_date = match.group("date").strip()
        run_id = match.group("run_id").strip()
        filename = match.group("filename").strip()

        # Validate calendar date syntax
        try:
            datetime.strptime(ingestion_date, "%Y-%m-%d")
        except ValueError as exc:
            if strict:
                raise LandingPathError(
                    f"Landing path contains invalid calendar date '{ingestion_date}': {path_str}"
                ) from exc

        # Validate non-empty fields
        if not ds or not run_id or not filename:
            if strict:
                raise LandingPathError(f"Landing path has empty dataset, run_id, or filename: {path_str}")

        # Validate expected extension
        if strict and not filename.lower().endswith(VALID_EXTENSIONS):
            raise LandingPathError(
                f"Landing file '{filename}' does not have an accepted extension {VALID_EXTENSIONS}: {path_str}"
            )
    else:
        if strict:
            raise LandingPathError(
                f"Malformed landing path violates cloud contract 'landing/retail/<dataset>/ingestion_date=<YYYY-MM-DD>/run_id=<RUN_ID>/<file>': {path_str}"
            )
        # Relaxed fallback for local test fixtures
        filename = normalized.split("/")[-1]
        stem = filename.split(".")[0]
        ds = stem
        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        run_id = "local-batch"

    fmt = "json" if filename.lower().endswith((".json", ".jsonl")) else "csv"
    return ds, ingestion_date, run_id, filename, fmt


def _get_cloud_filesystem_and_path(spark: SparkSession, root_str: str):
    """Retrieve Hadoop FileSystem and Path objects for a cloud URI."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(root_str)
    fs = jvm_path.getFileSystem(hadoop_conf)
    return fs, jvm_path


def discover_landing_files(
    spark: SparkSession,
    landing_root: Path | str,
    datasets: list[str] | None = None,
    strict_contract: bool | None = None,
) -> list[LandingFileInfo]:
    """
    Scan landing location (supporting both local paths and cloud ABFSS URIs)
    and extract metadata for all landing files.

    Strict contract enforcement is ALWAYS enabled for cloud paths (abfss://, wasbs://, s3://, etc.)
    and defaults to False for local filesystem directories unless explicitly requested.

    Raises:
        LandingDiscoveryError: If cloud directory does not exist or cloud/filesystem access fails.
        LandingPathError: If discovered files violate landing contract under strict mode.
    """
    discovered: list[LandingFileInfo] = []
    root_str = str(landing_root).rstrip("/")

    is_cloud = "://" in root_str
    use_strict = True if is_cloud else (strict_contract if strict_contract is not None else False)

    if not is_cloud:
        local_root = Path(root_str)
        if not local_root.exists():
            if strict_contract:
                raise LandingDiscoveryError(f"Local landing root directory does not exist: {root_str}")
            logger.warning("Local landing root does not exist: %s", root_str)
            return []

        for file_path in sorted(local_root.rglob("*")):
            if not file_path.is_file() or file_path.name.startswith((".", "_")):
                continue

            str_path = file_path.as_posix()
            ds, ingestion_date, run_id, filename, fmt = parse_landing_path(str_path, strict=use_strict)

            if datasets and ds not in datasets:
                continue

            sha256 = compute_local_file_sha256(file_path)
            ing_id = compute_ingestion_id(ds, str_path, sha256)
            discovered.append(
                LandingFileInfo(
                    dataset_name=ds,
                    ingestion_date=ingestion_date,
                    adf_run_id=run_id,
                    file_name=filename,
                    source_path=str_path,
                    file_sha256=sha256,
                    format=fmt,
                    ingestion_id=ing_id,
                )
            )
    else:
        # Cloud / Remote path discovery via Hadoop FileSystem / Spark
        try:
            fs, jvm_path = _get_cloud_filesystem_and_path(spark, root_str)

            if not fs.exists(jvm_path):
                raise LandingDiscoveryError(f"Cloud landing root directory does not exist: {root_str}")

            file_statuses = fs.listStatus(jvm_path)
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

                    ds, ingestion_date, run_id, filename, fmt = parse_landing_path(file_uri, strict=True)
                    if datasets and ds not in datasets:
                        continue

                    ing_id = compute_ingestion_id(ds, file_uri, None)
                    discovered.append(
                        LandingFileInfo(
                            dataset_name=ds,
                            ingestion_date=ingestion_date,
                            adf_run_id=run_id,
                            file_name=filename,
                            source_path=file_uri,
                            file_sha256=None,  # Nullable: cloud ingestion identity uses immutable source path
                            format=fmt,
                            ingestion_id=ing_id,
                        )
                    )
        except LandingDiscoveryError:
            raise
        except Exception as exc:
            sanitized_err = sanitize_error_message(str(exc))
            logger.error("Cloud filesystem scan failed for %s: %s", root_str, sanitized_err)
            raise LandingDiscoveryError(
                f"Cloud landing discovery failed for '{root_str}': {sanitized_err}"
            ) from exc

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


def get_ingested_ids(spark: SparkSession, audit_table_path: Path | str) -> set[str]:
    """Retrieve set of already ingested ingestion IDs from Delta audit log."""
    path_str = str(audit_table_path)
    if not DeltaTable.isDeltaTable(spark, path_str):
        return set()

    try:
        audit_df = spark.read.format("delta").load(path_str)
        if "ingestion_id" not in audit_df.columns:
            return set()
        rows = audit_df.filter(audit_df.status == "SUCCESS").select("ingestion_id").collect()
        return {r["ingestion_id"] for r in rows}
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not load ingestion audit IDs at %s: %s", path_str, e)
        return set()


def filter_uningested_files(
    spark: SparkSession,
    discovered_files: list[LandingFileInfo],
    audit_table_path: Path | str,
) -> list[LandingFileInfo]:
    """Filter out files that have already been recorded in the Delta audit log."""
    already_ingested_paths = get_ingested_paths(spark, audit_table_path)
    already_ingested_ids = get_ingested_ids(spark, audit_table_path)

    pending = [
        f for f in discovered_files
        if f.source_path not in already_ingested_paths and f.ingestion_id not in already_ingested_ids
    ]
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
    """
    Upsert newly ingested landing files into the Delta audit log table via Delta MERGE.

    Uses deterministic ingestion_id as primary key to prevent duplicate audit records.
    """
    if not ingested_files:
        return

    path_str = str(audit_table_path)
    now = datetime.now(timezone.utc)
    records = [
        (
            f.ingestion_id,
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

    if DeltaTable.isDeltaTable(spark, path_str):
        delta_audit = DeltaTable.forPath(spark, path_str)
        delta_audit.alias("target").merge(
            audit_df.alias("source"),
            "target.ingestion_id = source.ingestion_id",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        audit_df.write.format("delta").mode("append").save(path_str)

    logger.info("Upserted %d entries into ingestion audit log at %s", len(records), path_str)
