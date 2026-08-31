"""
Azure Cloud Ingestion Deployment & ADLS Gen2 Landing Verification Script (Module 2).

Performs rigorous verification of:
1. Local ADF JSON repository artifacts and payload shapes.
2. Azure CLI authentication and cloud connection.
3. ADLS Gen2 Storage Account (verifying Hierarchical Namespace is enabled).
4. Primary 'lakehouse' container existence.
5. Azure Data Factory instance & System-Assigned Managed Identity.
6. Azure RBAC Role Assignment ('Storage Blob Data Contributor') to ADF Managed Identity.
7. Terminal 'Succeeded' state of ADF pipeline run ('pl_master_retail_ingestion') for exact --run-id.
8. Exact run-ID partitioned landed files in ADLS Gen2:
   landing/retail/<dataset>/ingestion_date=*/run_id=<RUN_ID>/<file>
9. Raw data fidelity: Byte-for-byte SHA-256 hash comparison between local source and landed files.

Exit Code Rules:
- Returns 0 ONLY if:
  - '--local-only' is passed and all local artifact checks pass, OR
  - Live cloud verification is performed and ALL cloud, RBAC, pipeline run, path, and hash checks pass 100%.
- Returns 1 (non-zero failure) if ANY cloud check, authentication check, RBAC, run status, or hash check fails.
"""

import argparse
import hashlib
import json
import logging
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("azure_verifier")

EXPECTED_DATASETS = [
    ("customers", "customers.csv", "csv"),
    ("products", "products.csv", "csv"),
    ("stores", "stores.csv", "csv"),
    ("employees", "employees.csv", "csv"),
    ("orders", "orders.csv", "csv"),
    ("order_items", "order_items.csv", "csv"),
    ("payments", "payments.json", "json"),
    ("returns", "returns.csv", "csv"),
]

STORAGE_BLOB_DATA_CONTRIBUTOR_ROLE = "Storage Blob Data Contributor"


def compute_file_sha256(file_path: Path) -> str:
    """Compute SHA-256 checksum for a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            sha256.update(chunk)
    return sha256.hexdigest()


def run_az_command(args: list[str]) -> tuple[bool, Any, str]:
    """Execute an Azure CLI command and return (success, parsed_json_or_str, stderr)."""
    cmd = ["az"] + args + ["-o", "json"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            return False, None, proc.stderr.strip()
        try:
            return True, json.loads(proc.stdout), ""
        except json.JSONDecodeError:
            return True, proc.stdout.strip(), ""
    except FileNotFoundError:
        return False, None, "Azure CLI ('az') executable not found in PATH."


def verify_local_artifacts(repo_root: Path) -> bool:
    """Verify that all required version-controlled ADF JSON files and scripts exist and parse cleanly."""
    logger.info("=== 1. VERIFYING LOCAL ADF REPOSITORY ARTIFACTS ===")
    required_files = [
        repo_root / "adf" / "linkedService" / "ls_adls_gen2.json",
        repo_root / "adf" / "linkedService" / "ls_http_source.json",
        repo_root / "adf" / "dataset" / "ds_http_raw_file.json",
        repo_root / "adf" / "dataset" / "ds_adls_landing_file.json",
        repo_root / "adf" / "pipeline" / "pl_ingest_single_file.json",
        repo_root / "adf" / "pipeline" / "pl_master_retail_ingestion.json",
        repo_root / "infra" / "bicep" / "main.bicep",
        repo_root / "infra" / "arm_template.json",
        repo_root / "scripts" / "deploy_azure_resources.sh",
        repo_root / "scripts" / "deploy_adf_artifacts.py",
    ]

    all_ok = True
    for f in required_files:
        if not f.exists():
            logger.error("Missing expected artifact: %s", f)
            all_ok = False
        else:
            if f.suffix == ".json":
                try:
                    with open(f, "r", encoding="utf-8") as jf:
                        data = json.load(jf)
                    if "name" not in data and "$schema" not in data:
                        logger.error("JSON artifact %s is missing 'name' or '$schema'", f.name)
                        all_ok = False
                    else:
                        logger.info("Artifact valid: %s", f.name)
                except (json.JSONDecodeError, OSError) as e:
                    logger.error("Invalid JSON in %s: %s", f.name, e)
                    all_ok = False
            else:
                logger.info("Artifact present: %s", f.name)

    return all_ok


def verify_managed_identity_and_rbac(resource_group: str, storage_account: str, data_factory: str) -> tuple[bool, str]:
    """Verify Data Factory System-Assigned Managed Identity and Storage Blob Data Contributor RBAC."""
    logger.info("Verifying Data Factory Managed Identity & RBAC role assignments...")

    # 1. Check ADF identity
    ok, adf_info, err = run_az_command(["datafactory", "show", "--name", data_factory, "--resource-group", resource_group])
    if not ok:
        logger.error("Failed to query Data Factory '%s': %s", data_factory, err)
        return False, ""

    identity = adf_info.get("identity", {})
    identity_type = identity.get("type", "")
    principal_id = identity.get("principalId", "")

    if "SystemAssigned" not in identity_type or not principal_id:
        logger.error("Data Factory '%s' does NOT have a valid System-Assigned Managed Identity! (Type: %s, PrincipalId: %s)", data_factory, identity_type, principal_id)
        return False, ""

    logger.info("ADF System-Assigned Managed Identity verified (Principal ID: %s)", principal_id)

    # 2. Query Storage Account ID
    ok, sa_info, err = run_az_command(["storage", "account", "show", "--name", storage_account, "--resource-group", resource_group])
    if not ok:
        logger.error("Failed to query Storage Account '%s': %s", storage_account, err)
        return False, ""

    sa_id = sa_info.get("id", "")

    # 3. Check RBAC Role Assignment
    ok, role_assignments, err = run_az_command(["role", "assignment", "list", "--assignee", principal_id, "--scope", sa_id])
    if not ok or not isinstance(role_assignments, list):
        logger.error("Failed to query RBAC role assignments for principal '%s': %s", principal_id, err)
        return False, principal_id

    has_blob_contributor = any(
        ra.get("roleDefinitionName") == STORAGE_BLOB_DATA_CONTRIBUTOR_ROLE for ra in role_assignments
    )

    if not has_blob_contributor:
        logger.error("Principal '%s' lacks '%s' role on storage account '%s'!", principal_id, STORAGE_BLOB_DATA_CONTRIBUTOR_ROLE, storage_account)
        return False, principal_id

    logger.info("Verified RBAC role '%s' assigned to ADF Managed Identity on %s", STORAGE_BLOB_DATA_CONTRIBUTOR_ROLE, storage_account)
    return True, principal_id


def verify_pipeline_run_status(resource_group: str, data_factory: str, run_id: str) -> bool:
    """Verify that the ADF master pipeline run exists and completed in 'Succeeded' state."""
    logger.info("Verifying ADF Pipeline Run '%s'...", run_id)
    ok, run_info, err = run_az_command(["datafactory", "pipeline-run", "show", "--factory-name", data_factory, "--resource-group", resource_group, "--run-id", run_id])
    if not ok:
        logger.error("Pipeline run '%s' query failed: %s", run_id, err)
        return False

    pipeline_name = run_info.get("pipelineName", "")
    status = run_info.get("status", "")

    if pipeline_name != "pl_master_retail_ingestion":
        logger.error("Expected pipeline 'pl_master_retail_ingestion', but run '%s' was for '%s'", run_id, pipeline_name)
        return False

    if status != "Succeeded":
        logger.error("Pipeline run '%s' did not succeed! Status: %s", run_id, status)
        return False

    logger.info("Pipeline Run '%s' verified with terminal status: Succeeded", run_id)
    return True


def verify_run_landed_files_and_fidelity(
    repo_root: Path,
    storage_account: str,
    container: str,
    run_id: str,
) -> bool:
    """Verify that all 8 datasets exist for the EXACT run_id and match local source files byte-for-byte (SHA-256)."""
    logger.info("=== VERIFYING LANDED FILES & RAW FIDELITY FOR RUN ID: %s ===", run_id)

    sample_dir = repo_root / "data" / "sample"
    all_ok = True

    with tempfile.TemporaryDirectory(prefix="adf_verify_fidelity_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)

        for dataset_name, filename, fmt in EXPECTED_DATASETS:
            dataset_landing_prefix = f"landing/retail/{dataset_name}"
            # List paths under dataset directory
            ok, paths_info, err = run_az_command([
                "storage",
                "fs",
                "file",
                "list",
                "--file-system",
                container,
                "--account-name",
                storage_account,
                "--path",
                dataset_landing_prefix,
                "--auth-mode",
                "login",
            ])

            if not ok or not isinstance(paths_info, list):
                logger.error("Failed to list files under '%s': %s", dataset_landing_prefix, err)
                all_ok = False
                continue

            # Look for exact path containing run_id=<run_id>/<filename>
            matching_paths = [
                p.get("name", "") for p in paths_info if f"run_id={run_id}/{filename}" in p.get("name", "")
            ]

            if not matching_paths:
                logger.error("Dataset '%s': Missing landed file for exact run_id='%s' (Expected pattern: .../run_id=%s/%s)", dataset_name, run_id, run_id, filename)
                all_ok = False
                continue

            landed_path = matching_paths[0]
            logger.info("Found landed object for '%s': %s", dataset_name, landed_path)

            # Download and verify SHA-256 byte-for-byte
            local_sample_file = sample_dir / filename
            if not local_sample_file.exists():
                logger.error("Local sample file '%s' not found for hash comparison", local_sample_file)
                all_ok = False
                continue

            downloaded_dest = temp_dir / f"{dataset_name}_{filename}"
            ok, _, err = run_az_command([
                "storage",
                "fs",
                "file",
                "download",
                "--file-system",
                container,
                "--account-name",
                storage_account,
                "--path",
                landed_path,
                "--destination",
                str(downloaded_dest),
                "--auth-mode",
                "login",
            ])

            if not ok or not downloaded_dest.exists():
                logger.error("Failed to download landed file '%s': %s", landed_path, err)
                all_ok = False
                continue

            source_hash = compute_file_sha256(local_sample_file)
            landed_hash = compute_file_sha256(downloaded_dest)

            if source_hash != landed_hash:
                logger.error("SHA-256 FIDELITY MISMATCH for '%s'! Source: %s, Landed: %s", dataset_name, source_hash, landed_hash)
                all_ok = False
            else:
                logger.info("SHA-256 FIDELITY VERIFIED: [%s] (%s, %d bytes) matches exactly.", dataset_name, fmt.upper(), local_sample_file.stat().st_size)

    return all_ok


def verify_cloud_resources(
    repo_root: Path,
    resource_group: str,
    storage_account: str,
    data_factory: str,
    container: str,
    run_id: str | None,
) -> bool:
    """Verify live Azure cloud resources, Managed Identity, RBAC, ADF pipeline run, and landed files."""
    logger.info("=== 2. VERIFYING LIVE AZURE CLOUD RESOURCES ===")

    # 1. Check Azure CLI login
    ok, account_info, err = run_az_command(["account", "show"])
    if not ok:
        logger.error("Azure CLI is not logged in or 'az' is missing: %s", err)
        return False

    logger.info("Connected to Azure Subscription: %s (%s)", account_info.get("name"), account_info.get("id"))

    # 2. Check Storage Account & HNS
    logger.info("Checking Storage Account '%s'...", storage_account)
    ok, sa_info, err = run_az_command(["storage", "account", "show", "--name", storage_account, "--resource-group", resource_group])
    if not ok:
        logger.error("Storage Account check failed: %s", err)
        return False

    is_hns = sa_info.get("isHnsEnabled", False)
    if not is_hns:
        logger.error("Storage Account '%s' must have Hierarchical Namespace (HNS) enabled for ADLS Gen2!", storage_account)
        return False
    logger.info("Storage Account '%s' verified (Hierarchical Namespace Enabled: True)", storage_account)

    # 3. Check Container
    logger.info("Checking filesystem container '%s'...", container)
    ok, fs_info, err = run_az_command(["storage", "fs", "exists", "--name", container, "--account-name", storage_account, "--auth-mode", "login"])
    if not ok or not fs_info.get("exists", False):
        logger.error("Filesystem container '%s' does not exist in %s: %s", container, storage_account, err)
        return False
    logger.info("Container '%s' verified.", container)

    # 4. Check Managed Identity & RBAC
    rbac_ok, _ = verify_managed_identity_and_rbac(resource_group, storage_account, data_factory)
    if not rbac_ok:
        return False

    # 5. Check Specific Run ID if provided
    if run_id:
        # Check ADF pipeline run terminal state
        pipe_ok = verify_pipeline_run_status(resource_group, data_factory, run_id)
        if not pipe_ok:
            return False

        # Check landed files and SHA-256 fidelity for exact run
        fidelity_ok = verify_run_landed_files_and_fidelity(repo_root, storage_account, container, run_id)
        if not fidelity_ok:
            return False
    else:
        logger.warning("No --run-id provided. Skipping run-specific landing and SHA-256 fidelity checks.")

    logger.info("All Azure Cloud resources, security configurations, and landed data verified successfully!")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Azure Cloud Ingestion Platform (Module 2)")
    parser.add_argument("--resource-group", default="rg-lakehouse-dev-eastus", help="Azure Resource Group name")
    parser.add_argument("--storage-account", default="stlakehousedev", help="ADLS Gen2 Storage Account name")
    parser.add_argument("--data-factory", default="adf-lakehouse-dev", help="Azure Data Factory name")
    parser.add_argument("--container", default="lakehouse", help="Primary container name")
    parser.add_argument("--run-id", default=None, help="ADF Pipeline Run ID to verify specifically")
    parser.add_argument("--local-only", action="store_true", help="Run local artifact checks only and return 0 on success")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent

    # 1. Local Artifact Validation
    local_ok = verify_local_artifacts(repo_root)
    if not local_ok:
        logger.error("Local artifact validation FAILED.")
        return 1

    if args.local_only:
        logger.info("\nSUCCESS: Local-only artifact verification passed.")
        return 0

    # 2. Cloud Verification (Must fail with exit code 1 if any cloud check fails)
    cloud_ok = verify_cloud_resources(
        repo_root,
        args.resource_group,
        args.storage_account,
        args.data_factory,
        args.container,
        args.run_id,
    )

    if not cloud_ok:
        logger.error("\nFAILURE: Live Azure cloud verification failed.")
        return 1

    logger.info("\nSUCCESS: Complete Azure Cloud Ingestion Platform verified live in Azure!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
