#!/usr/bin/env python3
"""
Azure Data Factory Artifact Deployment Helper (Module 2).

Extracts the inner '.properties' payload from version-controlled ARM/resource-style
JSON files (adf/linkedService, adf/dataset, adf/pipeline) and provides them to
Azure CLI create commands via managed temporary files.

Ensures Azure CLI commands receive the exact inner specification:
- az datafactory linked-service create --properties @<temp_properties.json>
- az datafactory dataset create --properties @<temp_properties.json>
- az datafactory pipeline create --pipeline @<temp_properties.json>
"""

import argparse
import json
import logging
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("adf_deployer")


def extract_adf_properties(json_file_path: Path) -> dict[str, Any]:
    """
    Extract the inner 'properties' payload from a checked-in ADF JSON artifact.

    If the JSON object has a top-level 'properties' key, that dictionary is returned.
    Otherwise, if it is already an unwrapped properties object, it is returned directly.
    """
    if not json_file_path.exists():
        raise FileNotFoundError(f"ADF artifact file not found: {json_file_path}")

    with open(json_file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise TypeError(f"Root of {json_file_path.name} must be a JSON object")

    if "properties" in data and isinstance(data["properties"], dict):
        return data["properties"]

    return data


def prepare_deployment_payloads(adf_dir: Path, target_dir: Path) -> dict[str, dict[str, Path]]:
    """
    Extract properties for all Linked Services, Datasets, and Pipelines into a target directory.

    Returns a nested dict: { 'linkedService': { name: path }, 'dataset': { name: path }, 'pipeline': { name: path } }
    """
    results: dict[str, dict[str, Path]] = {
        "linkedService": {},
        "dataset": {},
        "pipeline": {},
    }

    # 1. Linked Services
    ls_dir = adf_dir / "linkedService"
    if ls_dir.exists():
        ls_target = target_dir / "linkedService"
        ls_target.mkdir(parents=True, exist_ok=True)
        for f in sorted(ls_dir.glob("*.json")):
            props = extract_adf_properties(f)
            out_file = ls_target / f.name
            with open(out_file, "w", encoding="utf-8") as out:
                json.dump(props, out, indent=2)
            results["linkedService"][f.stem] = out_file

    # 2. Datasets
    ds_dir = adf_dir / "dataset"
    if ds_dir.exists():
        ds_target = target_dir / "dataset"
        ds_target.mkdir(parents=True, exist_ok=True)
        for f in sorted(ds_dir.glob("*.json")):
            props = extract_adf_properties(f)
            out_file = ds_target / f.name
            with open(out_file, "w", encoding="utf-8") as out:
                json.dump(props, out, indent=2)
            results["dataset"][f.stem] = out_file

    # 3. Pipelines (Deploy pl_ingest_single_file before pl_master_retail_ingestion)
    pipe_dir = adf_dir / "pipeline"
    if pipe_dir.exists():
        pipe_target = target_dir / "pipeline"
        pipe_target.mkdir(parents=True, exist_ok=True)
        # Child pipelines first, then master orchestration
        pipeline_order = ["pl_ingest_single_file.json", "pl_master_retail_ingestion.json"]
        for fname in pipeline_order:
            f = pipe_dir / fname
            if f.exists():
                props = extract_adf_properties(f)
                out_file = pipe_target / f.name
                with open(out_file, "w", encoding="utf-8") as out:
                    json.dump(props, out, indent=2)
                results["pipeline"][f.stem] = out_file

    return results


def run_az_command(cmd: list[str]) -> tuple[bool, str]:
    """Execute an Azure CLI command and return success status and output/stderr."""
    try:
        proc = subprocess.run(["az"] + cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            return False, proc.stderr.strip()
        return True, proc.stdout.strip()
    except FileNotFoundError:
        return False, "Azure CLI ('az') executable not found in PATH."


def deploy_adf_artifacts(
    resource_group: str,
    factory_name: str,
    adf_dir: Path,
) -> bool:
    """Deploy all ADF artifacts using extracted properties payloads via Azure CLI."""
    logger.info("--> Deploying ADF artifacts to Data Factory '%s' in Resource Group '%s'...", factory_name, resource_group)

    with tempfile.TemporaryDirectory(prefix="adf_deploy_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        payloads = prepare_deployment_payloads(adf_dir, temp_dir)

        # 1. Deploy Linked Services
        for name, payload_path in payloads["linkedService"].items():
            logger.info("Deploying Linked Service: %s...", name)
            cmd = [
                "datafactory",
                "linked-service",
                "create",
                "--factory-name",
                factory_name,
                "--resource-group",
                resource_group,
                "--name",
                name,
                "--properties",
                f"@{payload_path}",
                "--output",
                "table",
            ]
            ok, out = run_az_command(cmd)
            if not ok:
                logger.error("Failed to deploy Linked Service '%s': %s", name, out)
                return False

        # 2. Deploy Datasets
        for name, payload_path in payloads["dataset"].items():
            logger.info("Deploying Dataset: %s...", name)
            cmd = [
                "datafactory",
                "dataset",
                "create",
                "--factory-name",
                factory_name,
                "--resource-group",
                resource_group,
                "--name",
                name,
                "--properties",
                f"@{payload_path}",
                "--output",
                "table",
            ]
            ok, out = run_az_command(cmd)
            if not ok:
                logger.error("Failed to deploy Dataset '%s': %s", name, out)
                return False

        # 3. Deploy Pipelines
        for name, payload_path in payloads["pipeline"].items():
            logger.info("Deploying Pipeline: %s...", name)
            cmd = [
                "datafactory",
                "pipeline",
                "create",
                "--factory-name",
                factory_name,
                "--resource-group",
                resource_group,
                "--name",
                name,
                "--pipeline",
                f"@{payload_path}",
                "--output",
                "table",
            ]
            ok, out = run_az_command(cmd)
            if not ok:
                logger.error("Failed to deploy Pipeline '%s': %s", name, out)
                return False

    logger.info("All ADF artifacts deployed successfully to %s!", factory_name)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Deploy ADF JSON artifacts with extracted properties payloads")
    parser.add_argument("--resource-group", required=True, help="Azure Resource Group name")
    parser.add_argument("--factory-name", required=True, help="Azure Data Factory name")
    parser.add_argument("--adf-dir", default=None, help="Path to 'adf' directory containing JSON artifacts")
    parser.add_argument("--export-only", default=None, help="Export extracted properties payloads to a target directory without deploying")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    adf_dir = Path(args.adf_dir) if args.adf_dir else (repo_root / "adf")

    if args.export_only:
        export_path = Path(args.export_only)
        export_path.mkdir(parents=True, exist_ok=True)
        payloads = prepare_deployment_payloads(adf_dir, export_path)
        logger.info("Exported extracted properties to %s: %s", export_path, payloads)
        return 0

    success = deploy_adf_artifacts(args.resource_group, args.factory_name, adf_dir)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
