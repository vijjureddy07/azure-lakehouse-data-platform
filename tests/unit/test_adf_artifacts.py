"""
Unit tests for Azure Data Factory (ADF) JSON artifacts, parameterization contracts, and secret scanning (Module 2).
"""

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
ADF_DIR = REPO_ROOT / "adf"
INFRA_DIR = REPO_ROOT / "infra"
SAMPLE_DIR = REPO_ROOT / "data" / "sample"

EXPECTED_8_DATASETS = {
    "customers",
    "products",
    "stores",
    "employees",
    "orders",
    "order_items",
    "payments",
    "returns",
}


def test_adf_json_artifacts_valid_syntax():
    """Verify all ADF linked services, datasets, and pipeline definitions are valid JSON."""
    json_files = list(ADF_DIR.glob("**/*.json")) + list(INFRA_DIR.glob("**/*.json"))
    assert len(json_files) >= 7, f"Expected at least 7 JSON artifacts, found {len(json_files)}"

    for f in json_files:
        with open(f, "r", encoding="utf-8") as jf:
            try:
                data = json.load(jf)
                assert isinstance(data, dict), f"Root of {f.name} must be a JSON object"
                assert "name" in data or "$schema" in data, f"{f.name} missing name or $schema"
            except json.JSONDecodeError as e:
                raise AssertionError(f"Syntax error in JSON file {f}: {e}") from e


def test_adls_linked_service_managed_identity():
    """Verify ADLS Gen2 Linked Service uses AzureBlobFS with Managed Identity and zero keys."""
    ls_path = ADF_DIR / "linkedService" / "ls_adls_gen2.json"
    assert ls_path.exists(), "ls_adls_gen2.json must exist"

    with open(ls_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    props = data["properties"]
    assert props["type"] == "AzureBlobFS"
    assert "url" in props["typeProperties"]
    assert "parameters" in props
    assert "storageAccountName" in props["parameters"]

    # Security check: absolutely no access keys or SAS tokens allowed
    raw_text = json.dumps(data)
    assert "accountKey" not in raw_text
    assert "sasToken" not in raw_text
    assert "connectionString" not in raw_text
    assert "password" not in raw_text


def test_http_linked_service_configuration():
    """Verify HTTP Source Linked Service is parameterized for external raw file retrieval."""
    ls_path = ADF_DIR / "linkedService" / "ls_http_source.json"
    assert ls_path.exists(), "ls_http_source.json must exist"

    with open(ls_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    props = data["properties"]
    assert props["type"] == "HttpServer"
    assert "baseUrl" in props["parameters"]
    assert props["typeProperties"]["url"] == "@linkedService().baseUrl"


def test_datasets_parameterization():
    """Verify HTTP source dataset and ADLS Gen2 landing dataset parameterization."""
    # 1. HTTP Dataset
    http_ds_path = ADF_DIR / "dataset" / "ds_http_raw_file.json"
    with open(http_ds_path, "r", encoding="utf-8") as f:
        http_ds = json.load(f)

    assert http_ds["properties"]["type"] == "Binary"
    assert http_ds["properties"]["linkedServiceName"]["referenceName"] == "ls_http_source"
    assert "relativeUrl" in http_ds["properties"]["parameters"]

    # 2. ADLS Landing Dataset
    adls_ds_path = ADF_DIR / "dataset" / "ds_adls_landing_file.json"
    with open(adls_ds_path, "r", encoding="utf-8") as f:
        adls_ds = json.load(f)

    assert adls_ds["properties"]["type"] == "Binary"
    assert adls_ds["properties"]["linkedServiceName"]["referenceName"] == "ls_adls_gen2"
    adls_params = adls_ds["properties"]["parameters"]
    assert "storageAccountName" in adls_params
    assert "fileSystem" in adls_params
    assert "folderPath" in adls_params
    assert "fileName" in adls_params


def test_master_pipeline_orchestration_and_all_datasets():
    """Verify pl_master_retail_ingestion iterates across all 8 retail datasets dynamically."""
    master_path = ADF_DIR / "pipeline" / "pl_master_retail_ingestion.json"
    with open(master_path, "r", encoding="utf-8") as f:
        master = json.load(f)

    props = master["properties"]
    assert "parameters" in props
    assert "datasets_config" in props["parameters"]

    # Verify all 8 retail datasets configured in metadata array
    datasets_config = props["parameters"]["datasets_config"]["defaultValue"]
    configured_names = {d["dataset_name"] for d in datasets_config}
    assert configured_names == EXPECTED_8_DATASETS, f"Missing datasets in master pipeline metadata: {EXPECTED_8_DATASETS - configured_names}"

    # Verify ForEach and ExecutePipeline activities
    activities = props["activities"]
    assert len(activities) >= 1
    foreach_act = next((a for a in activities if a["type"] == "ForEach"), None)
    assert foreach_act is not None, "Master pipeline must contain a ForEach activity"

    inner_activities = foreach_act["typeProperties"]["activities"]
    exec_pipe_act = next((a for a in inner_activities if a["type"] == "ExecutePipeline"), None)
    assert exec_pipe_act is not None, "ForEach must contain an ExecutePipeline activity"
    assert exec_pipe_act["typeProperties"]["pipeline"]["referenceName"] == "pl_ingest_single_file"

    # Verify dynamic landing path expression
    dest_path_expr = exec_pipe_act["typeProperties"]["parameters"]["destination_folder_path"]
    assert "landing/retail/" in dest_path_expr
    assert "formatDateTime" in dest_path_expr
    assert "pipeline().RunId" in dest_path_expr


def test_child_pipeline_copy_activity():
    """Verify pl_ingest_single_file contains a Copy Activity from HTTP dataset to ADLS Gen2 dataset."""
    child_path = ADF_DIR / "pipeline" / "pl_ingest_single_file.json"
    with open(child_path, "r", encoding="utf-8") as f:
        child = json.load(f)

    props = child["properties"]
    copy_act = next((a for a in props["activities"] if a["type"] == "Copy"), None)
    assert copy_act is not None, "Child pipeline must contain a Copy activity"

    assert copy_act["inputs"][0]["referenceName"] == "ds_http_raw_file"
    assert copy_act["outputs"][0]["referenceName"] == "ds_adls_landing_file"

    # Verify required parameters exist
    child_params = props["parameters"]
    for required_param in [
        "dataset_name",
        "source_base_url",
        "source_relative_url",
        "source_file_name",
        "destination_file_name",
        "storage_account_name",
        "destination_container",
        "destination_folder_path",
    ]:
        assert required_param in child_params, f"Child pipeline missing parameter '{required_param}'"


def test_secret_scanning_across_repository():
    """Verify zero hardcoded secrets, connection strings, keys, or passwords in ADF, infra, and scripts."""
    forbidden_patterns = [
        re.compile(r"DefaultEndpointsProtocol=https;AccountName=", re.IGNORECASE),
        re.compile(r"AccountKey=[A-Za-z0-9+/=]{40,}", re.IGNORECASE),
        re.compile(r"SharedAccessSignature=sv=", re.IGNORECASE),
        re.compile(r"client_secret\s*=\s*['\"][A-Za-z0-9_-]{10,}['\"]", re.IGNORECASE),
        re.compile(r"-----BEGIN (RSA|EC|OPENSSH) PRIVATE KEY-----"),
    ]

    target_dirs = [ADF_DIR, INFRA_DIR, REPO_ROOT / "scripts", REPO_ROOT / "src"]
    for d in target_dirs:
        for f in d.rglob("*"):
            if f.is_file() and f.suffix in [".json", ".bicep", ".sh", ".py", ".md", ".yaml", ".yml"]:
                content = f.read_text(encoding="utf-8", errors="ignore")
                for pattern in forbidden_patterns:
                    assert not pattern.search(content), f"Potential hardcoded secret matching {pattern.pattern} found in {f}"


def test_sample_datasets_presence_and_integrity():
    """Verify all 8 sample retail files exist in data/sample/ and are non-empty."""
    for dataset_name, filename, fmt in [
        ("customers", "customers.csv", "csv"),
        ("products", "products.csv", "csv"),
        ("stores", "stores.csv", "csv"),
        ("employees", "employees.csv", "csv"),
        ("orders", "orders.csv", "csv"),
        ("order_items", "order_items.csv", "csv"),
        ("payments", "payments.json", "json"),
        ("returns", "returns.csv", "csv"),
    ]:
        fpath = SAMPLE_DIR / filename
        assert fpath.exists(), f"Sample file missing: {filename}"
        assert fpath.stat().st_size > 0, f"Sample file {filename} is empty"

        # Check content format
        if fmt == "csv":
            lines = fpath.read_text(encoding="utf-8").strip().splitlines()
            assert len(lines) >= 2, f"CSV file {filename} must have header and at least 1 data row"
        elif fmt == "json":
            lines = fpath.read_text(encoding="utf-8").strip().splitlines()
            assert len(lines) >= 1, f"JSON file {filename} must have at least 1 JSON line"
            # Verify each line is valid JSON
            for line in lines:
                json.loads(line)
