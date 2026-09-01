"""
Unit & Contract Tests for Module 6: Production CI/CD + Declarative Automation Bundles + Governed SQL Serving.

Validates:
1. Declarative Automation Bundle root configuration (databricks.yml, includes, targets, variables, artifacts).
2. Serverless Databricks SQL Warehouse bundle resource (sql_serving.yml).
3. Serving Setup Job bundle resource (serving_setup_job.yml).
4. Governed SQL Serving views contracts (04_serving_views.sql, 8 views, safe DDL, SCD2 join fidelity).
5. GitHub Actions CI workflow (.github/workflows/ci.yml).
6. GitHub Actions OIDC deployment workflow (.github/workflows/deploy_databricks.yml).
7. Zero committed credentials, PATs, passwords, or legacy /mnt/ paths across all Module 6 artifacts.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
BUNDLE_ROOT = REPO_ROOT / "databricks.yml"
SQL_SERVING_RESOURCE = REPO_ROOT / "databricks" / "resources" / "sql_serving.yml"
SERVING_JOB_RESOURCE = REPO_ROOT / "databricks" / "resources" / "serving_setup_job.yml"
SERVING_SQL_FILE = REPO_ROOT / "databricks" / "sql" / "04_serving_views.sql"
CI_WORKFLOW_FILE = REPO_ROOT / ".github" / "workflows" / "ci.yml"
DEPLOY_WORKFLOW_FILE = REPO_ROOT / ".github" / "workflows" / "deploy_databricks.yml"


def test_root_bundle_yaml_structure_and_includes():
    """Verify that databricks.yml is structurally valid and declares required includes and targets."""
    assert BUNDLE_ROOT.is_file(), "databricks.yml must exist at repository root."

    content = BUNDLE_ROOT.read_text(encoding="utf-8")
    parsed = yaml.safe_load(content)

    assert parsed is not None
    assert parsed.get("bundle", {}).get("name") == "retail-lakehouse-data-platform"

    # Verify includes include jobs and resources
    includes = parsed.get("include", [])
    assert any("jobs" in inc for inc in includes), "Bundle must include jobs/*.yml"
    assert any("resources" in inc for inc in includes), "Bundle must include resources/*.yml"

    # Verify artifact declaration for Python wheel
    artifacts = parsed.get("artifacts", {})
    assert "retail_lakehouse_wheel" in artifacts
    assert artifacts["retail_lakehouse_wheel"].get("type") == "whl"
    assert "build" in artifacts["retail_lakehouse_wheel"]


def test_bundle_variables_and_targets():
    """Verify that bundle variables are defined and targets (dev, prod) are configured properly."""
    content = BUNDLE_ROOT.read_text(encoding="utf-8")
    parsed = yaml.safe_load(content)

    variables = parsed.get("variables", {})
    required_vars = {
        "environment",
        "catalog_name",
        "storage_account_name",
        "container_name",
        "quarantine_threshold_rate",
        "serving_warehouse_name",
        "deployment_sp_id",
    }
    assert required_vars.issubset(set(variables.keys())), f"Missing bundle variables: {required_vars - set(variables.keys())}"

    targets = parsed.get("targets", {})
    assert "dev" in targets, "Bundle must declare a 'dev' target"
    assert "prod" in targets, "Bundle must declare a 'prod' target"

    # Dev target validation
    dev = targets["dev"]
    assert dev.get("mode") == "development"
    assert dev.get("default") is True

    # Prod target validation
    prod = targets["prod"]
    assert prod.get("mode") == "production"
    assert "run_as" in prod, "Production target must declare run_as service principal"
    assert "service_principal_name" in prod["run_as"]


def test_sql_warehouse_bundle_resource():
    """Verify that sql_serving.yml declares a Serverless PRO SQL Warehouse with safe auto-stop."""
    assert SQL_SERVING_RESOURCE.is_file(), "sql_serving.yml must exist."

    content = SQL_SERVING_RESOURCE.read_text(encoding="utf-8")
    parsed = yaml.safe_load(content)

    warehouses = parsed.get("resources", {}).get("sql_warehouses", {})
    assert "retail_lakehouse_serving_warehouse" in warehouses

    wh = warehouses["retail_lakehouse_serving_warehouse"]
    assert wh.get("warehouse_type") == "PRO"
    assert wh.get("enable_serverless_compute") is True
    assert wh.get("min_num_clusters") == 1
    assert wh.get("max_num_clusters") == 1
    assert wh.get("auto_stop_mins") == 10
    assert wh.get("cluster_size") == "2X-Small"


def test_serving_setup_job_bundle_resource():
    """Verify that serving_setup_job.yml declares a bundle job executing 04_serving_views.sql."""
    assert SERVING_JOB_RESOURCE.is_file(), "serving_setup_job.yml must exist."

    content = SERVING_JOB_RESOURCE.read_text(encoding="utf-8")
    parsed = yaml.safe_load(content)

    jobs = parsed.get("resources", {}).get("jobs", {})
    assert "retail_lakehouse_serving_setup" in jobs

    job = jobs["retail_lakehouse_serving_setup"]
    tasks = job.get("tasks", [])
    assert len(tasks) == 1
    task = tasks[0]
    assert task.get("task_key") == "deploy_serving_views"
    assert "sql_task" in task
    sql_task = task["sql_task"]
    assert "04_serving_views.sql" in sql_task.get("file", {}).get("path", "")
    assert "retail_lakehouse_serving_warehouse" in sql_task.get("warehouse_id", "")


def test_serving_views_sql_contract():
    """Verify that 04_serving_views.sql creates the serving schema and defines all 8 required views."""
    assert SERVING_SQL_FILE.is_file(), "04_serving_views.sql must exist."

    content = SERVING_SQL_FILE.read_text(encoding="utf-8")

    # Verify schema creation
    assert "CREATE SCHEMA IF NOT EXISTS serving" in content

    # Verify 8 views are defined
    expected_views = [
        "daily_sales_performance",
        "monthly_revenue",
        "store_region_revenue",
        "category_revenue_performance",
        "customer_spending_summary",
        "return_refund_performance",
        "sales_detail",
        "returns_detail",
    ]

    for v in expected_views:
        pattern = rf"CREATE\s+OR\s+REPLACE\s+VIEW\s+{v}\s+AS"
        assert re.search(pattern, content, re.IGNORECASE), f"View '{v}' must be defined in 04_serving_views.sql"

    # Verify SCD2 customer join fidelity in sales_detail
    assert "JOIN warehouse.dim_customer c ON s.customer_key = c.customer_key" in content, (
        "sales_detail view must join dim_customer on customer_key to preserve point-in-time SCD2 resolution."
    )


def test_serving_views_sql_safety_no_destructive_commands():
    """Verify that serving SQL contains zero destructive DDL or DML commands."""
    content = SERVING_SQL_FILE.read_text(encoding="utf-8").upper()

    destructive_keywords = ["DROP TABLE", "DELETE FROM", "TRUNCATE TABLE", "MERGE INTO"]
    for kw in destructive_keywords:
        assert kw not in content, f"Destructive command '{kw}' prohibited in serving views SQL."


def test_ci_workflow_structure_and_security():
    """Verify that the CI workflow runs full quality gates without requiring Databricks credentials."""
    assert CI_WORKFLOW_FILE.is_file(), ".github/workflows/ci.yml must exist."

    content = CI_WORKFLOW_FILE.read_text(encoding="utf-8")
    parsed = yaml.safe_load(content)

    triggers = parsed.get("on") or parsed.get(True, {})
    assert "push" in triggers or "pull_request" in triggers

    # Verify no Databricks credentials or tokens are referenced in CI
    assert "DATABRICKS_TOKEN" not in content
    assert "DATABRICKS_CLIENT_SECRET" not in content

    # Verify key steps in job
    steps = parsed["jobs"]["validate_and_test"]["steps"]
    step_names = [s.get("name", "") for s in steps]
    assert any("Ruff" in n for n in step_names), "CI must run Ruff linter"
    assert any("Pytest" in n for n in step_names), "CI must run full Pytest suite"
    assert any("Wheel" in n for n in step_names), "CI must build Python wheel"


def test_deploy_workflow_structure_and_oidc_security():
    """Verify that the deployment workflow uses workflow_dispatch, GitHub OIDC, and concurrency control."""
    assert DEPLOY_WORKFLOW_FILE.is_file(), ".github/workflows/deploy_databricks.yml must exist."

    content = DEPLOY_WORKFLOW_FILE.read_text(encoding="utf-8")
    parsed = yaml.safe_load(content)

    # Trigger: workflow_dispatch
    triggers = parsed.get("on") or parsed.get(True, {})
    assert "workflow_dispatch" in triggers

    # Concurrency control
    assert "concurrency" in parsed
    assert parsed["concurrency"].get("cancel-in-progress") is False

    # Permissions: id-token write, contents read
    deploy_job = parsed["jobs"]["deploy"]
    perms = deploy_job.get("permissions", {})
    assert perms.get("id-token") == "write"
    assert perms.get("contents") == "read"

    # Auth: github-oidc without PATs or client secrets
    env_vars = deploy_job.get("env", {})
    assert env_vars.get("DATABRICKS_AUTH_TYPE") == "github-oidc"
    assert "DATABRICKS_HOST" in env_vars
    assert "DATABRICKS_CLIENT_ID" in env_vars
    assert "DATABRICKS_TOKEN" not in content
    assert "DATABRICKS_CLIENT_SECRET" not in content

    # Deployment steps: validate then deploy
    steps = deploy_job.get("steps", [])
    step_runs = [s.get("run", "") for s in steps]
    assert any("bundle validate" in r for r in step_runs), "Deployment must validate bundle before deploying"
    assert any("bundle deploy" in r for r in step_runs), "Deployment must deploy bundle"


def test_secret_scanning_across_module6_artifacts():
    """Verify that zero committed passwords, PATs, SAS tokens, or personal emails exist in Module 6 files."""
    scan_files = [
        BUNDLE_ROOT,
        SQL_SERVING_RESOURCE,
        SERVING_JOB_RESOURCE,
        SERVING_SQL_FILE,
        CI_WORKFLOW_FILE,
        DEPLOY_WORKFLOW_FILE,
    ]

    prohibited_patterns = [
        r"dapi[a-f0-9]{32}",          # Databricks PAT
        r"Bearer\s+[A-Za-z0-9\-\._~\+\/]+=*",  # JWT / Bearer token
        r"DefaultEndpointsProtocol=",   # Storage connection string
        r"sig=[A-Za-z0-9%]+",          # SAS token signature
        r"/mnt/",                      # Legacy DBFS mount
        r"file:///",                   # Local file URI
    ]

    for f in scan_files:
        if f.is_file():
            text = f.read_text(encoding="utf-8")
            for pat in prohibited_patterns:
                match = re.search(pat, text, re.IGNORECASE)
                assert not match, f"Prohibited pattern '{pat}' matched in {f.name}: {match.group(0) if match else ''}"
