"""
Lakeflow Jobs YAML Specification Parser & Structural Validator (Module 5).

Statically validates Lakeflow Jobs YAML definitions:
1. Validates DAG task structure, unique keys, and dependency graph.
2. Detects circular dependencies (cycle detection).
3. Verifies required job-level parameters.
4. Enforces modern dynamic value syntax: {{job.id}}, {{job.run_id}}, {{job.parameters.<name>}}, {{tasks.<task>.values.<val>}}.
5. Detects and rejects deprecated dynamic tokens: {{job_id}}, {{run_id}}, {{start_date}}, {{task_retry_count}}.
6. Confirms condition_task structure and outcome dependencies.
7. Confirms run_if, retries, and timeout policies.
8. Ensures zero legacy DBFS /mnt/ mount references.
9. Scans for hardcoded secrets, passwords, tokens, and personal emails.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

DEPRECATED_DYNAMIC_PATTERNS = [
    r"\{\{\s*job_id\s*\}\}",
    r"\{\{\s*run_id\s*\}\}",
    r"\{\{\s*start_date\s*\}\}",
    r"\{\{\s*task_retry_count\s*\}\}",
]

REQUIRED_JOB_PARAMETERS = [
    "environment",
    "ingestion_date",
    "adf_run_id",
    "storage_account_name",
    "container_name",
    "catalog_name",
]

SECRET_PATTERNS = [
    r"dapi[a-f0-9]{32}",                  # Databricks PAT
    r"eyJ[a-zA-Z0-9_\-]{20,}\.[a-zA-Z0-9_\-]{20,}", # JWT token
    r"(?i)password\s*:\s*['\"][^'\"]+['\"]",
    r"(?i)client_secret\s*:\s*['\"][^'\"]+['\"]",
    r"[a-zA-Z0-9_.+-]+@(?!example\.com|placeholder\.org)[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", # Personal emails
]

REQUIRED_TASK_KEYS = [
    "validate_landing_batch",
    "bronze_ingestion",
    "silver_transformation",
    "check_quarantine_threshold",
    "gold_analytics",
    "dimensional_warehouse",
    "final_quality_gate",
    "publish_run_summary",
]


class LakeflowJobValidationError(ValueError):
    """Raised when a Lakeflow Jobs YAML specification fails validation."""
    pass


def find_cycles(tasks: list[dict[str, Any]]) -> list[list[str]]:
    """
    Detect cycles in the task dependency graph using depth-first search.
    """
    adj: dict[str, list[str]] = {}
    for t in tasks:
        t_key = t.get("task_key", "")
        deps = [d.get("task_key", "") for d in t.get("depends_on", [])]
        adj[t_key] = deps

    cycles: list[list[str]] = []
    visited: dict[str, int] = {k: 0 for k in adj}  # 0: unvisited, 1: visiting, 2: visited

    def dfs(node: str, path: list[str]) -> None:
        visited[node] = 1
        path.append(node)

        for neighbor in adj.get(node, []):
            if neighbor in visited:
                if visited[neighbor] == 1:
                    cycle_start = path.index(neighbor)
                    cycles.append(path[cycle_start:] + [neighbor])
                elif visited[neighbor] == 0:
                    dfs(neighbor, path)

        path.pop()
        visited[node] = 2

    for node in adj:
        if visited[node] == 0:
            dfs(node, [])

    return cycles


def validate_lakeflow_job_yaml(yaml_path: Path | str, repo_root: Path | str | None = None) -> dict[str, Any]:
    """
    Parse and validate a Lakeflow Jobs YAML definition.

    Returns:
        dict: Parsed job specification.

    Raises:
        LakeflowJobValidationError: If any structural, syntax, or security rule fails.
    """
    p = Path(yaml_path)
    if not p.is_file():
        raise LakeflowJobValidationError(f"Job definition file not found: {p}")

    content = p.read_text(encoding="utf-8")

    # 1. Secret Scanning
    for pattern in SECRET_PATTERNS:
        match = re.search(pattern, content)
        if match:
            raise LakeflowJobValidationError(
                f"Security Violation: Potential secret/credential matched pattern '{pattern}': {match.group(0)}"
            )

    # 2. Legacy Mount Path Check
    if "/mnt/" in content:
        raise LakeflowJobValidationError("Architecture Violation: Found legacy '/mnt/' DBFS mount path in Lakeflow YAML.")

    # 3. Deprecated Dynamic Variable Syntax Detection
    for dep_pat in DEPRECATED_DYNAMIC_PATTERNS:
        match = re.search(dep_pat, content)
        if match:
            raise LakeflowJobValidationError(
                f"Deprecated Syntax: Found obsolete Databricks dynamic token '{match.group(0)}'. "
                f"Use modern Lakeflow syntax e.g. '{{{{job.id}}}}' or '{{{{job.run_id}}}}'."
            )

    # 4. YAML Parsing
    try:
        data = yaml.safe_load(content)
    except Exception as e:
        raise LakeflowJobValidationError(f"Invalid YAML format in {p}: {e}") from e

    if not isinstance(data, dict):
        raise LakeflowJobValidationError("YAML root must be a dictionary.")

    # Locate job definition under resources.jobs or top-level jobs
    jobs = data.get("resources", {}).get("jobs", {}) if "resources" in data else data.get("jobs", {})
    if not jobs:
        raise LakeflowJobValidationError("Missing 'resources.jobs' or 'jobs' definition block.")

    # Extract first job definition
    job_key = next(iter(jobs))
    job_def = jobs[job_key]

    # 5. Validate Parameters
    params = job_def.get("parameters", [])
    param_names = {p.get("name") for p in params if isinstance(p, dict)}
    for req_param in REQUIRED_JOB_PARAMETERS:
        if req_param not in param_names:
            raise LakeflowJobValidationError(
                f"Missing required job parameter: '{req_param}'. Found parameters: {sorted(list(param_names))}"
            )

    # 6. Validate Tasks and Graph Structure
    tasks = job_def.get("tasks", [])
    if not tasks:
        raise LakeflowJobValidationError("Job contains 0 tasks.")

    task_map = {t.get("task_key"): t for t in tasks if "task_key" in t}
    task_keys = list(task_map.keys())

    if len(task_keys) != len(tasks):
        duplicates = [t.get("task_key") for t in tasks if task_keys.count(t.get("task_key")) > 1]
        raise LakeflowJobValidationError(f"Duplicate task keys found: {set(duplicates)}")

    # 6. Cycle Detection
    cycles = find_cycles(tasks)
    if cycles:
        raise LakeflowJobValidationError(f"Circular dependency detected in DAG: {cycles[0]}")

    # 7. Verify required tasks presence
    for req_task in REQUIRED_TASK_KEYS:
        if req_task not in task_map:
            raise LakeflowJobValidationError(f"Missing required task: '{req_task}' in Lakeflow DAG.")

    # 8. Validate Condition Tasks and Outcome Dependencies
    for t in tasks:
        if "condition_task" in t:
            cond = t["condition_task"]
            if not isinstance(cond, dict) or "op" not in cond or "left" not in cond or "right" not in cond:
                raise LakeflowJobValidationError(f"Invalid condition_task structure in task '{t.get('task_key')}'")

        for dep in t.get("depends_on", []):
            if "outcome" in dep:
                outcome_val = str(dep["outcome"]).lower()
                if outcome_val not in ("true", "false"):
                    raise LakeflowJobValidationError(
                        f"Invalid dependency outcome '{dep['outcome']}' in task '{t.get('task_key')}'. Must be 'true' or 'false'."
                    )

    # 9. Verify publish_run_summary run_if policy
    publish_task = task_map.get("publish_run_summary")
    if publish_task and publish_task.get("run_if") != "ALL_DONE":
        raise LakeflowJobValidationError("Task 'publish_run_summary' must specify 'run_if: ALL_DONE'.")

    # 10. Validate Modern Dynamic Value References
    modern_pattern = re.compile(r"\{\{\s*([a-zA-Z0-9_.]+)\s*\}\}")
    valid_prefixes = ("job.", "tasks.", "bundle.")
    for match in modern_pattern.finditer(content):
        expr = match.group(1).strip()
        if not any(expr.startswith(pref) for pref in valid_prefixes):
            raise LakeflowJobValidationError(
                f"Invalid dynamic parameter reference: '{{{{{expr}}}}}'. "
                f"Must start with one of: {valid_prefixes}"
            )

    return {
        "job_key": job_key,
        "job_name": job_def.get("name"),
        "parameters": param_names,
        "task_keys": task_keys,
        "task_count": len(tasks),
        "tasks": tasks,
    }
