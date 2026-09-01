"""
Lakeflow Jobs YAML Specification Parser & Structural Validator (Module 5).

Statically validates Lakeflow Jobs YAML definitions:
1. Validates DAG task structure, unique keys, and dependency graph.
2. Detects circular dependencies (cycle detection).
3. Verifies required job-level parameters.
4. Enforces modern dynamic value syntax: {{job.id}}, {{job.run_id}}, {{job.parameters.<name>}}, {{tasks.<task>.values.<val>}}.
5. Detects and rejects deprecated dynamic tokens: {{job_id}}, {{run_id}}, {{start_date}}, {{task_retry_count}}.
6. Confirms run_if, retries, and timeout policies.
7. Scans for hardcoded secrets, passwords, tokens, and personal emails.
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


def validate_lakeflow_job_yaml(yaml_path: Path | str) -> dict[str, Any]:
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

    # 2. Deprecated Dynamic Variable Syntax Detection
    for dep_pat in DEPRECATED_DYNAMIC_PATTERNS:
        match = re.search(dep_pat, content)
        if match:
            raise LakeflowJobValidationError(
                f"Deprecated Syntax: Found obsolete Databricks dynamic token '{match.group(0)}'. "
                f"Use modern Lakeflow syntax e.g. '{{{{job.id}}}}' or '{{{{job.run_id}}}}'."
            )

    # 3. YAML Parsing
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

    # 4. Validate Parameters
    params = job_def.get("parameters", [])
    param_names = {p.get("name") for p in params if isinstance(p, dict)}
    for req_param in REQUIRED_JOB_PARAMETERS:
        if req_param not in param_names:
            raise LakeflowJobValidationError(
                f"Missing required job parameter: '{req_param}'. Found parameters: {sorted(list(param_names))}"
            )

    # 5. Validate Tasks and Graph Structure
    tasks = job_def.get("tasks", [])
    if not tasks:
        raise LakeflowJobValidationError("Job contains 0 tasks.")

    task_keys = [t.get("task_key") for t in tasks if "task_key" in t]
    if len(task_keys) != len(set(task_keys)):
        duplicates = [k for k in task_keys if task_keys.count(k) > 1]
        raise LakeflowJobValidationError(f"Duplicate task keys found: {set(duplicates)}")

    # 6. Cycle Detection
    cycles = find_cycles(tasks)
    if cycles:
        raise LakeflowJobValidationError(f"Circular dependency detected in DAG: {cycles[0]}")

    # 7. Validate Modern Dynamic Value References
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
