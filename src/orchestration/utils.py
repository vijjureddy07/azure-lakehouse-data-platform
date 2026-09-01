"""
Storage URI and Path Utilities for Orchestration (Module 5).

Provides string-safe URI composition supporting both local filesystem paths
and cloud object store URIs (abfss://, s3://, wasbs://) without schema corruption.
"""

from __future__ import annotations

from pathlib import Path


def join_storage_uri(base: str | Path, *parts: str) -> str:
    """
    Join a base storage path or cloud URI with subpaths safely.

    Preserves cloud URI schemes (e.g. `abfss://container@account.dfs.core.windows.net/`)
    without corrupting double slashes into single slashes.

    Args:
        base: Root path string or Path object.
        *parts: Subdirectory and file parts to join.

    Returns:
        str: Normalized combined path or URI.

    Examples:
        >>> join_storage_uri("abfss://lakehouse@stdev.dfs.core.windows.net", "delta", "bronze")
        'abfss://lakehouse@stdev.dfs.core.windows.net/delta/bronze'
        >>> join_storage_uri("/tmp/lakehouse", "delta", "bronze")
        '/tmp/lakehouse/delta/bronze'
    """
    base_str = str(base).strip()
    clean_parts = [p.strip().strip("/") for p in parts if p and str(p).strip()]

    if "://" in base_str:
        scheme, remainder = base_str.split("://", 1)
        clean_remainder = remainder.rstrip("/")
        if clean_parts:
            combined = "/".join([clean_remainder] + clean_parts)
        else:
            combined = clean_remainder
        return f"{scheme}://{combined}"

    # Local filesystem path
    p = Path(base_str)
    for part in clean_parts:
        p = p / part
    return p.as_posix()
