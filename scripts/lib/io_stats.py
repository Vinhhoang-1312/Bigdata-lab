"""Helpers for storage statistics and JSON reports."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any


def write_json(path: Path | str, payload: dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def dir_file_stats(root: Path) -> dict[str, Any]:
    """Walk a local directory: file count + total bytes (for before/after packing)."""
    n_files = 0
    n_bytes = 0
    if not root.exists():
        return {"exists": False, "n_files": 0, "n_bytes": 0, "n_bytes_h": "0 B"}
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            fp = Path(dirpath) / name
            try:
                n_bytes += fp.stat().st_size
                n_files += 1
            except OSError:
                continue
    return {
        "exists": True,
        "path": str(root),
        "n_files": n_files,
        "n_bytes": n_bytes,
        "n_bytes_h": _human(n_bytes),
    }


def _human(n: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    x = float(n)
    for u in units:
        if x < 1024 or u == units[-1]:
            return f"{x:.2f} {u}"
        x /= 1024
    return f"{n} B"


def try_hdfs_count(path: str) -> dict[str, Any] | None:
    """Best-effort `hdfs dfs -count` / `-du` when on a cluster."""
    out: dict[str, Any] = {"path": path}
    for args, key in (
        (["hdfs", "dfs", "-count", path], "count"),
        (["hdfs", "dfs", "-du", "-s", "-h", path], "du"),
    ):
        try:
            proc = subprocess.run(args, capture_output=True, text=True, check=False)
            out[key] = (proc.stdout or proc.stderr).strip()
        except FileNotFoundError:
            return None
    return out
