"""Path resolution: local mirror vs real HDFS."""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def use_local() -> bool:
    """Default local=True so a laptop dry-run works without a cluster."""
    return os.environ.get("BEN_USE_LOCAL", "1").strip() not in {"0", "false", "False", "no"}


def repo_root() -> Path:
    return REPO_ROOT


def data_incoming() -> Path:
    return REPO_ROOT / "data" / "incoming"


def subset_dir() -> Path:
    return REPO_ROOT / "data" / "subset"


def figures_dir() -> Path:
    p = REPO_ROOT / "reports" / "figures"
    p.mkdir(parents=True, exist_ok=True)
    return p


def local_hdfs_mirror() -> Path:
    p = REPO_ROOT / "hdfs_mirror" / "bigearthnet"
    p.mkdir(parents=True, exist_ok=True)
    return p


def hdfs_root() -> str:
    """Logical root used by Spark paths."""
    if use_local():
        return local_hdfs_mirror().resolve().as_uri()
    return os.environ.get("BEN_HDFS_ROOT", "/bigearthnet")


def join_root(*parts: str) -> str:
    root = hdfs_root().rstrip("/")
    tail = "/".join(p.strip("/") for p in parts if p)
    return f"{root}/{tail}" if tail else root


def raw_path() -> str:
    return join_root("raw")


def packed_path() -> str:
    return join_root("packed")


def metadata_store_path() -> str:
    return join_root("metadata")


def features_path() -> str:
    return join_root("features")


def features_v2_path() -> str:
    return join_root("features_v2")


def models_path() -> str:
    return join_root("models")


def predictions_path() -> str:
    return join_root("predictions")


def results_path() -> str:
    return join_root("results")


def spark_uri(path: str | Path) -> str:
    """Normalize a filesystem path for Spark read/write."""
    s = str(path)
    if s.startswith("hdfs://") or s.startswith("file:"):
        return s
    return Path(s).resolve().as_uri()
