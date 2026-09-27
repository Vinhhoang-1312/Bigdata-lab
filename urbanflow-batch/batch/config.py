"""Configuration loading and validation for Batch jobs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

FOUNDATION_VERSION = "1.0.0"
REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class BatchSettings:
    foundation_version: str
    analytics_timezone: str
    source_trips_path: str
    source_trips_format: str
    zone_lookup_path: str
    silver_output_path: str
    rejected_output_path: str
    write_mode: str = "overwrite"


def _parse_yaml(text: str) -> dict[str, Any]:
    try:
        import yaml
        return yaml.safe_load(text)
    except ImportError:
        result: dict[str, Any] = {}
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" in line:
                key, val = line.split(":", 1)
                key = key.strip()
                val = val.strip().strip("\"'")
                result[key] = val
        return result


def _resolve_path(raw_path: str) -> str:
    if raw_path.startswith(("s3a://", "http://", "https://", "s3://")):
        return raw_path
    path = Path(raw_path)
    if path.exists():
        return str(path)
    if raw_path.startswith("/workspace/"):
        candidate = REPO_ROOT / raw_path.removeprefix("/workspace/")
        if candidate.exists():
            return str(candidate)
    candidate = REPO_ROOT / path
    if candidate.exists():
        return str(candidate)
    return raw_path


def load_settings(path: str | Path) -> BatchSettings:
    config_path = Path(path).resolve()
    with config_path.open(encoding="utf-8") as handle:
        content = handle.read()

    if config_path.suffix == ".json":
        raw = json.loads(content)
    else:
        raw = _parse_yaml(content)

    if "source_trips_path" in raw:
        raw["source_trips_path"] = _resolve_path(str(raw["source_trips_path"]))
    if "zone_lookup_path" in raw:
        raw["zone_lookup_path"] = _resolve_path(str(raw["zone_lookup_path"]))

    settings = BatchSettings(**raw)
    if settings.foundation_version != FOUNDATION_VERSION:
        raise ValueError(
            f"Expected Foundation {FOUNDATION_VERSION}, got "
            f"{settings.foundation_version}"
        )
    if settings.analytics_timezone != "America/New_York":
        raise ValueError("analytics_timezone must be America/New_York")
    if settings.source_trips_format not in {"csv", "parquet"}:
        raise ValueError("source_trips_format must be csv or parquet")
    if settings.write_mode not in {"error", "errorifexists", "append", "overwrite"}:
        raise ValueError(f"Unsupported write_mode: {settings.write_mode}")
    return settings
