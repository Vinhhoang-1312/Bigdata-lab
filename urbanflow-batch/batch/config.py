"""Standard-library configuration loading for Batch jobs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

FOUNDATION_VERSION = "1.0.0"


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


def load_settings(path: str) -> BatchSettings:
    with Path(path).open(encoding="utf-8") as handle:
        raw = json.load(handle)

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
