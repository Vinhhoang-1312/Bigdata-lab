"""Read telemetry heartbeat JSON written by the Spark driver process."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_DEFAULT_PATH = Path(__file__).resolve().parents[2] / ".telemetry" / "streaming_health.json"


def read_telemetry(path: str | None = None) -> dict[str, Any] | None:
    """Read the latest telemetry heartbeat. Returns None if unavailable.

    The heartbeat file is written by the Spark driver via a shared Docker
    volume at ``/workspace/.telemetry/streaming_health.json``. On the host
    the same file is at ``<repo_root>/.telemetry/streaming_health.json``.
    """
    resolved = Path(path) if path else _DEFAULT_PATH
    # Also check via env override
    if not resolved.exists():
        env_path = os.getenv("URBANFLOW_TELEMETRY_PATH")
        if env_path:
            resolved = Path(env_path)
    if not resolved.exists():
        return None
    try:
        text = resolved.read_text(encoding="utf-8")
        data = json.loads(text)
        if not isinstance(data, dict):
            return None
        return data
    except (json.JSONDecodeError, OSError):
        return None


def telemetry_age_seconds(
    telemetry: dict[str, Any] | None, now: datetime | None = None
) -> int | None:
    """Seconds since the telemetry heartbeat was written. None if unavailable."""
    if telemetry is None:
        return None
    ts_str = telemetry.get("timestamp")
    if not ts_str:
        return None
    try:
        ts = datetime.fromisoformat(ts_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        ref = now or datetime.now(timezone.utc)
        return max(0, int((ref - ts).total_seconds()))
    except (ValueError, TypeError):
        return None
