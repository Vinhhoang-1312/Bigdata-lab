"""Watermark and event-time progression observability for Spark Structured Streaming."""
from __future__ import annotations

from typing import Any


def build_watermark_status(
    telemetry: dict[str, Any] | None,
    configured_delay: str = "10 minutes",
) -> dict[str, Any]:
    """Extract and format event-time and watermark progression metrics."""
    if not telemetry:
        return {
            "max_event_time": None,
            "current_watermark": None,
            "configured_delay": configured_delay,
            "advancing": None,
        }

    wm = telemetry.get("watermark")
    max_ev = telemetry.get("max_event_time")
    advancing = bool(wm and str(wm).strip())

    return {
        "max_event_time": max_ev,
        "current_watermark": wm,
        "configured_delay": configured_delay,
        "advancing": advancing,
    }
