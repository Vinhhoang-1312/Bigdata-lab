"""Watermark and event-time progression observability for Spark Structured Streaming."""
from __future__ import annotations

from typing import Any


def build_watermark_status(
    telemetry: dict[str, Any] | None,
    configured_delay: str = "10 minutes",
    prev_watermark: str | None = None,
) -> dict[str, Any]:
    """Extract and format event-time and watermark progression metrics.

    Truly measures whether watermark has advanced compared to previous observation.
    """
    if not telemetry:
        return {
            "max_event_time": None,
            "current_watermark": None,
            "configured_delay": configured_delay,
            "advancing": None,
        }

    wm = telemetry.get("watermark")
    max_ev = telemetry.get("max_event_time")

    if not wm or not str(wm).strip():
        return {
            "max_event_time": max_ev,
            "current_watermark": None,
            "configured_delay": configured_delay,
            "advancing": False,
        }

    wm_str = str(wm).strip()
    if prev_watermark is not None and str(prev_watermark).strip():
        advancing = wm_str > str(prev_watermark).strip()
    elif "watermark_advancing" in telemetry and telemetry["watermark_advancing"] is not None:
        advancing = bool(telemetry["watermark_advancing"])
    elif prev_watermark is None:
        advancing = True
    else:
        advancing = False

    return {
        "max_event_time": max_ev,
        "current_watermark": wm,
        "configured_delay": configured_delay,
        "advancing": advancing,
    }
