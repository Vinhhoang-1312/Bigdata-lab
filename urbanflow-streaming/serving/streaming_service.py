"""Read-only status models built from Streaming Gold outputs."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from analytics_app.data_access import GoldReader, prepare_live_view


def _iso(value: pd.Timestamp) -> str:
    return value.to_pydatetime().astimezone(timezone.utc).isoformat()


def build_streaming_overview(reader: GoldReader, lookup: pd.DataFrame) -> dict[str, Any]:
    """Summarize the latest completed Live Gold window without reproducing ETL."""
    view = prepare_live_view(reader.live(), lookup)
    if view.empty:
        return {"status": "empty", "latest_window": None, "latest_rows": [], "current_trip_count": 0}
    latest = view[view["window_start"] == view["window_start"].max()].copy()
    columns = [
        "pickup_zone_id",
        "zone_name",
        "current_trip_count",
        "historical_mean_trip_count",
        "demand_ratio",
        "z_score",
        "is_high_demand",
    ]
    return {
        "status": "available",
        "latest_window": _iso(latest["window_start"].iloc[0]),
        "current_trip_count": int(latest["current_trip_count"].sum()),
        "high_demand_zone_count": int(latest["is_high_demand"].sum()),
        "latest_rows": latest.loc[:, columns].sort_values("demand_ratio", ascending=False).to_dict("records"),
    }


def build_streaming_health(
    reader: GoldReader,
    telemetry: dict[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    """Report freshness and optional telemetry while preserving unknown states."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must include a timezone")
    live = reader.live()
    if live.empty:
        return {
            "status": "empty",
            "latest_window_end": None,
            "live_gold_age_seconds": None,
            "live_gold_row_count": 0,
            "telemetry_status": "unavailable" if telemetry is None else "available",
            "invalid_record_count": None if telemetry is None else telemetry.get("invalid_record_count"),
        }
    latest_end = live["window_end"].max()
    age_seconds = max(0, int((pd.Timestamp(now).tz_convert("UTC") - latest_end).total_seconds()))
    return {
        "status": "degraded" if age_seconds > 300 else "available",
        "latest_window_end": _iso(latest_end),
        "live_gold_age_seconds": age_seconds,
        "live_gold_row_count": int(len(live)),
        "telemetry_status": "unavailable" if telemetry is None else "available",
        "invalid_record_count": None if telemetry is None else telemetry.get("invalid_record_count"),
    }
