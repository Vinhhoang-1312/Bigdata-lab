"""Read-only status models built from Streaming Gold outputs."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from analytics_app.data_access import GoldReader, prepare_live_view


def _iso(value: pd.Timestamp) -> str:
    return value.to_pydatetime().astimezone(timezone.utc).isoformat()


def build_streaming_overview(
    reader: GoldReader, lookup: pd.DataFrame, now: datetime | None = None,
    live: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Summarize the latest completed Live Gold window without reproducing ETL."""
    view = prepare_live_view(reader.live() if live is None else live, lookup)
    if view.empty:
        return {
            "status": "empty", "source_mode": "unknown", "timezone": "America/New_York",
            "window_minutes": 5, "latest_window": None, "latest_rows": [],
            "current_trip_count": 0, "high_demand_zone_count": 0,
        }
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
    current_time = now or datetime.now(timezone.utc)
    age_seconds = (pd.Timestamp(current_time) - latest["window_end"].max()).total_seconds()
    rows = latest.loc[:, columns].sort_values(
        ["current_trip_count", "pickup_zone_id"], ascending=[False, True]
    )
    rows = rows.astype(object).where(pd.notna(rows), None)
    return {
        "status": "available",
        "source_mode": "historical_replay" if age_seconds > 86400 else "live",
        "timezone": "America/New_York",
        "window_minutes": 5,
        "latest_window": _iso(latest["window_start"].iloc[0]),
        "current_trip_count": int(latest["current_trip_count"].sum()),
        "high_demand_zone_count": int(latest["is_high_demand"].sum()),
        "latest_rows": rows.to_dict("records"),
    }


def build_replay_catalog(live: pd.DataFrame) -> dict[str, Any]:
    """Index stored Gold windows by New York month without filling source gaps."""
    if live.empty:
        return {"timezone": "America/New_York", "months": []}
    frame = live.loc[:, ["window_start", "current_trip_count"]].copy()
    frame["window_start"] = pd.to_datetime(frame["window_start"], utc=True)
    local = frame["window_start"].dt.tz_convert("America/New_York")
    frame["month"] = local.dt.strftime("%Y-%m")
    frame["date"] = local.dt.strftime("%Y-%m-%d")
    frame["hour"] = local.dt.hour
    months = []
    for month, rows in frame.groupby("month", sort=True):
        windows = sorted(timestamp.isoformat() for timestamp in rows["window_start"].unique())
        hourly = rows.groupby("hour")["current_trip_count"].sum()
        months.append({
            "month": month,
            "windows": windows,
            "window_count": len(windows),
            "observed_day_count": int(rows["date"].nunique()),
            "hourly_pickups": [int(hourly.get(hour, 0)) for hour in range(24)],
        })
    return {"timezone": "America/New_York", "months": months}


def build_streaming_health(
    reader: GoldReader,
    telemetry: dict[str, Any] | None,
    now: datetime,
    live: pd.DataFrame | None = None,
    spark_stream_status: str = "unavailable",
) -> dict[str, Any]:
    """Report freshness and optional telemetry while preserving unknown states."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must include a timezone")
    live = reader.live() if live is None else live
    if live.empty:
        return {
            "status": "empty",
            "gold_source_uri": reader.live_path,
            "latest_window_end": None,
            "live_gold_age_seconds": None,
            "live_gold_row_count": 0,
            "telemetry_status": "unavailable" if telemetry is None else "available",
            "invalid_record_count": None if telemetry is None else telemetry.get("invalid_record_count"),
            "spark_stream_status": spark_stream_status,
        }
    latest_end = live["window_end"].max()
    age_seconds = max(0, int((pd.Timestamp(now).tz_convert("UTC") - latest_end).total_seconds()))
    return {
        "status": "degraded" if age_seconds > 300 else "available",
        "gold_source_uri": reader.live_path,
        "latest_window_end": _iso(latest_end),
        "live_gold_age_seconds": age_seconds,
        "live_gold_row_count": int(len(live)),
        "telemetry_status": "unavailable" if telemetry is None else "available",
        "invalid_record_count": None if telemetry is None else telemetry.get("invalid_record_count"),
        "spark_stream_status": spark_stream_status,
    }
