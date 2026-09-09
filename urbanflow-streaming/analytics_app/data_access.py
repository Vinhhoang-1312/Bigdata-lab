"""Read-only access to Historical and Live Gold datasets."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

try:
    from .zones import ZONE_CENTROIDS
except ImportError:
    from zones import ZONE_CENTROIDS

ANALYTICS_TIMEZONE = "America/New_York"
HIGH_DEMAND_RATIO = 1.5

HISTORICAL_COLUMNS = [
    "pickup_zone_id", "day_of_week", "hour", "five_minute_slot",
    "historical_mean_trip_count", "historical_std_trip_count",
    "historical_mean_distance", "historical_mean_duration",
]
LIVE_COLUMNS = [
    "window_start", "window_end", "pickup_zone_id", "current_trip_count",
    "historical_mean_trip_count", "demand_ratio", "z_score",
]


def _read(path: str) -> pd.DataFrame:
    suffix = Path(path).suffix.lower()
    return pd.read_json(path, lines=True) if suffix in {".json", ".jsonl"} else pd.read_parquet(path)


def _require(frame: pd.DataFrame, columns: list[str], dataset: str) -> pd.DataFrame:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"{dataset} Gold missing columns: {missing}")
    return frame.loc[:, columns].copy()


@dataclass(frozen=True)
class GoldReader:
    historical_path: str
    live_path: str

    def historical(self) -> pd.DataFrame:
        return _require(_read(self.historical_path), HISTORICAL_COLUMNS, "Historical")

    def live(self) -> pd.DataFrame:
        frame = _require(_read(self.live_path), LIVE_COLUMNS, "Live")
        frame["window_start"] = pd.to_datetime(frame["window_start"], utc=True)
        frame["window_end"] = pd.to_datetime(frame["window_end"], utc=True)
        return frame

    def filtered_live(
        self, zones: list[int] | None = None, start=None, end=None
    ) -> pd.DataFrame:
        frame = self.live()
        if zones:
            frame = frame[frame.pickup_zone_id.isin(zones)]
        if start is not None:
            frame = frame[frame.window_start >= pd.Timestamp(start, tz="UTC")]
        if end is not None:
            frame = frame[frame.window_start <= pd.Timestamp(end, tz="UTC")]
        return frame.sort_values(["window_start", "pickup_zone_id"])

    def latest(self) -> pd.DataFrame:
        frame = self.live()
        return frame[frame.window_start == frame.window_start.max()].copy() if not frame.empty else frame


def to_analytics_local(series: pd.Series) -> pd.Series:
    """UTC instants as naive America/New_York clocks so charts do not shift to the laptop TZ."""
    utc = pd.to_datetime(series, utc=True)
    return utc.dt.tz_convert(ANALYTICS_TIMEZONE).dt.tz_localize(None)


def chart_mode_for(frame: pd.DataFrame) -> str:
    if frame.empty or "window_start_local" not in frame.columns:
        return "grouped"
    return "line" if frame["window_start_local"].nunique() >= 2 else "grouped"


def prepare_live_view(live: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    view = live.copy()
    view["window_start_local"] = to_analytics_local(view["window_start"])
    view["window_end_local"] = to_analytics_local(view["window_end"])
    view["window_label"] = view["window_start_local"].dt.strftime("%Y-%m-%d %H:%M NY")
    named = lookup.loc[:, ["pickup_zone_id", "zone_name"]]
    view = view.merge(named, on="pickup_zone_id", how="left")
    view["zone_name"] = view["zone_name"].fillna("Unknown zone")
    view["zone_label"] = view["pickup_zone_id"].astype(int).astype(str) + " — " + view["zone_name"]
    view["is_high_demand"] = view["demand_ratio"].gt(HIGH_DEMAND_RATIO)
    view["lat"] = view["pickup_zone_id"].map(lambda z: ZONE_CENTROIDS.get(int(z), (None, None))[0])
    view["lon"] = view["pickup_zone_id"].map(lambda z: ZONE_CENTROIDS.get(int(z), (None, None))[1])
    return view.sort_values(["window_start_local", "pickup_zone_id"]).reset_index(drop=True)
