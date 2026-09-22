"""Taxi-zone labels and TLC GeoJSON for the analytics map (not ETL)."""
from __future__ import annotations

import json
from copy import deepcopy
from functools import lru_cache
from pathlib import Path

import pandas as pd

try:
    from .storage import readable_path, storage_options
except ImportError:
    from storage import readable_path, storage_options

HIGH_DEMAND_RATIO = 1.5
GEOJSON_PATH = Path(__file__).resolve().parent / "data" / "taxi_zones.geojson"

# Approximate TLC zone centroids (WGS84), kept for tests and fallbacks.
ZONE_CENTROIDS: dict[int, tuple[float, float]] = {
    4: (40.7238, -73.9760),
    13: (40.7116, -74.0160),
    48: (40.7620, -73.9890),
    161: (40.7547, -73.9812),
    170: (40.7478, -73.9768),
    234: (40.7359, -73.9903),
    236: (40.7808, -73.9532),
    237: (40.7686, -73.9656),
}


def load_zone_lookup(path: str | Path) -> pd.DataFrame:
    source = str(path)
    frame = pd.read_csv(readable_path(source), **storage_options(source))
    required = {"LocationID", "Zone"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"zone lookup missing columns: {sorted(missing)}")
    out = frame.rename(columns={"LocationID": "pickup_zone_id", "Zone": "zone_name"})
    return out.loc[:, ["pickup_zone_id", "zone_name"]].drop_duplicates("pickup_zone_id")


def ratio_fill_color(ratio: float | None) -> list[int]:
    if ratio is None or pd.isna(ratio):
        return [186, 186, 186, 90]
    if ratio > HIGH_DEMAND_RATIO:
        return [193, 18, 31, 200]
    if ratio >= 1:
        return [233, 196, 106, 200]
    return [42, 157, 143, 180]


@lru_cache(maxsize=1)
def load_taxi_zone_geojson(path: str | None = None) -> dict:
    geo_path = Path(path) if path else GEOJSON_PATH
    return json.loads(geo_path.read_text(encoding="utf-8"))


def attach_demand_to_geojson(geojson: dict, live_view: pd.DataFrame) -> dict:
    """Copy TLC polygons and paint fill_color from Live Gold demand_ratio."""
    painted = deepcopy(geojson)
    by_zone: dict[int, pd.Series] = {}
    if not live_view.empty:
        for _, row in live_view.iterrows():
            by_zone[int(row.pickup_zone_id)] = row
    for feat in painted["features"]:
        loc = int(feat["properties"]["location_id"])
        row = by_zone.get(loc)
        ratio = None if row is None else row.demand_ratio
        ratio = None if ratio is not None and pd.isna(ratio) else (None if ratio is None else float(ratio))
        trips = None if row is None else int(row.current_trip_count)
        feat["properties"]["demand_ratio"] = ratio
        feat["properties"]["current_trip_count"] = trips
        feat["properties"]["zone_label"] = f"{loc} — {feat['properties']['zone']}"
        color = ratio_fill_color(ratio)
        feat["properties"]["fill_color"] = color
        feat["properties"]["fill_r"] = color[0]
        feat["properties"]["fill_g"] = color[1]
        feat["properties"]["fill_b"] = color[2]
        feat["properties"]["fill_a"] = color[3]
    return painted
