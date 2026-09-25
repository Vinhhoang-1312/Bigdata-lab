"""Taxi-zone labels and TLC GeoJSON for the analytics map (not ETL)."""
from __future__ import annotations

import json
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
    cols = ["pickup_zone_id", "zone_name"]
    rename_dict = {"LocationID": "pickup_zone_id", "Zone": "zone_name"}
    if "Borough" in frame.columns:
        rename_dict["Borough"] = "borough"
        cols.append("borough")
    out = frame.rename(columns=rename_dict)
    return out.loc[:, cols].drop_duplicates("pickup_zone_id")


def ratio_fill_color(ratio: float | None, dark_mode: bool = True) -> list[int]:
    if ratio is None or pd.isna(ratio):
        return [70, 70, 70, 140] if dark_mode else [186, 186, 186, 140]
    if ratio > HIGH_DEMAND_RATIO:
        return [239, 68, 68, 220]  # Vibrant coral-red
    if ratio >= 1:
        return [245, 158, 11, 220]  # Amber gold
    return [16, 185, 129, 210]  # Emerald green


def trip_count_fill_color(trips: int | None, dark_mode: bool = True) -> list[int]:
    if trips is None or trips <= 0:
        return [30, 41, 59, 140] if dark_mode else [226, 232, 240, 160]
    if trips >= 50:
        return [244, 63, 94, 230]  # Bright rose
    if trips >= 25:
        return [168, 85, 247, 220]  # Purple
    if trips >= 10:
        return [59, 130, 246, 220]  # Electric blue
    return [20, 184, 166, 200]  # Teal


def z_score_fill_color(z: float | None, dark_mode: bool = True) -> list[int]:
    if z is None or pd.isna(z):
        return [30, 41, 59, 140] if dark_mode else [226, 232, 240, 160]
    if z >= 2.0:
        return [239, 68, 68, 230]  # High surge anomaly
    if z >= 1.0:
        return [249, 115, 22, 220]  # Elevated
    if z >= -1.0:
        return [99, 102, 241, 200]  # Normal baseline range
    return [14, 165, 233, 190]  # Low anomaly


def _point_line_distance(point: list[float], start: list[float], end: list[float]) -> float:
    """Return planar point-to-line distance for lightweight map simplification."""
    from math import hypot

    x, y = point[:2]
    x1, y1 = start[:2]
    x2, y2 = end[:2]
    dx = x2 - x1
    dy = y2 - y1
    if dx == 0 and dy == 0:
        return hypot(x - x1, y - y1)
    return abs(dy * x - dx * y + x2 * y1 - y2 * x1) / hypot(dx, dy)


def _douglas_peucker(points: list[list[float]], tolerance: float) -> list[list[float]]:
    if len(points) <= 2:
        return points
    start, end = points[0], points[-1]
    max_distance = 0.0
    split_index = 0
    for index in range(1, len(points) - 1):
        distance = _point_line_distance(points[index], start, end)
        if distance > max_distance:
            max_distance = distance
            split_index = index
    if max_distance <= tolerance:
        return [start, end]
    left = _douglas_peucker(points[: split_index + 1], tolerance)
    right = _douglas_peucker(points[split_index:], tolerance)
    return left[:-1] + right


def _simplify_ring(ring: list, tolerance: float, precision: int = 4) -> list:
    points = [[round(float(point[0]), precision), round(float(point[1]), precision)] for point in ring]
    if len(points) < 5:
        return points
    core = points[:-1] if points[0] == points[-1] else points
    center_x = sum(point[0] for point in core) / len(core)
    center_y = sum(point[1] for point in core) / len(core)
    anchor = max(
        range(len(core)),
        key=lambda index: (core[index][0] - center_x) ** 2 + (core[index][1] - center_y) ** 2,
    )
    rotated = core[anchor:] + core[:anchor] + [core[anchor]]
    simplified = _douglas_peucker(rotated, tolerance)
    if len(simplified) < 4:
        simplified = rotated[:3] + [rotated[0]]
    elif simplified[0] != simplified[-1]:
        simplified.append(simplified[0])
    return simplified


def _simplify_geometry(geometry: dict, tolerance: float = 0.0002) -> None:
    """Reduce browser/WebGL payload while preserving taxi-zone topology visually."""
    coordinates = geometry.get("coordinates")
    if not coordinates:
        return
    if geometry.get("type") == "Polygon":
        geometry["coordinates"] = [_simplify_ring(ring, tolerance) for ring in coordinates]
    elif geometry.get("type") == "MultiPolygon":
        geometry["coordinates"] = [
            [_simplify_ring(ring, tolerance) for ring in polygon]
            for polygon in coordinates
        ]


@lru_cache(maxsize=1)
def load_taxi_zone_geojson(path: str | None = None) -> dict:
    geo_path = Path(path) if path else GEOJSON_PATH
    data = json.loads(geo_path.read_text(encoding="utf-8"))
    for feat in data.get("features", []):
        geom = feat.get("geometry")
        if geom:
            _simplify_geometry(geom)
    return data


def attach_demand_to_geojson(
    geojson: dict,
    live_view: pd.DataFrame,
    metric: str = "demand_ratio",
    dark_mode: bool = True,
) -> dict:
    """Copy TLC polygons and paint fill_color and borders from Live Gold metrics."""
    by_zone: dict[int, dict] = {}
    if not live_view.empty:
        for row in live_view.itertuples(index=False):
            by_zone[int(row.pickup_zone_id)] = {
                "demand_ratio": getattr(row, "demand_ratio", None),
                "current_trip_count": getattr(row, "current_trip_count", None),
                "historical_mean": getattr(row, "historical_mean_trip_count", None),
                "z_score": getattr(row, "z_score", None),
            }

    features = []
    for feat in geojson.get("features", []):
        props = dict(feat.get("properties", {}))
        loc = int(props.get("location_id", 0))
        data = by_zone.get(loc)

        ratio = None if data is None else data["demand_ratio"]
        ratio = None if ratio is not None and pd.isna(ratio) else (None if ratio is None else float(ratio))
        trips = None if data is None else data["current_trip_count"]
        trips = None if trips is not None and pd.isna(trips) else (None if trips is None else int(trips))
        mean = None if data is None else data["historical_mean"]
        mean = None if mean is not None and pd.isna(mean) else (None if mean is None else float(mean))
        z = None if data is None else data["z_score"]
        z = None if z is not None and pd.isna(z) else (None if z is None else float(z))

        is_active = trips is not None and trips > 0
        zone_title = props.get("zone", f"Zone {loc}")
        borough = props.get("borough", "Unknown")

        # Color calculation based on selected metric
        if metric == "current_trip_count":
            color = trip_count_fill_color(trips, dark_mode=dark_mode)
            ratio_hex = "#38bdf8"
        elif metric == "z_score":
            color = z_score_fill_color(z, dark_mode=dark_mode)
            ratio_hex = "#a855f7" if z and z >= 2.0 else "#38bdf8"
        else:  # default "demand_ratio"
            color = ratio_fill_color(ratio, dark_mode=dark_mode)
            if ratio is not None:
                ratio_hex = "#ef4444" if ratio > HIGH_DEMAND_RATIO else ("#f59e0b" if ratio >= 1.0 else "#10b981")
            else:
                ratio_hex = "#94a3b8"

        # Boundary line color: crisp high contrast for active zones; subtle for inactive
        if is_active:
            line_color = [255, 255, 255, 220] if dark_mode else [15, 23, 42, 220]
        else:
            line_color = [71, 85, 105, 160] if dark_mode else [148, 163, 184, 180]

        # 3D Elevation computation
        if is_active:
            if metric == "current_trip_count" and trips is not None:
                elevation = max(20.0, float(trips) * 35.0)
            elif ratio is not None:
                elevation = max(20.0, float(ratio) * 300.0)
            else:
                elevation = 50.0
        else:
            elevation = 0.0

        props["location_id"] = loc
        props["zone"] = zone_title
        props["zone_name"] = zone_title
        props["borough"] = borough
        props["zone_label"] = f"{loc} — {zone_title}"
        props["demand_ratio"] = ratio
        props["demand_ratio_str"] = f"{ratio:.2f}×" if ratio is not None else "—"
        props["current_trip_count"] = trips
        props["current_trip_count_str"] = f"{trips:,} trips" if trips is not None else "0 (No activity)"
        props["historical_mean"] = mean
        props["historical_mean_str"] = f"{mean:.1f} trips" if mean is not None else "—"
        props["z_score"] = z
        props["z_score_str"] = f"{z:+.2f}σ" if z is not None else "—"
        props["ratio_color_hex"] = ratio_hex
        props["is_active"] = is_active
        props["elevation"] = elevation

        props["fill_color"] = color
        props["fill_r"] = color[0]
        props["fill_g"] = color[1]
        props["fill_b"] = color[2]
        props["fill_a"] = color[3]

        props["line_color"] = line_color
        props["line_r"] = line_color[0]
        props["line_g"] = line_color[1]
        props["line_b"] = line_color[2]
        props["line_a"] = line_color[3]

        features.append({
            "type": feat.get("type", "Feature"),
            "geometry": feat.get("geometry"),
            "properties": props,
        })
    return {"type": geojson.get("type", "FeatureCollection"), "features": features}
