"""Taxi-zone labels and map centroids for the analytics app (not ETL)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

# Approximate TLC zone centroids (WGS84). Enough for a demo scatter map.
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
    frame = pd.read_csv(path)
    required = {"LocationID", "Zone"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"zone lookup missing columns: {sorted(missing)}")
    out = frame.rename(columns={"LocationID": "pickup_zone_id", "Zone": "zone_name"})
    return out.loc[:, ["pickup_zone_id", "zone_name"]].drop_duplicates("pickup_zone_id")
