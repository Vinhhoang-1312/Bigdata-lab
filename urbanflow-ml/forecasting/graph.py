"""Training-only directed zone graph construction."""

from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, Mapping

import numpy as np


def _trip_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()


def build_od_adjacency(
    trips: Iterable[Mapping[str, object]],
    zone_ids: Iterable[int],
    train_end: str | date,
) -> np.ndarray:
    zones = sorted({int(zone_id) for zone_id in zone_ids})
    if not zones:
        raise ValueError("zone_ids must not be empty")
    cutoff = train_end if isinstance(train_end, date) else date.fromisoformat(str(train_end))
    index = {zone_id: position for position, zone_id in enumerate(zones)}
    adjacency = np.eye(len(zones), dtype=np.float64)

    for trip in trips:
        if _trip_date(trip["pickup_datetime"]) > cutoff:
            continue
        pickup = int(trip["pickup_zone_id"])
        dropoff = int(trip["dropoff_zone_id"])
        if pickup in index and dropoff in index:
            weight = float(trip.get("edge_count", 1.0))
            if not np.isfinite(weight) or weight < 0:
                raise ValueError(f"invalid edge_count: {weight}")
            adjacency[index[pickup], index[dropoff]] += weight

    row_sums = adjacency.sum(axis=1, keepdims=True)
    return (adjacency / row_sums).astype(np.float32)
