"""Compact, deterministic tensor artifacts for zone-demand forecasting."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Iterable, Mapping
from zoneinfo import ZoneInfo

import numpy as np


TIMEZONE = ZoneInfo("America/New_York")
CALENDAR_COLUMNS = [
    "half_hour_sin",
    "half_hour_cos",
    "weekday_sin",
    "weekday_cos",
    "is_weekend",
    "month_sin",
    "month_cos",
]


def _as_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _split_indices(start: date, days: int, split_config: Mapping[str, str]) -> dict[str, list[int]]:
    result: dict[str, list[int]] = {}
    total = days * 48
    for name in ("train", "validation", "test"):
        split_start = _as_date(split_config[f"{name}_start"])
        split_end = _as_date(split_config[f"{name}_end"])
        left = max(0, (split_start - start).days * 48)
        right = min(total, ((split_end - start).days + 1) * 48)
        if left >= right:
            raise ValueError(f"{name} split has no target buckets in the artifact")
        result[name] = [left, right]
    return result


def _calendar_features(current_date: date, slot: int) -> list[float]:
    half_hour_angle = 2.0 * math.pi * slot / 48.0
    weekday_angle = 2.0 * math.pi * current_date.weekday() / 7.0
    month_angle = 2.0 * math.pi * (current_date.month - 1) / 12.0
    return [
        math.sin(half_hour_angle),
        math.cos(half_hour_angle),
        math.sin(weekday_angle),
        math.cos(weekday_angle),
        float(current_date.weekday() >= 5),
        math.sin(month_angle),
        math.cos(month_angle),
    ]


def dataset_fingerprint(arrays: Mapping[str, np.ndarray], split_indices: Mapping[str, list[int]]) -> str:
    digest = hashlib.sha256()
    for name in sorted(arrays):
        value = np.ascontiguousarray(arrays[name])
        digest.update(name.encode("utf-8"))
        digest.update(str(value.dtype).encode("ascii"))
        digest.update(json.dumps(value.shape).encode("ascii"))
        digest.update(value.tobytes())
    digest.update(json.dumps(split_indices, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    return digest.hexdigest()


def _value(item, name: str):
    if isinstance(item, Mapping):
        return item[name]
    return getattr(item, name)


def _expanded_rows(rows: list[Mapping[str, object]]):
    for row in rows:
        if "zone_counts" not in row:
            yield row, None
            continue
        for zone_count in row["zone_counts"]:
            yield (
                {
                    "service_date": row["service_date"],
                    "half_hour_slot": row["half_hour_slot"],
                    "pickup_zone_id": _value(zone_count, "pickup_zone_id"),
                    "trip_count": _value(zone_count, "trip_count"),
                },
                bool(row.get("source_observed", True)),
            )


def build_training_arrays(
    demand_rows: Iterable[Mapping[str, object]],
    zone_ids: Iterable[int],
    split_config: Mapping[str, str],
) -> tuple[dict[str, np.ndarray], dict]:
    zones = sorted({int(zone_id) for zone_id in zone_ids})
    if not zones:
        raise ValueError("zone_ids must not be empty")
    zone_index = {zone_id: index for index, zone_id in enumerate(zones)}

    rows = list(demand_rows)
    if not rows:
        raise ValueError("demand_rows must not be empty")
    start = min(_as_date(row["service_date"]) for row in rows)
    end = max(_as_date(row["service_date"]) for row in rows)
    days = (end - start).days + 1
    time_count = days * 48
    counts = np.zeros((time_count, len(zones)), dtype=np.float32)
    bucket_present = np.zeros(time_count, dtype=np.bool_)
    seen: set[tuple[int, int]] = set()

    for row, explicit_observed in _expanded_rows(rows):
        current_date = _as_date(row["service_date"])
        slot = int(row["half_hour_slot"])
        zone_id = int(row["pickup_zone_id"])
        count = float(row["trip_count"])
        if not 0 <= slot < 48:
            raise ValueError(f"half_hour_slot out of range: {slot}")
        if zone_id not in zone_index:
            raise ValueError(f"unknown pickup_zone_id: {zone_id}")
        if not math.isfinite(count) or count < 0:
            raise ValueError(f"invalid trip_count: {count}")
        time_index = (current_date - start).days * 48 + slot
        key = (time_index, zone_id)
        if key in seen:
            raise ValueError(f"duplicate demand row for time index {time_index}, zone {zone_id}")
        seen.add(key)
        counts[time_index, zone_index[zone_id]] = count
        if explicit_observed is None:
            bucket_present[time_index] = True
        else:
            bucket_present[time_index] = explicit_observed

    observed = np.repeat(bucket_present[:, None], len(zones), axis=1)
    calendar = np.empty((time_count, len(CALENDAR_COLUMNS)), dtype=np.float32)
    timestamp_values: list[str] = []
    for index in range(time_count):
        current_date = start + timedelta(days=index // 48)
        slot = index % 48
        calendar[index] = _calendar_features(current_date, slot)
        local_time = time(hour=slot // 2, minute=(slot % 2) * 30, tzinfo=TIMEZONE)
        timestamp_values.append(datetime.combine(current_date, local_time).isoformat())

    splits = _split_indices(start, days, split_config)
    arrays = {
        "counts": counts,
        "observed": observed,
        "calendar": calendar,
        "zone_ids": np.asarray(zones, dtype=np.int32),
        "timestamps": np.asarray(timestamp_values),
    }
    fingerprint = dataset_fingerprint(arrays, splits)
    metadata = {
        "timezone": str(TIMEZONE),
        "bucket_minutes": 30,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "time_count": time_count,
        "zone_count": len(zones),
        "calendar_columns": CALENDAR_COLUMNS,
        "split_indices": splits,
        "dataset_fingerprint": fingerprint,
        "unobserved_bucket_count": int((~bucket_present).sum()),
    }
    return arrays, metadata


def save_tensor_artifact(path: str | Path, arrays: Mapping[str, np.ndarray], metadata: Mapping[str, object]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {name: np.asarray(value) for name, value in arrays.items()}
    payload["metadata_json"] = np.asarray(json.dumps(metadata, sort_keys=True, separators=(",", ":")))
    np.savez_compressed(target, **payload)


def load_tensor_artifact(path: str | Path) -> tuple[dict[str, np.ndarray], dict]:
    with np.load(Path(path), allow_pickle=False) as archive:
        metadata = json.loads(str(archive["metadata_json"].item()))
        arrays = {name: archive[name].copy() for name in archive.files if name != "metadata_json"}
    fingerprint = dataset_fingerprint(arrays, metadata["split_indices"])
    if fingerprint != metadata["dataset_fingerprint"]:
        raise ValueError("tensor artifact fingerprint mismatch")
    return arrays, metadata
