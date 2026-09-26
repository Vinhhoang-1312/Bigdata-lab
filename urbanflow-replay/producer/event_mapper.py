"""Projection from the Foundation Silver contract to Kafka trip events."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from typing import Any

from producer.event_time import event_time_id_parts

EVENT_FIELDS = (
    "event_id",
    "event_time",
    "pickup_zone_id",
    "dropoff_zone_id",
    "trip_distance",
    "trip_duration_minutes",
    "fare_amount",
)
SILVER_EVENT_FIELDS = (
    "pickup_datetime",
    "pickup_zone_id",
    "dropoff_zone_id",
    "trip_distance",
    "trip_duration_minutes",
    "fare_amount",
)


class EventMapper:
    """Create traceable IDs and project Silver rows without redoing Batch ETL."""

    def __init__(self, run_id: str | None = None) -> None:
        if run_id is not None and not run_id.strip():
            raise ValueError("run_id must not be blank")
        self._run_id = run_id
        self._zone_sequences: dict[int, int] = defaultdict(int)

    def map_record(self, silver: Mapping[str, Any]) -> dict[str, Any]:
        missing = [field for field in SILVER_EVENT_FIELDS if field not in silver]
        if missing:
            raise ValueError(f"Silver record is missing fields: {missing}")

        event_time = silver["pickup_datetime"]
        pickup_zone_id = silver["pickup_zone_id"]
        if not isinstance(event_time, str):
            raise ValueError("pickup_datetime must be a string")
        if not isinstance(pickup_zone_id, int) or isinstance(pickup_zone_id, bool):
            raise ValueError("pickup_zone_id must be an integer")

        self._zone_sequences[pickup_zone_id] += 1
        sequence = self._zone_sequences[pickup_zone_id]
        event_id = (
            f"trip-{event_time_id_parts(event_time)}-{pickup_zone_id}-{sequence:03d}"
        )
        if self._run_id is not None:
            event_id = f"{event_id}-{self._run_id}"

        return {
            "event_id": event_id,
            "event_time": event_time,
            "pickup_zone_id": pickup_zone_id,
            "dropoff_zone_id": silver["dropoff_zone_id"],
            "trip_distance": silver["trip_distance"],
            "trip_duration_minutes": silver["trip_duration_minutes"],
            "fare_amount": silver["fare_amount"],
        }
