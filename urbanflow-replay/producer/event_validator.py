"""Strict Foundation Kafka event validation."""

from __future__ import annotations

import math
from collections.abc import Collection, Mapping
from typing import Any

from producer.event_mapper import EVENT_FIELDS
from producer.event_time import EventTimeError, parse_event_time


class EventValidator:
    def __init__(
        self,
        valid_zone_ids: Collection[int] | None = None,
        *,
        track_duplicates: bool = True,
    ) -> None:
        self._valid_zone_ids = (
            set(valid_zone_ids) if valid_zone_ids is not None else None
        )
        self._track_duplicates = track_duplicates
        self._seen_event_ids: set[str] = set()

    def validate(self, event: Mapping[str, Any]) -> list[str]:
        errors: list[str] = []
        actual_fields = set(event)
        expected_fields = set(EVENT_FIELDS)
        missing = sorted(expected_fields - actual_fields)
        extra = sorted(actual_fields - expected_fields)
        if missing:
            errors.append(f"missing_fields:{','.join(missing)}")
        if extra:
            errors.append(f"extra_fields:{','.join(extra)}")
        if missing:
            return errors

        event_id = event["event_id"]
        id_is_valid = isinstance(event_id, str) and bool(event_id.strip())
        if not id_is_valid:
            errors.append("event_id_invalid")
        elif self._track_duplicates and event_id in self._seen_event_ids:
            errors.append("event_id_duplicate")

        try:
            parse_event_time(event["event_time"])
        except EventTimeError:
            errors.append("event_time_invalid")

        for field in ("pickup_zone_id", "dropoff_zone_id"):
            value = event[field]
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                errors.append(f"{field}_invalid")
            elif self._valid_zone_ids is not None and value not in self._valid_zone_ids:
                errors.append(f"{field}_unknown")

        for field in ("trip_distance", "trip_duration_minutes", "fare_amount"):
            value = event[field]
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
                or value < 0
            ):
                errors.append(f"{field}_invalid")

        if not errors and id_is_valid and self._track_duplicates:
            self._seen_event_ids.add(event_id)
        return errors
