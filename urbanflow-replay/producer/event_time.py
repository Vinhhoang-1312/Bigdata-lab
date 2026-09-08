"""Foundation-compliant RFC 3339 and New York offset handling."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

NYC = ZoneInfo("America/New_York")
RFC3339_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})$"
)


class EventTimeError(ValueError):
    """Raised when a timestamp violates Foundation event-time semantics."""


def parse_event_time(value: object) -> datetime:
    if not isinstance(value, str) or not RFC3339_PATTERN.fullmatch(value):
        raise EventTimeError("event_time must be RFC 3339 with an explicit offset")
    try:
        normalized = value[:-1] + "+00:00" if value[-1] in "Zz" else value
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise EventTimeError("event_time is not a valid timestamp") from error
    if parsed.utcoffset() is None:
        raise EventTimeError("event_time must include an explicit offset")

    wall = parsed.replace(tzinfo=None)
    canonical = wall.replace(tzinfo=NYC, fold=0)
    round_trip = canonical.astimezone(UTC).astimezone(NYC).replace(tzinfo=None)
    if round_trip != wall:
        raise EventTimeError("event_time is in the nonexistent New York DST hour")
    if parsed.utcoffset() != canonical.utcoffset():
        raise EventTimeError("event_time offset does not match America/New_York")
    return parsed


def event_time_id_parts(value: str) -> str:
    parsed = parse_event_time(value)
    return parsed.strftime("%Y%m%d-%H%M%S")
