"""Strict JSON serialization for Kafka event values."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any


def serialize_event(event: Mapping[str, Any]) -> bytes:
    return json.dumps(
        event,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def message_key(event: Mapping[str, Any]) -> bytes:
    return str(event["pickup_zone_id"]).encode("ascii")
