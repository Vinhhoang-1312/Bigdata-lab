"""Replay configuration loading and frozen-value validation."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

FOUNDATION_VERSION = "1.0.0"
ANALYTICS_TIMEZONE = "America/New_York"
TOPIC = "urbanflow.trip-events"
REPLAY_START_DATE = "2023-02-01"
REPLAY_END_DATE = "2023-02-28"


@dataclass(frozen=True)
class ReplayConfig:
    foundation_version: str
    analytics_timezone: str
    bootstrap_servers: str
    topic: str
    fixture_path: str
    integrated_silver_uri: str
    s3_endpoint: str
    s3_region: str
    replay_start_date: str
    replay_end_date: str
    planned_rates: tuple[int, ...]


def load_config(path: str | Path) -> ReplayConfig:
    with Path(path).open(encoding="utf-8") as handle:
        raw = json.load(handle)

    config = ReplayConfig(
        foundation_version=str(raw["foundation_version"]),
        analytics_timezone=str(raw["analytics_timezone"]),
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", str(raw["bootstrap_servers"])
        ),
        topic=str(raw["topic"]),
        fixture_path=str(raw["fixture_path"]),
        integrated_silver_uri=str(raw["integrated_silver_uri"]),
        s3_endpoint=str(raw["s3_endpoint"]),
        s3_region=str(raw["s3_region"]),
        replay_start_date=str(raw["replay_start_date"]),
        replay_end_date=str(raw["replay_end_date"]),
        planned_rates=tuple(int(rate) for rate in raw["planned_rates"]),
    )
    _validate_frozen_values(config)
    return config


def _validate_frozen_values(config: ReplayConfig) -> None:
    expected = {
        "foundation_version": FOUNDATION_VERSION,
        "analytics_timezone": ANALYTICS_TIMEZONE,
        "topic": TOPIC,
        "replay_start_date": REPLAY_START_DATE,
        "replay_end_date": REPLAY_END_DATE,
    }
    for field, value in expected.items():
        actual = getattr(config, field)
        if actual != value:
            raise ValueError(f"{field} must be {value!r}, got {actual!r}")
    if not config.bootstrap_servers.strip():
        raise ValueError("bootstrap_servers must not be blank")
    if not config.integrated_silver_uri.startswith("s3a://"):
        raise ValueError("integrated_silver_uri must use s3a://")
    if not config.planned_rates or any(rate <= 0 for rate in config.planned_rates):
        raise ValueError("planned_rates must contain positive integers")
