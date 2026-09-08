"""Replay configuration loading and frozen-value validation."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

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


REPO_ROOT = Path(__file__).resolve().parents[2]


def _resolve_fixture_path(raw_path: str, config_file: Path) -> str:
    path = Path(raw_path)
    if path.exists():
        return str(path)
    if raw_path.startswith("/workspace/"):
        candidate = REPO_ROOT / raw_path.removeprefix("/workspace/")
        if candidate.exists():
            return str(candidate)
    candidate = REPO_ROOT / path
    if candidate.exists():
        return str(candidate)
    candidate = config_file.parent / path
    if candidate.exists():
        return str(candidate)
    return raw_path


def load_config(path: str | Path) -> ReplayConfig:
    config_path = Path(path).resolve()
    with config_path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    config = ReplayConfig(
        foundation_version=str(raw["foundation_version"]),
        analytics_timezone=str(raw["analytics_timezone"]),
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            str(raw["kafka"]["bootstrap_servers"]),
        ),
        topic=str(raw["kafka"]["topic"]),
        fixture_path=_resolve_fixture_path(
            str(raw["input"]["fixture_path"]), config_path
        ),
        integrated_silver_uri=str(raw["input"]["parquet_uri"]),
        s3_endpoint=str(raw["input"]["s3_endpoint"]),
        s3_region=str(raw["input"]["s3_region"]),
        replay_start_date=str(raw["replay"]["start_date"]),
        replay_end_date=str(raw["replay"]["end_date"]),
        planned_rates=tuple(int(rate) for rate in raw["replay"]["planned_rates"]),
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
