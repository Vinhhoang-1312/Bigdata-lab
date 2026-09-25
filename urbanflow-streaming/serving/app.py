"""HTTP boundary for read-only UrbanFlow Streaming status and analytics."""
from __future__ import annotations

import os
from collections.abc import Callable
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, TypeVar

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict

from analytics_app.data_access import GoldReader
from analytics_app.zones import load_zone_lookup
from observability.telemetry_reader import read_telemetry
from serving.streaming_service import build_streaming_health, build_streaming_overview

T = TypeVar("T")


class ServiceHealth(BaseModel):
    status: str


class ReadinessHealth(BaseModel):
    status: str
    telemetry_age_seconds: int | None = None


class PipelineStatus(BaseModel):
    model_config = ConfigDict(extra="allow")

    status: str
    reason: str
    pipeline: dict[str, Any]
    spark_telemetry: dict[str, Any] | None
    watermark_status: dict[str, Any]
    kafka_backlog: dict[str, Any]
    data_governance: dict[str, Any]
    live_gold_freshness: dict[str, Any]


class AnalyticsOverview(BaseModel):
    status: str
    latest_window: str | None
    latest_rows: list[dict[str, Any]]
    current_trip_count: int
    high_demand_zone_count: int = 0


def _required_path(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise HTTPException(status_code=503, detail=f"{name} is not configured")
    return value


@lru_cache(maxsize=1)
def _configured_reader() -> GoldReader:
    return GoldReader(
        _required_path("URBANFLOW_HISTORICAL_PATH"),
        _required_path("URBANFLOW_LIVE_PATH"),
    )


@lru_cache(maxsize=1)
def _configured_lookup():
    return load_zone_lookup(_required_path("URBANFLOW_ZONE_LOOKUP_PATH"))


def read_gold_or_503(read: Callable[[], T]) -> T:
    """Keep storage and schema failures at a stable service boundary."""
    try:
        return read()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Gold data is temporarily unavailable") from exc


def create_app() -> FastAPI:
    app = FastAPI(title="UrbanFlow Streaming API", version="1.0.0")

    def overview_document() -> dict[str, Any]:
        return read_gold_or_503(
            lambda: build_streaming_overview(_configured_reader(), _configured_lookup())
        )

    def pipeline_document() -> dict[str, Any]:
        return read_gold_or_503(
            lambda: build_streaming_health(
                _configured_reader(), telemetry=read_telemetry(), now=datetime.now(timezone.utc)
            )
        )

    @app.get("/health/live", response_model=ServiceHealth)
    def liveness() -> dict[str, str]:
        """Process-only liveness probe; never contacts Kafka or object storage."""
        return {"status": "ok"}

    @app.get("/health/ready", response_model=ReadinessHealth)
    def readiness() -> dict[str, Any]:
        """Cheap readiness probe based on configuration and telemetry heartbeat."""
        for name in (
            "URBANFLOW_HISTORICAL_PATH",
            "URBANFLOW_LIVE_PATH",
            "URBANFLOW_ZONE_LOOKUP_PATH",
        ):
            _required_path(name)
        telemetry = read_telemetry()
        if telemetry is None:
            raise HTTPException(status_code=503, detail="Streaming telemetry is unavailable")
        timestamp = telemetry.get("timestamp")
        age = None
        if timestamp:
            try:
                age = max(
                    0,
                    int(
                        (
                            datetime.now(timezone.utc)
                            - datetime.fromisoformat(str(timestamp)).astimezone(timezone.utc)
                        ).total_seconds()
                    ),
                )
            except (TypeError, ValueError):
                age = None
        if age is None or age > 60:
            raise HTTPException(status_code=503, detail="Streaming telemetry is stale")
        return {"status": "ready", "telemetry_age_seconds": age}

    @app.get("/v1/analytics/latest", response_model=AnalyticsOverview)
    def analytics_latest() -> dict[str, Any]:
        return overview_document()

    @app.get("/v1/streaming/overview", response_model=AnalyticsOverview)
    def overview_legacy() -> dict[str, Any]:
        return overview_document()

    @app.get("/v1/pipeline/status", response_model=PipelineStatus)
    def pipeline_status() -> dict[str, Any]:
        return pipeline_document()

    @app.get("/v1/streaming/health", response_model=PipelineStatus)
    def health_legacy() -> dict[str, Any]:
        return pipeline_document()

    return app


app = create_app()
