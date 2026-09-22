"""HTTP boundary for read-only UrbanFlow Streaming status."""
from __future__ import annotations

import os
from collections.abc import Callable
from datetime import datetime, timezone
from typing import TypeVar

from fastapi import FastAPI, HTTPException

from analytics_app.data_access import GoldReader
from analytics_app.zones import load_zone_lookup
from serving.streaming_service import build_streaming_health, build_streaming_overview

T = TypeVar("T")


def _required_path(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise HTTPException(status_code=503, detail=f"{name} is not configured")
    return value


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

    @app.get("/v1/streaming/overview")
    def overview() -> dict:
        reader = GoldReader(_required_path("URBANFLOW_HISTORICAL_PATH"), _required_path("URBANFLOW_LIVE_PATH"))
        return read_gold_or_503(
            lambda: build_streaming_overview(reader, load_zone_lookup(_required_path("URBANFLOW_ZONE_LOOKUP_PATH")))
        )

    @app.get("/v1/streaming/health")
    def health() -> dict:
        reader = GoldReader(_required_path("URBANFLOW_HISTORICAL_PATH"), _required_path("URBANFLOW_LIVE_PATH"))
        return read_gold_or_503(lambda: build_streaming_health(reader, telemetry=None, now=datetime.now(timezone.utc)))

    return app


app = create_app()
