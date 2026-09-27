"""HTTP boundary for read-only UrbanFlow Streaming status."""
from __future__ import annotations

import os
from collections.abc import Callable
from datetime import datetime, timezone
from functools import lru_cache
from threading import Lock
import time
from typing import TypeVar

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from analytics_app.data_access import GoldReader
from analytics_app.zones import load_zone_lookup
from analytics_app.zones import GEOJSON_PATH
from serving.streaming_service import build_replay_catalog, build_streaming_health, build_streaming_overview
from serving.spark_status import read_stream_job_status

T = TypeVar("T")
_live_cache: dict[str, tuple[float, object]] = {}
_live_lock = Lock()


def read_live_snapshot(reader: GoldReader, *, refresh: bool = False, at: float | None = None):
    """Coalesce concurrent S3 reads; an explicit refresh bypasses the 60-second cache."""
    now = time.monotonic() if at is None else at
    with _live_lock:
        cached = _live_cache.get(reader.live_path)
        if cached and not refresh and now - cached[0] < 60:
            return cached[1]
        live = reader.live()
        _live_cache[reader.live_path] = (now, live)
        return live


@lru_cache(maxsize=8)
def _lookup(path: str):
    return load_zone_lookup(path)


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
    def overview(refresh: bool = False) -> dict:
        reader = GoldReader(_required_path("URBANFLOW_HISTORICAL_PATH"), _required_path("URBANFLOW_LIVE_PATH"))
        return read_gold_or_503(
            lambda: build_streaming_overview(
                reader, _lookup(_required_path("URBANFLOW_ZONE_LOOKUP_PATH")),
                live=read_live_snapshot(reader, refresh=refresh),
            )
        )

    @app.get("/v1/streaming/health")
    def health(refresh: bool = False) -> dict:
        reader = GoldReader(_required_path("URBANFLOW_HISTORICAL_PATH"), _required_path("URBANFLOW_LIVE_PATH"))
        return read_gold_or_503(lambda: build_streaming_health(
            reader, telemetry=None, now=datetime.now(timezone.utc),
            live=read_live_snapshot(reader, refresh=refresh),
            spark_stream_status=read_stream_job_status(
                os.getenv("URBANFLOW_SPARK_MASTER_STATUS_URL", "http://spark-master:8080/json")
            ),
        ))

    @app.get("/v1/streaming/zones")
    def zones() -> FileResponse:
        return FileResponse(GEOJSON_PATH, media_type="application/geo+json")

    @app.get("/v1/streaming/replay/catalog")
    def replay_catalog(refresh: bool = False) -> dict:
        reader = GoldReader(_required_path("URBANFLOW_HISTORICAL_PATH"), _required_path("URBANFLOW_LIVE_PATH"))
        return read_gold_or_503(lambda: build_replay_catalog(read_live_snapshot(reader, refresh=refresh)))

    @app.get("/v1/streaming/replay/frame")
    def replay_frame(window_start: str, refresh: bool = False) -> dict:
        try:
            selected = datetime.fromisoformat(window_start)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="Invalid window_start timestamp") from exc
        if selected.tzinfo is None or selected.utcoffset() is None:
            raise HTTPException(status_code=422, detail="window_start requires a UTC offset")
        reader = GoldReader(_required_path("URBANFLOW_HISTORICAL_PATH"), _required_path("URBANFLOW_LIVE_PATH"))

        def read_frame() -> dict:
            live = read_live_snapshot(reader, refresh=refresh)
            at = selected.astimezone(timezone.utc)
            rows = live.loc[live["window_start"] == at]
            if rows.empty:
                raise HTTPException(status_code=404, detail="Gold window is not stored")
            return build_streaming_overview(
                reader, _lookup(_required_path("URBANFLOW_ZONE_LOOKUP_PATH")), live=rows,
            )

        return read_gold_or_503(read_frame)

    return app


app = create_app()
