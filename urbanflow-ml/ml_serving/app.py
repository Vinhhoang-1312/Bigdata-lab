"""HTTP boundary for GTQN forecasting and its experimental evidence."""
from __future__ import annotations

import math
import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

from ml_serving.model_store import get_model_store
from ml_serving.trip_anomalies import (
    DEFAULT_RUN_ID,
    get_anomaly_snapshot,
    score_trip,
)


class ForecastRequest(BaseModel):
    zone_id: int = Field(gt=0)
    forecast_at: str


class TripScoreRequest(BaseModel):
    pickup_zone_id: int = Field(gt=0)
    dropoff_zone_id: int = Field(gt=0)
    trip_distance_miles: float = Field(gt=0)
    trip_duration_minutes: float = Field(gt=0)
    fare_amount_usd: float = Field(ge=0)

    @field_validator("trip_distance_miles", "trip_duration_minutes", "fare_amount_usd")
    @classmethod
    def require_finite_measurement(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("trip measurements must be finite")
        return value


def _store():
    try:
        return get_model_store()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Model artifact unavailable: {type(exc).__name__}") from exc


def create_app() -> FastAPI:
    app = FastAPI(title="UrbanFlow ML API", version="1.0.0")

    @app.get("/v1/forecast/options")
    def options() -> dict:
        return _store().options()

    @app.get("/v1/forecast/evidence")
    def evidence() -> dict:
        return _store().evidence()

    @app.post("/v1/forecast")
    def forecast(request: ForecastRequest) -> dict:
        try:
            return _store().forecast(request.zone_id, request.forecast_at)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/v1/anomaly/trips")
    def anomaly_trips() -> dict:
        try:
            return get_anomaly_snapshot(os.getenv("URBANFLOW_TRIP_ANOMALY_RUN_ID", DEFAULT_RUN_ID))
        except Exception as exc:
            raise HTTPException(status_code=503, detail="Trip anomaly artifact is unavailable") from exc

    @app.post("/v1/anomaly/score")
    def anomaly_score(request: TripScoreRequest) -> dict:
        run_id = os.getenv("URBANFLOW_TRIP_ANOMALY_RUN_ID", DEFAULT_RUN_ID)
        try:
            snapshot = get_anomaly_snapshot(run_id)
        except Exception as exc:
            raise HTTPException(status_code=503, detail="Trip anomaly artifact is unavailable") from exc
        try:
            return score_trip(
                snapshot,
                distance_miles=request.trip_distance_miles,
                duration_minutes=request.trip_duration_minutes,
                fare_usd=request.fare_amount_usd,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    return app


app = create_app()
