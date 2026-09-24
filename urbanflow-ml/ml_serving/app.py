"""HTTP boundary for GTQN forecasting and its experimental evidence."""
from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from ml_serving.model_store import get_model_store


class ForecastRequest(BaseModel):
    zone_id: int = Field(gt=0)
    forecast_at: str


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

    return app


app = create_app()
