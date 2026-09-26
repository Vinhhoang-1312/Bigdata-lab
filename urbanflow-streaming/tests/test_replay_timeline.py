"""Monthly map playback uses only Gold windows that actually exist."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from serving.app import create_app
from serving.streaming_service import build_replay_catalog


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "urbanflow-foundation" / "fixtures"


def test_catalog_groups_by_new_york_month_without_inventing_missing_windows():
    live = pd.DataFrame(
        {
            "window_start": pd.to_datetime(
                [
                    "2023-02-01T04:55:00Z",  # January 31 in New York.
                    "2023-02-01T05:00:00Z",
                    "2023-02-01T05:00:00Z",  # Two zones in one window.
                    "2023-02-03T06:00:00Z",  # Gap is not a zero-filled frame.
                ], utc=True
            ),
            "current_trip_count": [7, 2, 3, 4],
        }
    )

    catalog = build_replay_catalog(live)

    assert [month["month"] for month in catalog["months"]] == ["2023-01", "2023-02"]
    february = catalog["months"][1]
    assert february["windows"] == ["2023-02-01T05:00:00+00:00", "2023-02-03T06:00:00+00:00"]
    assert february["window_count"] == 2
    assert february["observed_day_count"] == 2
    assert february["hourly_pickups"][0] == 5
    assert february["hourly_pickups"][1] == 4
    assert sum(february["hourly_pickups"]) == 9


def test_frame_route_returns_selected_gold_window_and_rejects_unknown_time(monkeypatch):
    monkeypatch.setenv("URBANFLOW_HISTORICAL_PATH", str(FIXTURES / "gold/historical_zone_baseline.jsonl"))
    monkeypatch.setenv("URBANFLOW_LIVE_PATH", str(FIXTURES / "gold/expected_live_zone_metrics.jsonl"))
    monkeypatch.setenv("URBANFLOW_ZONE_LOOKUP_PATH", str(FIXTURES / "bronze/zone_lookup.csv"))
    client = TestClient(create_app())

    catalog = client.get("/v1/streaming/replay/catalog")
    assert catalog.status_code == 200
    assert catalog.json()["months"][0]["window_count"] == 1

    frame = client.get("/v1/streaming/replay/frame", params={"window_start": "2025-01-06T13:00:00+00:00"})
    assert frame.status_code == 200
    assert frame.json()["current_trip_count"] == 7
    assert len(frame.json()["latest_rows"]) == 2

    missing = client.get("/v1/streaming/replay/frame", params={"window_start": "2025-01-06T13:05:00+00:00"})
    assert missing.status_code == 404
    invalid = client.get("/v1/streaming/replay/frame", params={"window_start": "2025-01-06T13:00:00"})
    assert invalid.status_code == 422
