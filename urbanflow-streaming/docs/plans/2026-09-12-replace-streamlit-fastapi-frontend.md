# Replace Streamlit with FastAPI + MapLibre Frontend

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Streamlit analytics UI with a thin FastAPI Gold API and a static MapLibre + Chart.js frontend that can refresh live via SSE, without changing Foundation 1.0.0 Gold contracts.

**Architecture:** Keep `analytics_app/data_access.py` and `zones.py` as the only Gold-read / paint helpers. Add `analytics_app/api.py` (FastAPI) that serves JSON + GeoJSON + SSE. Serve `analytics_app/static/` (HTML/CSS/JS) from FastAPI. Remove Streamlit as the primary UI; Spark pipeline unchanged.

**Tech Stack:** FastAPI, Uvicorn, Pandas (existing), MapLibre GL JS (CDN), Chart.js (CDN), Server-Sent Events for live refresh. No React build step for v1.

## Global Constraints

- Foundation version floor: `1.0.0` — do **not** change Silver/Kafka/Gold schemas or S3A paths for this work.
- Analytics remains thin: read Gold only; no ETL, no Spark inside the web process.
- Analytics timezone: `America/New_York` (via existing `to_analytics_local`).
- High-demand threshold: `HIGH_DEMAND_RATIO = 1.5` from `zones.py`.
- Default fixture paths stay under `urbanflow-foundation/fixtures/gold/` and `fixtures/bronze/zone_lookup.csv`.
- GeoJSON stays at `urbanflow-streaming/analytics_app/data/taxi_zones.geojson`.
- Branch: `urbanflow-streaming` (never push `main` unless asked).
- Framework swap is explicitly allowed by `urbanflow-foundation/docs/change-control.md` (“specific web framework may change”).

## File structure

| Path | Responsibility |
|------|----------------|
| `analytics_app/data_access.py` | Unchanged Gold readers + `prepare_live_view` |
| `analytics_app/zones.py` | Unchanged lookup + choropleth paint |
| `analytics_app/settings.py` | Path resolution from env / defaults (new) |
| `analytics_app/api.py` | FastAPI routes + SSE (new; replaces Streamlit entry) |
| `analytics_app/static/index.html` | Single-page UI shell (new) |
| `analytics_app/static/app.js` | Fetch API, MapLibre choropleth, charts, SSE (new) |
| `analytics_app/static/styles.css` | Layout / legend (new) |
| `analytics_app/app.py` | Delete or thin redirect note after cutover |
| `tests/test_api.py` | API contract tests (new) |
| `pyproject.toml` / `requirements/analytics.txt` | Swap streamlit→fastapi/uvicorn |
| `README.md` | New run commands |

---

### Task 1: Settings helper + API JSON endpoints (TDD)

**Files:**
- Create: `urbanflow-streaming/analytics_app/settings.py`
- Create: `urbanflow-streaming/analytics_app/api.py`
- Create: `urbanflow-streaming/tests/test_api.py`
- Modify: `urbanflow-streaming/pyproject.toml` (analytics extras)
- Modify: `urbanflow-streaming/requirements/analytics.txt` if it only pins extras

**Interfaces:**
- Consumes: `GoldReader`, `prepare_live_view`, `load_zone_lookup`, `load_taxi_zone_geojson`, `attach_demand_to_geojson`, `HIGH_DEMAND_RATIO`, `chart_mode_for`
- Produces:
  - `load_settings() -> AnalyticsSettings` with fields `historical_path: str`, `live_path: str`, `lookup_path: str`, `poll_seconds: float`
  - FastAPI app object named `app`
  - `GET /api/health` → `{"status":"ok"}`
  - `GET /api/live` → JSON list of live view rows (latest window by default; optional `zones`, `all_windows`)
  - `GET /api/map` → GeoJSON FeatureCollection with demand paint props
  - `GET /api/meta` → `{"high_demand_ratio": 1.5, "timezone":"America/New_York", "chart_mode": "...", "latest_window_start": "..."}`

- [ ] **Step 1: Update analytics dependencies**

In `pyproject.toml`, change:

```toml
analytics = [
  "fastapi>=0.115",
  "uvicorn[standard]>=0.30",
  "pandas>=2.2,<3",
  "pyarrow>=17",
  "s3fs>=2024.9",
]
```

Remove `streamlit`, `pydeck`, `altair` from the analytics extra.

Run from `urbanflow-streaming/`:

```bash
python -m pip install -e ".[analytics,dev]"
```

Expected: install succeeds.

- [ ] **Step 2: Write failing API tests**

Create `tests/test_api.py`:

```python
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
LIVE = ROOT / "urbanflow-foundation/fixtures/gold/expected_live_zone_metrics.jsonl"
HIST = ROOT / "urbanflow-foundation/fixtures/gold/historical_zone_baseline.jsonl"
LOOKUP = ROOT / "urbanflow-foundation/fixtures/bronze/zone_lookup.csv"


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setenv("URBANFLOW_HISTORICAL_PATH", str(HIST))
    monkeypatch.setenv("URBANFLOW_LIVE_PATH", str(LIVE))
    monkeypatch.setenv("URBANFLOW_LOOKUP_PATH", str(LOOKUP))
    from fastapi.testclient import TestClient
    from analytics_app.api import app

    return TestClient(app)


def test_health(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_live_latest_has_zone_labels(client):
    r = client.get("/api/live")
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert len(rows) >= 1
    assert "zone_label" in rows[0]
    assert "demand_ratio" in rows[0]
    assert "window_label" in rows[0]


def test_map_geojson_paints_live_zones(client):
    r = client.get("/api/map")
    assert r.status_code == 200
    body = r.json()
    assert body["type"] == "FeatureCollection"
    painted = [f for f in body["features"] if f["properties"].get("demand_ratio") is not None]
    assert len(painted) >= 1
    assert "fill_r" in painted[0]["properties"]


def test_meta_exposes_threshold(client):
    r = client.get("/api/meta")
    assert r.status_code == 200
    assert r.json()["high_demand_ratio"] == 1.5
    assert r.json()["timezone"] == "America/New_York"
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
cd urbanflow-streaming
pytest -q tests/test_api.py
```

Expected: FAIL (import / app missing).

- [ ] **Step 4: Implement `settings.py`**

```python
"""Analytics path settings (env + Foundation fixture defaults)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HISTORICAL = ROOT / "urbanflow-foundation/fixtures/gold/historical_zone_baseline.jsonl"
DEFAULT_LIVE = ROOT / "urbanflow-foundation/fixtures/gold/expected_live_zone_metrics.jsonl"
DEFAULT_LOOKUP = ROOT / "urbanflow-foundation/fixtures/bronze/zone_lookup.csv"


@dataclass(frozen=True)
class AnalyticsSettings:
    historical_path: str
    live_path: str
    lookup_path: str
    poll_seconds: float = 5.0


def load_settings() -> AnalyticsSettings:
    return AnalyticsSettings(
        historical_path=os.getenv("URBANFLOW_HISTORICAL_PATH", str(DEFAULT_HISTORICAL)),
        live_path=os.getenv("URBANFLOW_LIVE_PATH", str(DEFAULT_LIVE)),
        lookup_path=os.getenv("URBANFLOW_LOOKUP_PATH", str(DEFAULT_LOOKUP)),
        poll_seconds=float(os.getenv("URBANFLOW_POLL_SECONDS", "5")),
    )
```

- [ ] **Step 5: Implement `api.py` with health/live/map/meta**

Minimal shape:

```python
"""FastAPI Gold-serving API (no ETL)."""
from __future__ import annotations

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from analytics_app.data_access import GoldReader, chart_mode_for, prepare_live_view
from analytics_app.settings import load_settings
from analytics_app.zones import (
    HIGH_DEMAND_RATIO,
    attach_demand_to_geojson,
    load_taxi_zone_geojson,
    load_zone_lookup,
)

app = FastAPI(title="UrbanFlow Analytics")


def _view(all_windows: bool = False, zones: list[int] | None = None):
    settings = load_settings()
    reader = GoldReader(settings.historical_path, settings.live_path)
    live = reader.live() if all_windows else reader.latest()
    if zones:
        live = live[live.pickup_zone_id.isin(zones)]
    lookup = load_zone_lookup(settings.lookup_path)
    return prepare_live_view(live, lookup)


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/live")
def live(
    all_windows: bool = False,
    zones: list[int] | None = Query(default=None),
):
    view = _view(all_windows=all_windows, zones=zones)
    # Serialize timestamps as ISO strings; NaN z_score -> None
    rows = view.where(view.notna(), None).to_dict(orient="records")
    for row in rows:
        for key in ("window_start", "window_end", "window_start_local", "window_end_local"):
            if row.get(key) is not None:
                row[key] = str(row[key])
    return {"rows": rows}


@app.get("/api/map")
def map_geojson():
    view = _view(all_windows=False)
    return attach_demand_to_geojson(load_taxi_zone_geojson(), view)


@app.get("/api/meta")
def meta():
    view = _view(all_windows=True)
    latest = view["window_start_local"].max() if not view.empty else None
    return {
        "high_demand_ratio": HIGH_DEMAND_RATIO,
        "timezone": "America/New_York",
        "chart_mode": chart_mode_for(view),
        "latest_window_start": None if latest is None else str(latest),
    }
```

Do **not** mount static files yet (Task 2). Ensure datetime/NaN JSON-safe.

- [ ] **Step 6: Run API tests**

```bash
pytest -q tests/test_api.py tests/test_data_access.py
```

Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add urbanflow-streaming/pyproject.toml urbanflow-streaming/analytics_app/settings.py urbanflow-streaming/analytics_app/api.py urbanflow-streaming/tests/test_api.py urbanflow-streaming/requirements/analytics.txt
git commit -m "feat(streaming): add FastAPI Gold read API for analytics"
```

---

### Task 2: Static frontend — choropleth + tables + charts

**Files:**
- Create: `urbanflow-streaming/analytics_app/static/index.html`
- Create: `urbanflow-streaming/analytics_app/static/styles.css`
- Create: `urbanflow-streaming/analytics_app/static/app.js`
- Modify: `urbanflow-streaming/analytics_app/api.py` (mount `StaticFiles`, root `/`)

**Interfaces:**
- Consumes: `GET /api/live`, `GET /api/map`, `GET /api/meta`
- Produces: browser UI at `http://localhost:8501/` with Overview (map+table), Comparison bars, Unusual list

- [ ] **Step 1: Mount static files in `api.py`**

After routes are defined:

```python
STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
```

Mount **after** `/api/*` routes so API is not shadowed. Import `Path`.

- [ ] **Step 2: Write `index.html`**

Single page with:
- Header: `UrbanFlow — Live mobility analytics`
- Meta strip: latest window (NY), high-demand threshold
- Tabs or sections: Overview | Comparison | Unusual
- `#map` div for MapLibre
- `#chart` canvas for Chart.js
- `#table` for zone rows
- Script tags: MapLibre CSS/JS CDN, Chart.js CDN, `/app.js`

- [ ] **Step 3: Write `styles.css`**

Full-viewport layout: header + map (~520px) + table. Legend under map: grey = no live data, teal = ratio &lt; 1, amber ≥ 1, red &gt; 1.5. No Streamlit chrome.

- [ ] **Step 4: Write `app.js`**

Required behavior:
1. `fetch('/api/meta')`, `fetch('/api/live')`, `fetch('/api/map')` on load.
2. MapLibre map centered on NYC (`[-73.97, 40.75]`, zoom ~11).
3. `map.addSource('zones', { type: 'geojson', data })` then fill layer using `fill_r/g/b/a` via MapLibre data-driven expressions **or** precomputed `fill_color` as rgba string property added server-side.
4. Prefer adding server field `fill_rgba` in `attach_demand_to_geojson` if MapLibre expression is awkward — extend `zones.py` + one unit test if needed:
   `fill_rgba = f"rgba({r},{g},{b},{a/255})"`
5. Bar chart of `current_trip_count` by `zone_label` colored by demand.
6. Table columns match former Streamlit: zone_label, window_label, current_trip_count, historical_mean_trip_count, demand_ratio, z_score, is_high_demand.
7. Unusual section: filter `demand_ratio > meta.high_demand_ratio`.

- [ ] **Step 5: Manual smoke**

```bash
cd urbanflow-streaming
uvicorn analytics_app.api:app --reload --host 127.0.0.1 --port 8501
```

Open http://127.0.0.1:8501 — expect NYC zone polygons, Midtown/UES painted when using fixture Gold, table non-empty.

- [ ] **Step 6: Commit**

```bash
git add urbanflow-streaming/analytics_app/static urbanflow-streaming/analytics_app/api.py urbanflow-streaming/analytics_app/zones.py urbanflow-streaming/tests
git commit -m "feat(streaming): MapLibre analytics UI over FastAPI Gold API"
```

---

### Task 3: SSE live refresh (replace Streamlit rerun)

**Files:**
- Modify: `urbanflow-streaming/analytics_app/api.py`
- Modify: `urbanflow-streaming/analytics_app/static/app.js`
- Modify: `urbanflow-streaming/tests/test_api.py`

**Interfaces:**
- Produces: `GET /api/stream` text/event-stream; events `{"type":"gold_update","latest_window_start":"..."}` when live Gold mtime or row fingerprint changes; heartbeat comment every 15s.

- [ ] **Step 1: Add failing SSE test**

```python
def test_stream_sends_event(client):
    with client.stream("GET", "/api/stream") as response:
        assert response.status_code == 200
        assert "text/event-stream" in response.headers["content-type"]
        # Read until first data line or timeout via iteration limit
        found = False
        for i, line in enumerate(response.iter_lines()):
            if line.startswith("data:"):
                found = True
                break
            if i > 50:
                break
        assert found
```

- [ ] **Step 2: Implement SSE in `api.py`**

Use `StreamingResponse` + async generator:
- Fingerprint = `(Path(live_path).stat().st_mtime_ns, latest window_start string)`
- On change or first connect, yield `data: {...}\n\n`
- Sleep `poll_seconds` between checks
- Client disconnect ends generator

- [ ] **Step 3: Wire `EventSource('/api/stream')` in `app.js`**

On `gold_update`, re-fetch live + map + meta and redraw. Do not full-page reload.

- [ ] **Step 4: Run tests**

```bash
pytest -q tests/test_api.py tests/test_data_access.py
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add urbanflow-streaming/analytics_app/api.py urbanflow-streaming/analytics_app/static/app.js urbanflow-streaming/tests/test_api.py
git commit -m "feat(streaming): SSE refresh for live Gold analytics UI"
```

---

### Task 4: Remove Streamlit entry + docs cutover

**Files:**
- Delete or replace: `urbanflow-streaming/analytics_app/app.py`
- Delete: `urbanflow-streaming/.streamlit/config.toml` (if unused)
- Modify: `urbanflow-streaming/README.md`
- Modify: plan/README references to `streamlit run`

- [ ] **Step 1: Replace `app.py` with a short module docstring pointing to API**

```python
"""Deprecated Streamlit entry.

Run the FastAPI analytics UI instead:

    uvicorn analytics_app.api:app --reload --host 127.0.0.1 --port 8501
"""
```

Or delete `app.py` if nothing imports it — prefer delete only after grep shows no imports.

- [ ] **Step 2: Update README analytics section**

Replace Streamlit commands with:

```bash
python -m pip install -r requirements/analytics.txt
uvicorn analytics_app.api:app --reload --host 127.0.0.1 --port 8501
```

Note: UI reads Gold only; Spark/Kafka unchanged. Env vars: `URBANFLOW_LIVE_PATH`, `URBANFLOW_HISTORICAL_PATH`, `URBANFLOW_LOOKUP_PATH`, `URBANFLOW_POLL_SECONDS`.

- [ ] **Step 3: Grep for leftover streamlit references in `urbanflow-streaming/`**

```bash
rg -n "streamlit|pydeck|altair" urbanflow-streaming
```

Expected: only historical docs mentions or none in code/deps.

- [ ] **Step 4: Final verification**

```bash
pytest -q tests/test_api.py tests/test_data_access.py
uvicorn analytics_app.api:app --host 127.0.0.1 --port 8501
```

Browser: map polygons + fixture red zones + SSE connected (DevTools EventStream).

- [ ] **Step 5: Commit + push branch (when user asks push)**

```bash
git add -A urbanflow-streaming
git commit -m "docs(streaming): cut over analytics from Streamlit to FastAPI UI"
git push origin urbanflow-streaming
```

---

## Out of scope (this plan)

- Spark MLlib / Foundation Gold forecast contracts (separate plan).
- React/Vite SPA build.
- Kafka websocket directly from browser (analytics stays Gold-only).
- Changing Live Gold schema or SeaweedFS paths.

## Self-review

1. **Spec coverage:** Thin Gold serving, choropleth, tables/charts, live refresh, no Foundation break, docs/run commands — covered in Tasks 1–4.
2. **Placeholders:** None intentional; CDN URLs to be pinned to current MapLibre/Chart.js stable majors at implement time.
3. **Types:** `AnalyticsSettings`, `/api/live` `{rows: [...]}`, `/api/map` FeatureCollection, SSE `gold_update` — consistent across tasks.
