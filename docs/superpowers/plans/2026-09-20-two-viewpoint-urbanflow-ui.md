# Two-Viewpoint UrbanFlow UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Deliver two audited, role-specific interfaces: Streaming Dispatcher/Reliability and ML Planner/Evidence.

**Architecture:** Keep the two application subprojects independent. Each has a thin FastAPI boundary, a Streamlit UI, typed service functions, and fixture-driven tests. Streaming reads only Gold and optional runtime telemetry; ML loads one verified GTQN artifact/checkpoint pair and exposes forecast/evidence read models.

**Tech Stack:** Python 3.11+, FastAPI, Pydantic, Streamlit, pandas, PyArrow, Pydeck/deck.gl, Altair, PyTorch, pytest, Playwright.

**Spec:** docs/superpowers/specs/2026-09-20-two-viewpoint-urbanflow-ui-design.md

## Global Constraints

- Foundation 1.0.0 contracts, paths, Kafka topic, and America/New_York semantics are frozen.
- Each application owns serving code; neither imports source from Batch, Replay, or the other application.
- APIs use /v1, validate typed input, and reject naive timestamps.
- The ML UI must not claim final quality until a PASS held-out evaluation artifact exists.
- Do not expose image upload in V1 because GTQN was not trained for image inputs.
- Use no paid SaaS, secret, map token, hosted LLM, or generated output in Git.
- Add focused tests before implementation, then run browser audit at desktop and mobile widths.

## Review Focus

- A stale Gold path must show a degraded timestamp, not look like live data; Task 2 tests it.
- Missing Kafka/Spark telemetry must be visibly unavailable, not displayed as zero; Task 2 tests it.
- Switching viewpoint must change the primary task and controls, not merely a heading; Tasks 3 and 7 test it.
- A planner request with an ambiguous zone or a naive timestamp must not call GTQN; Tasks 5 and 6 test it.
- A checkpoint whose config hash or dataset fingerprint differs from the artifact must never serve a forecast; Task 5 tests it.

---

## File Structure

| Path | Responsibility |
| --- | --- |
| urbanflow-streaming/serving/streaming_service.py | Read-only Gold summaries and optional telemetry normalization. |
| urbanflow-streaming/serving/app.py | Typed Streaming API routes. |
| urbanflow-streaming/analytics_app/dispatcher.py | Dispatcher visual components. |
| urbanflow-streaming/analytics_app/reliability.py | Reliability visual components. |
| urbanflow-ml/serving/forecast_service.py | Verified GTQN artifact/checkpoint loading and one-window inference. |
| urbanflow-ml/serving/app.py | Typed ML API routes. |
| urbanflow-ml/forecast_app/app.py | Planner/Evidence UI shell. |
| urbanflow-ml/forecast_app/planner.py | Deterministic parsing and forecast presentation. |
| urbanflow-ml/forecast_app/evidence.py | Test-artifact evidence presentation. |
| tests/e2e/test_two_viewpoint_ui.py | Playwright checks and screenshots. |

### Task 1: Publish PASS-Only ML Evidence

**Files:**
- Modify: urbanflow-ml/spark/evaluate_gtqn_test.py
- Create: urbanflow-ml/tests/test_evaluate_gtqn_test.py
- Modify: urbanflow-ml/README.md

**Interfaces:**
- Consumes: best.pt, tensor artifact, chronological test split.
- Produces: test_metrics.json with run identity, artifact identity, metrics, and UTC evaluation time.

- [ ] **Step 1: Write the failing evidence test**

~~~python
def test_build_evidence_rejects_non_pass_metrics():
    with pytest.raises(ValueError, match="PASS"):
        build_evidence({"status": "FAIL"}, run_id="r1", variant="full")
~~~

- [ ] **Step 2: Run the focused test**

Run: python -m pytest urbanflow-ml/tests/test_evaluate_gtqn_test.py -q
Expected: FAIL because build_evidence does not exist.

- [ ] **Step 3: Implement the minimal evidence builder**

~~~python
def build_evidence(metrics: dict, *, run_id: str, variant: str) -> dict:
    if metrics.get("status") != "PASS":
        raise ValueError("evaluation metrics must have PASS status")
    return {"status": "PASS", "run_id": run_id, "variant": variant,
            "metrics": metrics, "evaluated_at": datetime.now(timezone.utc).isoformat()}
~~~

- [ ] **Step 4: Verify and commit**

Run: python -m pytest urbanflow-ml/tests/test_evaluate_gtqn_test.py -q
Expected: PASS.

~~~bash
git add urbanflow-ml/spark/evaluate_gtqn_test.py urbanflow-ml/tests/test_evaluate_gtqn_test.py urbanflow-ml/README.md
git commit -m "feat(ml): publish verified forecast evidence"
~~~

### Task 2: Add Streaming Read Models and API

**Files:**
- Create: urbanflow-streaming/serving/__init__.py
- Create: urbanflow-streaming/serving/streaming_service.py
- Create: urbanflow-streaming/serving/app.py
- Create: urbanflow-streaming/tests/test_serving.py
- Modify: urbanflow-streaming/pyproject.toml

**Interfaces:**
- Consumes: GoldReader, prepare_live_view, optional telemetry dictionary.
- Produces: build_streaming_overview(reader, lookup), build_streaming_health(reader, telemetry, now), and two HTTP endpoints.

- [ ] **Step 1: Write failing service and route tests**

~~~python
def test_health_marks_missing_telemetry_unavailable(fixture_reader):
    health = build_streaming_health(fixture_reader, telemetry=None, now=NOW)
    assert health["telemetry_status"] == "unavailable"
    assert health["invalid_record_count"] is None

def test_health_route_returns_degraded_freshness(client):
    response = client.get("/v1/streaming/health")
    assert response.status_code == 200
    assert response.json()["live_gold_age_seconds"] >= 0
~~~

- [ ] **Step 2: Run tests to verify failure**

Run: python -m pytest urbanflow-streaming/tests/test_serving.py -q
Expected: FAIL because service and routes do not exist.

- [ ] **Step 3: Implement read-only typed summaries**

~~~python
def build_streaming_overview(reader: GoldReader, lookup: pd.DataFrame) -> dict: ...
def build_streaming_health(reader: GoldReader, telemetry: dict | None, now: datetime) -> dict: ...

@app.get("/v1/streaming/overview")
def overview() -> StreamingOverview: ...

@app.get("/v1/streaming/health")
def health() -> StreamingHealth: ...
~~~

Compute freshness from explicit UTC Live Gold time. Return unavailable, never
zero, for absent telemetry. Use environment-provided Gold paths; no ETL.

- [ ] **Step 4: Verify and commit**

Run: python -m pytest urbanflow-streaming/tests/test_serving.py urbanflow-streaming/tests/test_data_access.py -q
Expected: PASS.

~~~bash
git add urbanflow-streaming/serving urbanflow-streaming/tests/test_serving.py urbanflow-streaming/pyproject.toml
git commit -m "feat(streaming): expose read-only live status API"
~~~

### Task 3: Refactor Streaming UI Into Dispatcher and Reliability

**Files:**
- Create: urbanflow-streaming/analytics_app/dispatcher.py
- Create: urbanflow-streaming/analytics_app/reliability.py
- Modify: urbanflow-streaming/analytics_app/app.py
- Create: urbanflow-streaming/tests/test_ui_views.py
- Modify: urbanflow-streaming/README.md

**Interfaces:**
- Consumes: Streaming API response objects and existing map/chart helpers.
- Produces: render_dispatcher(overview), render_reliability(health), and VIEWPOINTS.

- [ ] **Step 1: Write the failing viewpoint test**

~~~python
def test_viewpoint_definitions_have_distinct_primary_questions():
    assert VIEWPOINTS["Dispatcher"]["primary_question"] != VIEWPOINTS["Reliability"]["primary_question"]
~~~

- [ ] **Step 2: Run it**

Run: python -m pytest urbanflow-streaming/tests/test_ui_views.py -q
Expected: FAIL because VIEWPOINTS does not exist.

- [ ] **Step 3: Implement two role-specific renders**

~~~python
VIEWPOINTS = {
    "Dispatcher": {"primary_question": "Where is live pickup demand unusual right now?"},
    "Reliability": {"primary_question": "Can I trust the live view right now?"},
}
~~~

Use st.segmented_control. Dispatcher owns map, zone explorer, current-vs-baseline
chart, and top zones. Reliability owns freshness, output row count, telemetry
availability, checkpoint state, and empty/degraded states.

- [ ] **Step 4: Verify and commit**

Run: python -m pytest urbanflow-streaming/tests/test_ui_views.py urbanflow-streaming/tests/test_data_access.py -q
Expected: PASS.

~~~bash
git add urbanflow-streaming/analytics_app urbanflow-streaming/tests/test_ui_views.py urbanflow-streaming/README.md
git commit -m "feat(streaming): add dispatcher and reliability viewpoints"
~~~

### Task 4: Build Verified GTQN Forecast Service

**Files:**
- Create: urbanflow-ml/serving/__init__.py
- Create: urbanflow-ml/serving/forecast_service.py
- Create: urbanflow-ml/tests/test_forecast_serving.py
- Modify: urbanflow-ml/pyproject.toml

**Interfaces:**
- Consumes: GTQN best.pt, tensor artifact, scaler, config hash, and dataset fingerprint.
- Produces: load_verified_forecaster(...) and VerifiedForecaster.forecast(zone_id, forecast_at).

- [ ] **Step 1: Write failing safety tests**

~~~python
def test_load_verified_forecaster_rejects_fingerprint_mismatch(tmp_path):
    with pytest.raises(ValueError, match="fingerprint"):
        load_verified_forecaster(artifact_path=ARTIFACT, checkpoint_path=BAD_CHECKPOINT)

def test_forecast_rejects_naive_timestamp(forecaster):
    with pytest.raises(ValueError, match="timezone"):
        forecaster.forecast(zone_id=132, forecast_at=datetime(2023, 2, 1, 9, 0))
~~~

- [ ] **Step 2: Run the test**

Run: python -m pytest urbanflow-ml/tests/test_forecast_serving.py -q
Expected: FAIL because load_verified_forecaster does not exist.

- [ ] **Step 3: Implement one-window inference**

~~~python
class VerifiedForecaster:
    def forecast(self, *, zone_id: int, forecast_at: datetime) -> dict:
        # Locate the exact artifact timestamp, build 336 historical buckets,
        # run GTQN, inverse scale P10/P50/P90, and validate ordering.
        ...
~~~

Accept only offset-aware timestamps represented by the artifact. Return zone_id,
forecast_at, p10, p50, p90, run_id, config hash, and dataset fingerprint.

- [ ] **Step 4: Verify and commit**

Run: python -m pytest urbanflow-ml/tests/test_forecast_serving.py urbanflow-ml/tests/test_gtqn.py -q
Expected: PASS.

~~~bash
git add urbanflow-ml/serving urbanflow-ml/tests/test_forecast_serving.py urbanflow-ml/pyproject.toml
git commit -m "feat(ml): serve verified GTQN forecasts"
~~~

### Task 5: Expose Forecast API and Evidence Gate

**Files:**
- Create: urbanflow-ml/serving/app.py
- Modify: urbanflow-ml/pyproject.toml
- Modify: urbanflow-ml/tests/test_forecast_serving.py

**Interfaces:**
- Consumes: VerifiedForecaster and PASS-only evaluation evidence.
- Produces: POST /v1/forecast and GET /v1/forecast/evidence.

- [ ] **Step 1: Write failing endpoint tests**

~~~python
def test_forecast_route_rejects_naive_timestamp(client):
    response = client.post("/v1/forecast", json={"zone_id": 132, "forecast_at": "2023-02-01T09:00:00"})
    assert response.status_code == 422

def test_evidence_route_withholds_unfinished_evaluation(client):
    assert client.get("/v1/forecast/evidence").status_code == 503
~~~

- [ ] **Step 2: Run them**

Run: python -m pytest urbanflow-ml/tests/test_forecast_serving.py -q
Expected: FAIL because API routes do not exist.

- [ ] **Step 3: Implement Pydantic routes**

~~~python
class ForecastRequest(BaseModel):
    zone_id: int
    forecast_at: datetime

@app.post("/v1/forecast")
def forecast(request: ForecastRequest) -> ForecastResponse: ...

@app.get("/v1/forecast/evidence")
def evidence() -> ForecastEvidence: ...
~~~

Return 422 for malformed/naive requests, 409 for incompatible artifacts, and
503 when final evidence is absent or non-PASS.

- [ ] **Step 4: Verify and commit**

Run: python -m pytest urbanflow-ml/tests/test_forecast_serving.py -q
Expected: PASS.

~~~bash
git add urbanflow-ml/serving/app.py urbanflow-ml/pyproject.toml urbanflow-ml/tests/test_forecast_serving.py
git commit -m "feat(ml): expose typed forecast API"
~~~

### Task 6: Build Planner and Evidence ML UI

**Files:**
- Create: urbanflow-ml/forecast_app/__init__.py
- Create: urbanflow-ml/forecast_app/planner.py
- Create: urbanflow-ml/forecast_app/evidence.py
- Create: urbanflow-ml/forecast_app/app.py
- Create: urbanflow-ml/tests/test_forecast_ui.py
- Modify: urbanflow-ml/README.md

**Interfaces:**
- Consumes: ML API, zone lookup, and API endpoint environment variable.
- Produces: parse_planner_question(text), render_planner, and render_evidence.

- [ ] **Step 1: Write failing parser tests**

~~~python
def test_parse_question_extracts_zone_and_offset_timestamp():
    parsed = parse_planner_question("Zone 132 at 2023-02-01T09:00:00-05:00")
    assert parsed.zone_id == 132
    assert parsed.forecast_at.isoformat() == "2023-02-01T09:00:00-05:00"

def test_parse_question_rejects_ambiguous_zone():
    assert parse_planner_question("Will it be busy tomorrow?") is None
~~~

- [ ] **Step 2: Run parser tests**

Run: python -m pytest urbanflow-ml/tests/test_forecast_ui.py -q
Expected: FAIL because parse_planner_question does not exist.

- [ ] **Step 3: Implement controls first, parser as convenience**

~~~python
VIEWPOINTS = ("Planner", "Evidence")
~~~

Planner includes map/selectbox, offset-aware date/time controls, a question
field, and explicit submit. Evidence shows verified data or a truthful not-ready
state. There is no image uploader.

- [ ] **Step 4: Verify and commit**

Run: python -m pytest urbanflow-ml/tests/test_forecast_ui.py -q
Expected: PASS.

~~~bash
git add urbanflow-ml/forecast_app urbanflow-ml/tests/test_forecast_ui.py urbanflow-ml/README.md
git commit -m "feat(ml): add planner and evidence viewpoints"
~~~

### Task 7: Compose, Documentation, and Browser Audit

**Files:**
- Modify: compose.yaml
- Modify: .env.example
- Modify: README.md
- Modify: urbanflow-streaming/README.md
- Modify: urbanflow-ml/README.md
- Create: tests/e2e/test_two_viewpoint_ui.py
- Create: docs/results/two-viewpoint-ui-audit-YYYYMMDD.md

**Interfaces:**
- Consumes: both FastAPI apps and both Streamlit apps through configured local ports.
- Produces: reproducible launch commands and screenshot audit evidence.

- [ ] **Step 1: Write failing Playwright behavior checks**

~~~python
def test_streaming_viewpoint_switch_changes_primary_content(page):
    page.goto(STREAMING_URL)
    page.get_by_label("Viewpoint").select_option("Reliability")
    expect(page.get_by_text("Can I trust the live view right now?")).to_be_visible()

def test_ml_planner_rejects_ambiguous_question(page):
    page.goto(ML_URL)
    page.get_by_label("Question").fill("Will it be busy tomorrow?")
    expect(page.get_by_text("Select a zone and time")).to_be_visible()
~~~

- [ ] **Step 2: Run the incomplete browser suite**

Run: python -m pytest tests/e2e/test_two_viewpoint_ui.py -q
Expected: FAIL before services and UI elements are wired.

- [ ] **Step 3: Add Compose services and documented commands**

~~~yaml
streaming-api: { command: uvicorn serving.app:app --host 0.0.0.0 --port 8010 }
forecast-api: { command: uvicorn serving.app:app --host 0.0.0.0 --port 8011 }
~~~

Use distinct working directories, health checks, environment-only endpoints,
and no new storage volumes.

- [ ] **Step 4: Run complete automated audit**

Run: docker compose config --quiet
Expected: exit 0.

Run: python -m pytest urbanflow-streaming/tests/test_serving.py urbanflow-streaming/tests/test_ui_views.py urbanflow-ml/tests/test_forecast_serving.py urbanflow-ml/tests/test_forecast_ui.py -q
Expected: PASS.

Run: python -m pytest tests/e2e/test_two_viewpoint_ui.py -q
Expected: PASS at desktop and mobile widths; save screenshots.

- [ ] **Step 5: Verify preserved streaming behavior and commit**

Run: docker compose run --rm spark-submit --master spark://spark-master:7077 /workspace/urbanflow-streaming/tests/verify_integrated_run.py --help
Expected: exit 0 and existing verifier remains available.

Record viewport sizes, API results, screenshots, limitations, and PASS/FAIL in
the audit report.

~~~bash
git add compose.yaml .env.example README.md urbanflow-streaming urbanflow-ml tests/e2e docs/results
git commit -m "feat: ship audited UrbanFlow user interfaces"
~~~

## Trade-offs and Decision Record

| Decision | Benefit | Cost / mitigation |
| --- | --- | --- |
| Two focused viewpoints per UI | Fast role-specific decisions, low cognitive load | Shared charts are intentionally not duplicated. |
| Separate APIs by subproject | Preserves ownership and independent deploys | Small code duplication; share only frozen Foundation contracts. |
| FastAPI plus Streamlit | Free, Python-native, typed, fast to audit | Less visual freedom than React; appropriate for this operational teaching project. |
| Map controls plus deterministic parser | Valid inputs with convenient questions | No general chatbot claim; controls remain authoritative. |
| No image upload in V1 | Scientifically honest and low risk | Vision requires a later labeled image dataset and a different model. |
| PASS-gated evidence | Prevents misleading quality claims | Evidence view remains not-ready until final evaluation completes. |

## Plan Self-Review

- Spec coverage: Tasks 2-3 implement Streaming; Tasks 1 and 4-6 implement ML; Task 7 audits and composes both.
- Placeholder scan: no unresolved placeholder or deferred implementation language remains.
- Type consistency: Streaming service functions feed streaming routes; VerifiedForecaster.forecast feeds ForecastRequest; ML UI calls only typed API endpoints.
- Review Focus coverage: stale Gold/telemetry are Task 2; viewpoints are Tasks 3/7; planner input and artifact safety are Tasks 4-6.
