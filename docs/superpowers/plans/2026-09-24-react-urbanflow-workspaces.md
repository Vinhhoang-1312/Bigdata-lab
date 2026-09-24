# React UrbanFlow Workspaces Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the primary Streamlit dashboard with two decision-oriented web workspaces without overstating historical replay or ML quality.

**Architecture:** A Vite/React client reads two independent FastAPI services. The Streaming service reads Gold and GeoJSON; the ML service reads only model artifacts. Neither service imports another application's implementation. The old Streamlit app remains a diagnostic.

**Tech Stack:** React, TypeScript, Ant Design, Apache ECharts, MapLibre GL JS, FastAPI, PyTorch, Playwright.

**Spec:** `docs/superpowers/specs/2026-09-20-two-viewpoint-urbanflow-ui-design.md`.

## Global Constraints

- Preserve Foundation 1.0.0 fields, paths, time semantics and ownership.
- Label the February 2023 Gold output as historical replay, not real-time 2026 traffic.
- Make every reload deliberate; no timer-based refetch or full-page rerender.
- Do not present the GTQN model as a chatbot, image model or proven production forecast.
- Do not call WAPE `accuracy`; do not present the raw test report as a final benchmark.
- Keep generated frontend dependencies and artifacts out of Git and Docker's C-drive storage.

## Review Focus

- Missing or null baseline: show `No baseline`, never infinity or fabricated ratio.
- Gold unavailable: retain the timestamp of any cached snapshot and explain the source error.
- ML artifact mismatch: reject inference with 503, never use a different checkpoint silently.
- February DST/timezone input: require an explicit New York bucket or offset.
- Narrow browser widths: no clipped labels, horizontal page overflow or invisible controls.

---

### Task 1: Streaming Read Model

**Files:** `urbanflow-streaming/serving/streaming_service.py`, `urbanflow-streaming/serving/app.py`, `urbanflow-streaming/tests/test_serving.py`.

**Interfaces:** `GET /v1/streaming/overview`, `GET /v1/streaming/health`, `GET /v1/streaming/zones`.

- [ ] Add failing fixture tests for replay source metadata, null-baseline JSON, map geometry and empty Gold.
- [ ] Run targeted tests and confirm the failure is the expected missing behavior.
- [ ] Return a bounded latest-window model, GeoJSON, and honest replay/staleness status; leave telemetry `unavailable` until wired.
- [ ] Re-run targeted tests and Foundation fixture validator.

### Task 2: ML Evidence And Forecast API

**Files:** `urbanflow-ml/ml_serving/app.py`, `urbanflow-ml/ml_serving/model_store.py`, `urbanflow-ml/tests/test_serving.py`.

**Interfaces:** `GET /v1/forecast/evidence`, `GET /v1/forecast/options`, `POST /v1/forecast` with `{zone_id, forecast_at}`.

- [ ] Test a deterministic tiny artifact/checkpoint, invalid zone/time, hash mismatch and ordered quantiles first.
- [ ] Run targeted tests and confirm they fail for missing API behavior.
- [ ] Load the immutable artifact and best checkpoint once per service process; validate fingerprint and config hash.
- [ ] Produce the next 30-minute P10/P50/P90 only for supported tensor buckets; do not use the target count as an input.
- [ ] Expose the raw February test report with `preliminary` benchmark status and missing-comparison reasons.
- [ ] Re-run tests, then exercise against the real SeaweedFS artifact.

### Task 3: React Workspaces

**Files:** `urbanflow-ui/package.json`, `urbanflow-ui/src/*`, `urbanflow-ui/index.html`, `urbanflow-ui/vite.config.ts`.

**Interfaces:** `/streaming` (Dispatch, Reliability) and `/ml` (Forecast, Evidence); consumes only `/v1` APIs.

- [ ] Add Vitest tests for formatting, null values, stale replay, viewpoint switching and forecast error states.
- [ ] Run tests and confirm expected failures.
- [ ] Build restrained Ant Design layout, readable responsive typography and explicit workspace/viewpoint controls.
- [ ] Render the latest Gold map with MapLibre and a small, meaningful zone comparison with ECharts; no giant multi-zone spaghetti chart.
- [ ] Build structured ML forecast controls and evidence panel; never offer unsupported image upload.
- [ ] Fetch on initial entry or Refresh only, cache last successful response with its original timestamp, and show failures.
- [ ] Run tests, TypeScript check and production build.

### Task 4: Runtime And Browser Audit

**Files:** `compose.yaml`, `README.md`, `urbanflow-streaming/README.md`, `urbanflow-ml/README.md`, Playwright tests.

- [ ] Add two read-only API services using the existing Spark ML image and D-drive-backed Docker environment; expose ports only on 127.0.0.1.
- [ ] Document one command to start all dependencies and the frontend, plus distinct Streaming/ML URLs.
- [ ] Run `docker compose config --quiet`, API tests, TypeScript tests/build, and the existing fixture validator.
- [ ] Run Playwright at desktop and mobile sizes; inspect screenshots for clipping, loading shifts, map tiles, tab/viewpoint state and errors.
- [ ] Report actual results and any unverified production/model limitations.
