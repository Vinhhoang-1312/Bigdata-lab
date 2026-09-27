# ML Workspace And Training Information Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship three clear React ML workspaces: demand forecasting, interactive Spark MLlib K-means trip scoring, and an artifact-backed training information view that explains preprocessing, features, distributed training, evidence, and limitations.

**Architecture:** Keep Spark and SeaweedFS as offline training/storage systems and keep FastAPI as a thin artifact-serving boundary. Score one trip in memory from the immutable completed K-means artifact; expose a separate information contract that combines real GTQN/K-means evidence with stable explanatory metadata; render the three concerns as independent React routes.

**Tech Stack:** Python 3, FastAPI, Pydantic, NumPy/math, Spark MLlib artifact semantics, PyTorch DDP evidence, SeaweedFS S3, React 19, TypeScript, Ant Design, Lucide, MapLibre GL, Vitest, Playwright.

**Spec:** `docs/superpowers/specs/2026-09-27-ml-workspace-and-training-information.md`

## Global Constraints

- Execute in worktree `C:\Users\DELL\Documents\Big Data\.tmp\team-merge-20260926` on branch `integration/team-merge-20260926`; do not implement this plan in the root `urbanflow-streaming` worktree.
- The implementation baseline is commit `24a9638`; use it for final diff review even if the number of task commits changes.
- This integration worktree has no `.env`. For manual Compose commands use `docker-compose --env-file "C:\Users\DELL\Documents\Big Data\.env" ...`; never copy, print or commit the credential values.
- Read the spec and `AGENTS.md` before Task 1. Preserve Foundation 1.0.0 contracts and frozen paths/time semantics.
- Do not retrain either model. Reuse the completed K-means and GTQN artifacts.
- Do not use or restore Streamlit. The customer UI is React at port 5173.
- Do not call Spark from an HTTP request. One-trip scoring must be pure in-memory inference.
- Do not add a router library. Preserve the existing History API routing pattern in `App.tsx`.
- Do not add chart/UI dependencies. Use Ant Design, Lucide, MapLibre, ECharts and CSS already installed.
- Do not commit `.env`, credentials, data, checkpoints, `node_modules`, build output, Playwright output or screenshots.
- Do not claim live node health from historical training evidence.
- Do not call K-means output fraud, probability, classification, accuracy, precision or recall.
- Keep UI copy in clear B1-B2 English. Documentation may explain the decisions in Vietnamese.
- Use TDD for each behavior change. Do not weaken existing tests or expected evidence.
- Commit after every task with the listed commit message. Do not push until all verification gates pass.

## Review Focus

1. A complete artifact with a zero scaler standard deviation must not divide by zero; Task 1 pins rejection of invalid scaler metadata.
2. NaN, infinity, zero duration/distance and negative fare must produce HTTP 422, not a 500 or a misleading score; Tasks 1-2 pin these cases.
3. Missing K-means evidence must degrade only that section of Training Information; Task 3 pins partial availability.
4. Historical two-worker evidence must not be labelled as current Spark health; Tasks 3 and 7 pin exact wording.
5. Direct URL reload, browser Back, narrow mobile layout and failed refresh must not blank or overflow the UI; Tasks 4, 8 and 9 pin these behaviors.

---

## Current Baseline: Do Not Rebuild

The executor must first recognize what already works:

- `urbanflow-ml/spark/train_trip_anomalies.py` already trains `k=8`, stores
  scaler mean/std, cluster centers, threshold, features, sample counts, worker
  hosts and top trips.
- `urbanflow-ml/forecasting/trip_anomaly.py` already contains Euclidean score
  and largest-deviation explanation primitives.
- `urbanflow-ml/ml_serving/app.py` already exposes forecast options, evidence,
  forecast inference and the anomaly review snapshot.
- `urbanflow-ui/src/TripAnomalies.tsx` and `TripAnomalyMap.tsx` already provide
  the review queue and zone-center map.
- `urbanflow-ui/src/MachineLearning.tsx` already provides forecast and evidence.
- `urbanflow-ui/qa/workspaces.spec.ts` already protects forecast, anomaly queue,
  route persistence, cache behavior and visible API failures.

Do not replace those parts wholesale. Extend and split only where this plan says.

## Target File Map

### Backend

- Modify `urbanflow-ml/forecasting/trip_anomaly.py`: pure feature transform,
  validation, nearest-center scoring and explanation interface.
- Modify `urbanflow-ml/ml_serving/trip_anomalies.py`: validate full inference
  metadata and add cached artifact-backed scorer.
- Create `urbanflow-ml/ml_serving/training_information.py`: build the stable
  information response without UI-specific markup.
- Modify `urbanflow-ml/ml_serving/model_store.py`: expose actual GTQN config,
  tensor metadata and evidence needed by information builder.
- Modify `urbanflow-ml/ml_serving/app.py`: request schema plus score and
  information endpoints.
- Modify backend tests under `urbanflow-ml/tests/`.

### Frontend

- Modify `urbanflow-ui/src/App.tsx`: canonical three-view routing and legacy URL compatibility.
- Modify `urbanflow-ui/src/MachineLearning.tsx`: forecast-only workspace shell.
- Modify `urbanflow-ui/src/TripAnomalies.tsx`: K-means workspace with queue/check modes.
- Create `urbanflow-ui/src/TripScoreForm.tsx`: input form and score result.
- Create `urbanflow-ui/src/TrainingInformation.tsx`: information workspace.
- Create `urbanflow-ui/src/TrainingPipeline.tsx`: accessible responsive pipeline diagram.
- Modify `urbanflow-ui/src/api.ts` and `urbanflow-ui/src/types.ts`: typed API contracts.
- Modify `urbanflow-ui/src/styles.css`: scoped responsive styles only.
- Modify `urbanflow-ui/qa/workspaces.spec.ts` and optionally create focused
  Vitest tests for pure frontend helpers.

### Documentation

- Modify `README.md` and `urbanflow-ml/README.md` after behavior is verified.

---

### Task 1: Pure K-means Inference Mathematics

**Files:**
- Modify: `urbanflow-ml/forecasting/trip_anomaly.py`
- Modify: `urbanflow-ml/tests/test_trip_anomaly_model.py`

**Interfaces:**
- Consumes: raw distance miles, duration minutes, fare USD, artifact scaler mean/std and cluster centers.
- Produces: `raw_trip_features(distance_miles: float, duration_minutes: float, fare_usd: float) -> list[float]`, `standardize_features(raw: Sequence[float], mean: Sequence[float], std: Sequence[float]) -> list[float]`, and `score_nearest_cluster(values: Sequence[float], centers: Sequence[Sequence[float]]) -> tuple[int, float, str]`.

- [x] **Step 1: Write failing tests for raw feature parity**

Add `test_raw_trip_features_match_spark_training_formula` asserting the four
values are `log1p(distance)`, `log1p(duration)`, `log1p(fare)`, and
`log1p(distance * 60 / duration)` in the same order as the training artifact.

- [x] **Step 2: Write failing validation tests**

Add table-driven tests asserting `ValueError` for distance `<= 0`, duration
`<= 0`, fare `< 0`, NaN and infinity. Add tests rejecting non-four-dimensional
mean/std/center arrays, empty centers, and scaler std `<= 0`.

- [x] **Step 3: Run focused tests and confirm RED**

Run:

```powershell
docker-compose --env-file "C:\Users\DELL\Documents\Big Data\.env" run --rm --no-deps --entrypoint /usr/bin/python3 ml-api -m unittest discover -s tests -p "test_trip_anomaly_model.py" -v
```

Expected: FAIL because the three new functions do not exist.

- [x] **Step 4: Implement the three pure functions**

Use `math.log1p`, `math.isfinite`, z-score `(raw - mean) / std`, existing
`score_distance`, and existing `explain_deviation`. Choose the minimum-distance
center with deterministic first-index tie behavior. Do not import Spark,
scikit-learn or pandas.

- [x] **Step 5: Run focused tests and confirm GREEN**

Expected: all tests in `test_trip_anomaly_model.py` PASS.

- [x] **Step 6: Commit**

```powershell
git add urbanflow-ml/forecasting/trip_anomaly.py urbanflow-ml/tests/test_trip_anomaly_model.py
git commit -m "feat(ml): add artifact-compatible trip scoring"
```

---

### Task 2: Artifact-backed One-trip Scoring API

**Files:**
- Modify: `urbanflow-ml/ml_serving/trip_anomalies.py`
- Modify: `urbanflow-ml/ml_serving/app.py`
- Modify: `urbanflow-ml/tests/test_trip_anomalies.py`
- Modify: `urbanflow-ml/tests/test_serving.py`
- Modify: `urbanflow-ml/forecasting/scaling.py`
- Modify: `urbanflow-ml/forecasting/probabilistic_evaluation.py`
- Modify: `urbanflow-ml/tests/test_scaling.py`
- Modify: `urbanflow-ml/tests/test_probabilistic_evaluation.py`
- Modify: `compose.yaml`
- Modify: `docker/spark-ml/Dockerfile`

**Interfaces:**
- Consumes: pure functions from Task 1 and completed anomaly artifact fields
  `scaler_mean`, `scaler_std`, `cluster_centers`, `threshold`, `features`,
  `run_id`, `method`.
- Produces: `score_trip(snapshot: dict, *, distance_miles: float, duration_minutes: float, fare_usd: float) -> dict`, cached `get_anomaly_snapshot() -> dict`, and `POST /v1/anomaly/score` matching the spec.

- [x] **Step 1: Strengthen snapshot validation tests**

Extend `test_trip_anomalies.py` with a complete four-feature fixture. Assert
that inference metadata is accepted and missing/malformed scaler, center or
threshold fields are rejected before scoring. Preserve the existing list API
fixture test by testing list-scope validation separately if necessary.

- [x] **Step 2: Write score contract tests**

Use a small deterministic artifact with two centers. Assert exact nearest
cluster, Euclidean score, `score >= threshold` review decision, derived speed,
reason, run ID, method, feature list and `research_only` status. Add a below-
threshold case and an exact-threshold case; exact threshold must require review.

- [x] **Step 3: Write FastAPI boundary tests**

Test the endpoint function or FastAPI TestClient following the existing suite
style. Assert valid request `200`, invalid numeric request `422`, unavailable
artifact `503`, and that exception details do not expose credentials or S3
internals.

- [x] **Step 4: Run focused tests and confirm RED**

```powershell
docker-compose --env-file "C:\Users\DELL\Documents\Big Data\.env" run --rm --no-deps --entrypoint /usr/bin/python3 ml-api -m unittest discover -s tests -p "test_trip_anomalies.py" -v
docker-compose --env-file "C:\Users\DELL\Documents\Big Data\.env" run --rm --no-deps --entrypoint /usr/bin/python3 ml-api -m unittest discover -s tests -p "test_serving.py" -v
```

- [x] **Step 5: Implement cached snapshot and scoring**

Add `@lru_cache(maxsize=1)` around the environment-selected completed snapshot.
Both GET and POST must reuse it. The POST request Pydantic model has positive
zone IDs, positive distance/duration and non-negative fare; also rely on Task 1
for finite-number defense. Do not accept speed from the client.

- [x] **Step 6: Run focused and full backend unit tests**

```powershell
docker-compose --env-file "C:\Users\DELL\Documents\Big Data\.env" run --rm --no-deps --entrypoint /usr/bin/python3 ml-api -m unittest discover -s tests -v
```

Expected: PASS; no Spark training job starts.

- [x] **Step 7: Commit**

```powershell
git add urbanflow-ml/ml_serving urbanflow-ml/tests/test_trip_anomalies.py urbanflow-ml/tests/test_serving.py
git commit -m "feat(api): score one trip with completed kmeans artifact"
```

---

### Task 3: Stable Training Information API Contract

**Files:**
- Create: `urbanflow-ml/ml_serving/training_information.py`
- Modify: `urbanflow-ml/ml_serving/model_store.py`
- Modify: `urbanflow-ml/ml_serving/app.py`
- Create: `urbanflow-ml/tests/test_training_information.py`
- Modify: `urbanflow-ml/tests/test_serving.py`

**Interfaces:**
- Consumes: GTQN config, tensor metadata, training result/evidence, test metrics,
  weekly baseline and optional anomaly snapshot.
- Produces: `build_training_information(*, config: dict, metadata: dict, evidence: dict, anomaly: dict | None, anomaly_error: str | None = None) -> dict`, `ModelStore.information(anomaly, anomaly_error=None) -> dict`, and `GET /v1/ml/information`.

- [x] **Step 1: Write the complete response test**

Assert stable top-level keys from the spec. Pin actual semantics: demand model
name `GTQN-Lite`, model family `graph_temporal_quantile_regression`, lookback
336 buckets, chronological split values from config/metadata, distributed
evidence including per-rank host/batches/checksum-match boolean, preprocessing
policies, architecture techniques, anomaly `k=8`, threshold/periods/features,
and both models marked `research_only`.

- [x] **Step 2: Write partial-evidence and honesty tests**

Assert `anomaly=None` yields `kmeans_anomaly.availability="unavailable"` while
demand data remains available. Assert the output calls hosts `training_hosts`
or `training-time evidence`, never `active nodes`, `healthy nodes` or current
cluster health. Assert checksum values are reduced to a safe prefix in the UI
contract while match status is preserved.

- [x] **Step 3: Write missing-data policy tests**

Pin all policies in Spec 5.3. In particular assert there is no generic
`mean_imputation` or `mode_imputation`; demand gaps include `observed_mask`,
seasonal lags include missing indicators, and K-means invalid fields are
rejected rather than filled.

- [x] **Step 4: Run focused tests and confirm RED**

```powershell
docker-compose --env-file "C:\Users\DELL\Documents\Big Data\.env" run --rm --no-deps --entrypoint /usr/bin/python3 ml-api -m unittest discover -s tests -p "test_training_information.py" -v
```

- [x] **Step 5: Implement the builder and ModelStore method**

Keep the builder deterministic and free of S3/network calls. Read values from
config, metadata, evidence and artifact wherever they exist. Stable technique
descriptions may be constants in this module, but measured values must never be
hard-coded. Return explicit `unavailable` values instead of invented zeros.

- [x] **Step 6: Add the endpoint with bounded degradation**

Load K-means snapshot through Task 2. Catch its loading failure, pass a generic
error code/string to the information builder, and still return demand evidence.
If the GTQN ModelStore itself is unavailable, preserve current HTTP 503 behavior.

- [x] **Step 7: Run all backend tests**

```powershell
docker-compose --env-file "C:\Users\DELL\Documents\Big Data\.env" run --rm --no-deps --entrypoint /usr/bin/python3 ml-api -m unittest discover -s tests -v
```

- [x] **Step 8: Commit**

```powershell
git add urbanflow-ml/ml_serving urbanflow-ml/tests
git commit -m "feat(api): expose ML training information"
```

---

### Task 4: Three Canonical ML Routes

**Files:**
- Modify: `urbanflow-ui/src/App.tsx`
- Modify: `urbanflow-ui/src/MachineLearning.tsx`
- Modify: `urbanflow-ui/qa/workspaces.spec.ts`

**Interfaces:**
- Consumes: existing History API navigation.
- Produces: `ModelView = 'forecast' | 'kmeans' | 'information'`, canonical routes
  `/ml`, `/ml/kmeans`, `/ml/information`, and legacy aliases.

- [x] **Step 1: Add failing route tests**

Update Playwright expectations so ML navigation contains three distinct
buttons: `Demand forecast`, `K-means anomaly`, `Training information`. Test
direct canonical reload, browser Back, and direct legacy URLs
`/ml/anomalies`/`/ml/evidence` rendering the matching workspace.

- [x] **Step 2: Run the focused Playwright test and confirm RED**

With services running:

```powershell
cd urbanflow-ui
npx playwright test qa/workspaces.spec.ts --grep "viewpoint URLs" --project=desktop
```

- [x] **Step 3: Implement routing and navigation**

Replace `Explore/Evidence` ambiguity with three direct sidebar items and Lucide
icons. `currentRoute()` accepts legacy aliases. `routePath()` emits only
canonical routes. Remove the forecast/anomaly inner segmented switch from
`MachineLearning.tsx`; each top-level route owns one purpose.

- [x] **Step 4: Run TypeScript build and focused route test**

```powershell
cd urbanflow-ui
npm run build
npx playwright test qa/workspaces.spec.ts --grep "viewpoint URLs" --project=desktop
```

- [x] **Step 5: Commit**

```powershell
git add urbanflow-ui/src/App.tsx urbanflow-ui/src/MachineLearning.tsx urbanflow-ui/qa/workspaces.spec.ts
git commit -m "feat(ui): split ML into three workspaces"
```

---

### Task 5: Typed Frontend API Boundary

**Files:**
- Modify: `urbanflow-ui/src/types.ts`
- Modify: `urbanflow-ui/src/api.ts`
- Create: `urbanflow-ui/src/mlContracts.test.ts`

**Interfaces:**
- Consumes: Task 2 and Task 3 JSON contracts.
- Produces: `TripScoreRequest`, `TripScoreResult`, `TrainingInformation`,
  `postTripScore(request: TripScoreRequest): Promise<TripScoreResult>`, and the
  existing `useResource<TrainingInformation>` pattern for GET information.

- [x] **Step 1: Define exact TypeScript types**

Model optional/unavailable fields explicitly; do not use `any`. Include units
in property names where the backend contract has units. Represent techniques
and preprocessing rows as typed arrays rather than arbitrary records.

- [x] **Step 2: Write failing fetch tests**

Mock `fetch` and assert `postTripScore` sends POST, JSON content type, exact body,
returns typed success, and turns FastAPI `detail` into a readable `Error`.

- [x] **Step 3: Implement the API helper**

Reuse the error parsing convention in existing `postForecast`; extract a small
private helper only if it removes real duplication.

- [x] **Step 4: Run unit tests and build**

```powershell
cd urbanflow-ui
npm test
npm run build
```

- [x] **Step 5: Commit**

```powershell
git add urbanflow-ui/src/types.ts urbanflow-ui/src/api.ts urbanflow-ui/src/mlContracts.test.ts
git commit -m "feat(ui): type ML scoring and information APIs"
```

---

### Task 6: Interactive K-means Check-a-trip Experience

**Files:**
- Modify: `urbanflow-ui/src/TripAnomalies.tsx`
- Create: `urbanflow-ui/src/TripScoreForm.tsx`
- Modify: `urbanflow-ui/src/TripAnomalyMap.tsx` only if it cannot display a
  scored request using the current selected-trip shape.
- Modify: `urbanflow-ui/src/styles.css`
- Modify: `urbanflow-ui/qa/workspaces.spec.ts`

**Interfaces:**
- Consumes: `postTripScore`, `TripScoreResult`, streaming zone geometry and
  existing anomaly map/recenter behavior.
- Produces: K-means page with `Review queue` and `Check a trip` modes.

- [ ] **Step 1: Add failing end-to-end score test**

Navigate to `/ml/kmeans`, switch to `Check a trip`, select pickup 43 and dropoff
161, enter valid distance/duration/fare, submit, and assert score, threshold,
review decision, nearest cluster, reason, units and research warning appear.
Assert the map is ready and the recenter button returns to the selected zones.

- [ ] **Step 2: Add failing invalid-input and API-failure tests**

Assert submit is disabled for blank/invalid values. Mock a 422 and assert inline
error without clearing inputs. Mock a 503 and assert artifact-unavailable copy;
the review queue remains independently usable when its cached GET data exists.

- [ ] **Step 3: Implement the form**

Use Ant Design `Select`, `InputNumber`, `Button`, `Alert`, `Tag` and form labels.
Default to realistic non-extreme example values. Do not ask for speed. State
next to zone selectors that zones provide map context and are not K-means
features. Never render score with `%`.

- [ ] **Step 4: Adapt the map without inventing a route**

Reuse zone geometry and dashed center-to-center line. Preserve CARTO/local
fallback and the existing recenter control. Do not display a GPS path claim.

- [ ] **Step 5: Run UI tests and visual check**

```powershell
cd urbanflow-ui
npm test
npm run build
npx playwright test qa/workspaces.spec.ts --grep "K-means|trip anomaly" --project=desktop
npx playwright test qa/workspaces.spec.ts --grep "K-means|trip anomaly" --project=mobile
```

Inspect screenshots for clipped form labels, hidden units, nested cards,
horizontal overflow and a map that is blank after mode changes.

- [ ] **Step 6: Commit**

```powershell
git add urbanflow-ui/src/TripAnomalies.tsx urbanflow-ui/src/TripScoreForm.tsx urbanflow-ui/src/TripAnomalyMap.tsx urbanflow-ui/src/styles.css urbanflow-ui/qa/workspaces.spec.ts
git commit -m "feat(ui): add interactive kmeans trip check"
```

---

### Task 7: Training Information And Two-worker Diagram

**Files:**
- Create: `urbanflow-ui/src/TrainingInformation.tsx`
- Create: `urbanflow-ui/src/TrainingPipeline.tsx`
- Modify: `urbanflow-ui/src/MachineLearning.tsx`
- Modify: `urbanflow-ui/src/styles.css`
- Modify: `urbanflow-ui/qa/workspaces.spec.ts`

**Interfaces:**
- Consumes: `GET /v1/ml/information` through `useResource<TrainingInformation>`.
- Produces: independent `/ml/information` page with demand/anomaly pipeline,
  evidence, preprocessing matrix, techniques and limitations.

- [ ] **Step 1: Add failing content/evidence tests**

Assert the page contains both model families, two pipeline sections, `336`
history buckets, chronological split, observed mask, no mean/mode fill, K-means
`k=8`, train/calibration/screen periods, threshold percentile, DDP world size,
two distinct training hosts, per-rank batches and checksum synchronization.

- [ ] **Step 2: Add honesty and degraded-state tests**

Assert copy includes `Training-time evidence` and does not include `currently
running`, `fraud probability` or `accuracy`. Mock unavailable K-means information
and assert demand pipeline remains visible with an anomaly warning. Mock total
information API failure and assert cached data remains if available plus a
visible refresh warning.

- [ ] **Step 3: Implement the page composition**

Use full-width bands/unframed sections. Recommended order:

1. Research-only status and model selector summary.
2. Two pipeline diagrams.
3. Distributed training evidence with master/orchestrator, rank 0, rank 1,
   all-reduce and SeaweedFS checkpoint.
4. Data preprocessing/missing policy table.
5. Feature extraction comparison.
6. Technique glossary.
7. Measured quality, limitations and what remains unproven.

Measured values come only from the API. Explanatory copy may come from the
API technique contract. Never infer an unavailable value as zero.

- [ ] **Step 4: Implement the diagram**

Use semantic ordered containers, CSS grid/flex, borders/arrows and Lucide icons.
Give the diagram an accessible region label and text equivalents. Desktop may
show workers in parallel; mobile must stack them while preserving reading order.
Do not use manual SVG or a decorative graph library.

- [ ] **Step 5: Run desktop/mobile visual verification**

```powershell
cd urbanflow-ui
npm run build
npx playwright test qa/workspaces.spec.ts --grep "Training information" --project=desktop
npx playwright test qa/workspaces.spec.ts --grep "Training information" --project=mobile
```

Explicitly assert `scrollWidth === clientWidth` and no page errors. Inspect
screenshots at 1440x900 and 390x844 for overlap, tiny text, truncated hostnames
and misleading status color.

- [ ] **Step 6: Commit**

```powershell
git add urbanflow-ui/src/TrainingInformation.tsx urbanflow-ui/src/TrainingPipeline.tsx urbanflow-ui/src/MachineLearning.tsx urbanflow-ui/src/styles.css urbanflow-ui/qa/workspaces.spec.ts
git commit -m "feat(ui): explain distributed ML training evidence"
```

---

### Task 8: Cache, Refresh And Navigation Regression Audit

**Files:**
- Modify: `urbanflow-ui/src/api.ts` only if current cache behavior cannot support
  the new resources.
- Modify: `urbanflow-ui/qa/workspaces.spec.ts`
- Modify: `urbanflow-ui/qa/map-diagnostic.spec.ts` only for score-map coverage.

**Interfaces:**
- Consumes: all routes and APIs from Tasks 2-7.
- Produces: proof that ML navigation does not reload the document or poll APIs.

- [ ] **Step 1: Extend cache request-count test**

Count `/v1/anomaly/trips`, `/v1/ml/information`, forecast options/evidence and
zone requests. Navigate across all three ML routes and back to Streaming;
assert already-loaded GET resources do not refetch automatically. Press each
visible Refresh button and assert exactly one corresponding re-read.

- [ ] **Step 2: Extend document-reload test**

Set a marker on `window`, switch among Forecast, K-means and Information, and
assert it remains. This proves route changes use `pushState`, not reload.

- [ ] **Step 3: Add repeated-mode map test**

Switch Review queue -> Check a trip -> Review queue multiple times. Assert one
working map canvas, no page errors, correct selected zones and functional
recenter after zooming far out.

- [ ] **Step 4: Run all Playwright projects**

```powershell
cd urbanflow-ui
npx playwright test
```

Expected: desktop and mobile PASS with no spontaneous reload, overflow or blank map.

- [ ] **Step 5: Commit**

```powershell
git add urbanflow-ui/src/api.ts urbanflow-ui/qa
git commit -m "test(ui): audit ML cache navigation and maps"
```

---

### Task 9: Documentation And Operator Handoff

**Files:**
- Modify: `README.md`
- Modify: `urbanflow-ml/README.md`
- Modify: `start-urbanflow.bat`
- Modify: `STATUS.md` if it exists in this integration worktree; otherwise do not create a duplicate status file.

**Interfaces:**
- Consumes: verified behavior from Tasks 1-8.
- Produces: accurate startup, usage, technical explanation and known-limit documentation.

- [ ] **Step 1: Make the one-command launcher detect Compose correctly**

Update `start-urbanflow.bat` to prefer `docker compose` when its version command
succeeds and otherwise use the installed standalone `docker-compose`. Store the
selected command once and use it for `up`. Fail with a clear message only when
neither form exists. This machine currently exposes `docker-compose version
v5.5.0` and does not expose the `docker compose` plugin, so both branches matter.
Support optional `URBANFLOW_ENV_FILE`; default it to `%URBANFLOW_ROOT%\.env` and
pass `--env-file` to Compose. This lets the integration worktree test with the
root `.env` without copying secrets.

- [ ] **Step 2: Update root route guide**

Keep `./start-urbanflow.bat` as the single startup command. Document the three
canonical ML URLs and state that the old Streamlit port 8501 is archived and
must not be used.

- [ ] **Step 3: Update ML README**

Explain, in Vietnamese plus concise B1-B2 English terms:

- type/owner/purpose/how/example/alternative/opposite for GTQN-Lite, DDP,
  TorchDistributor, Gloo and K-means;
- preprocessing, feature extraction, missing-data policy and chronological splits;
- how to use Check a trip and interpret score/threshold;
- why score is not probability or fraud classification;
- how two-worker evidence was proven and why it is not live health;
- actual quality limits, including weekly baseline outperforming the GTQN run.

- [ ] **Step 4: Record validated commands and status**

Only document commands that pass in Task 10. Do not claim 100% or production
readiness; use `research-only integrated release` and list remaining evidence gaps.

- [ ] **Step 5: Verify both launcher selection paths without starting duplicate services**

Factor the detection so `start-urbanflow.bat` prints the selected Compose form
before `up`. On this machine set `URBANFLOW_ENV_FILE=C:\Users\DELL\Documents\Big Data\.env`,
run with `URBANFLOW_SKIP_OPEN=1`, and confirm it selects `docker-compose`.
Review the fallback branch statically or with a PATH shim in a test process; do
not uninstall or alter Docker Desktop.

- [ ] **Step 6: Commit**

```powershell
git add README.md urbanflow-ml/README.md start-urbanflow.bat STATUS.md
git commit -m "docs: document ML workspaces and evidence limits"
```

If `STATUS.md` does not exist, omit it from `git add`.

---

### Task 10: Full Release Verification

**Files:**
- Modify only files required to fix a verified failure. Add a regression test
  before each bug fix.

**Interfaces:**
- Consumes: complete implementation.
- Produces: command evidence that the integrated branch is ready for human review.

- [ ] **Step 1: Inspect repository hygiene**

```powershell
git status --short
git diff --check
git diff --stat 24a9638..HEAD
```

Confirm no `.env`, token, secret, generated artifact, screenshot, data,
checkpoint, `dist`, `node_modules` or Playwright output is staged.

- [ ] **Step 2: Validate Compose and Foundation**

```powershell
docker-compose --env-file "C:\Users\DELL\Documents\Big Data\.env" config --quiet
docker-compose --env-file "C:\Users\DELL\Documents\Big Data\.env" run --rm foundation-check
```

Expected: exit 0 and `FOUNDATION FIXTURES = PASS`.

- [ ] **Step 3: Run complete backend tests**

```powershell
docker-compose --env-file "C:\Users\DELL\Documents\Big Data\.env" run --rm --no-deps --entrypoint /usr/bin/python3 ml-api -m unittest discover -s tests -v
```

Expected: all PASS.

- [ ] **Step 4: Run complete frontend static checks**

```powershell
cd urbanflow-ui
npm test
npm run build
```

Expected: Vitest PASS and TypeScript/Vite build exit 0.

- [ ] **Step 5: Start the real stack without opening extra windows**

```powershell
cd ..
$env:URBANFLOW_SKIP_OPEN='1'
$env:URBANFLOW_NONINTERACTIVE='1'
$env:URBANFLOW_ENV_FILE='C:\Users\DELL\Documents\Big Data\.env'
cmd /c start-urbanflow.bat
Remove-Item Env:URBANFLOW_SKIP_OPEN
Remove-Item Env:URBANFLOW_NONINTERACTIVE
Remove-Item Env:URBANFLOW_ENV_FILE
```

Expected: `URBANFLOW READY`; ML API 8002, React 5173 and Spark UI 8080 respond.

- [ ] **Step 6: Smoke-test real API contracts**

```powershell
Invoke-RestMethod http://127.0.0.1:8002/v1/ml/information
Invoke-RestMethod http://127.0.0.1:8002/v1/anomaly/trips
$body = @{pickup_zone_id=43;dropoff_zone_id=161;trip_distance_miles=4.2;trip_duration_minutes=18;fare_amount_usd=22.5} | ConvertTo-Json
Invoke-RestMethod http://127.0.0.1:8002/v1/anomaly/score -Method Post -ContentType 'application/json' -Body $body
```

Confirm values are finite, artifact/run IDs match, and no Spark application is
created by the POST request.

- [ ] **Step 7: Run the complete browser suite**

```powershell
cd urbanflow-ui
npx playwright test
```

Expected: every desktop and mobile test PASS.

- [ ] **Step 8: Manual visual review**

Open `/ml`, `/ml/kmeans`, and `/ml/information`. Review all success, loading,
degraded and error states; both K-means modes; map fallback/recenter; browser
Back/reload; long hostnames; units; claims; typography; overflow and mobile
stacking. Check browser console and network panel for errors or polling.

- [ ] **Step 9: Review the final diff**

```powershell
git status --short --branch
git log --oneline -12
git diff --check 24a9638..HEAD
```

The work is complete only if all acceptance criteria in the spec map to passing
tests or direct manual evidence. If any gate fails, add a regression test, fix
the cause, rerun the smallest failing gate, then rerun Steps 2-8.

- [ ] **Step 10: Final verification commit if needed**

Only when verification required actual code/test/doc changes:

```powershell
git add <verified-fix-files>
git commit -m "fix: close ML release verification gaps"
```

Do not create an empty commit. Do not push until the user explicitly requests
the final verified branch push.

---

## Executor Progress Protocol

The GPT 5.6 Terra executor should follow this exact loop:

1. Read the spec, this plan, `AGENTS.md`, and the files listed in the current task.
2. Work on one task only; do not pre-edit later tasks.
3. Mark each completed checkbox in this plan.
4. Run the focused RED/GREEN commands before committing.
5. Record unexpected findings directly below the relevant task under
   `Implementation notes`; do not silently redesign the contract.
6. Commit the task, then continue automatically to the next task.
7. Never report completion from code inspection alone; Task 10 evidence is mandatory.

## Definition Of Done

- Three independent, canonical ML workspaces are usable.
- K-means can score a user-entered trip from the immutable artifact without Spark.
- Training Information accurately displays preprocessing, features, model
  training, distributed evidence, prediction outputs, benchmarks and limits.
- Missing data is explained as rejection/mask/missing-indicator policy, not
  generic mean/mode filling.
- Two-worker evidence is visibly historical training evidence, not live health.
- Existing Forecast, Streaming, maps, cache and Back/reload behavior still work.
- Every verification gate in Task 10 passes on the integration worktree.
