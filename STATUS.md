# UrbanFlow Current Status

Snapshot time: 2026-09-13 18:13:50 +07:00  
Repository: `https://github.com/msa30dn/big-data-team-project`  
Snapshot method: local inspection plus `git fetch origin`

## Short Summary

This repository already has README files for the root project and all four
subprojects. Before this file, there was no root `STATUS.md` in the current
`urbanflow-streaming` branch.

Current local branch:

```text
urbanflow-streaming at 68ed3bd
tracking origin/urbanflow-streaming
```

Execution baseline for `plan.md`:

```text
execution_date: 2026-09-13
execution_mode: in-place on branch urbanflow-streaming
local_head: 68ed3bd997fe8936bd8db5ecd76d05b42f453ab7
origin/urbanflow-batch: 749ea2e5420b538d45280f86b48569a576ac1ad9
origin/urbanflow-replay: a7f2cfb557146033ef0991f3526ca5516300dad1
origin/urbanflow-streaming: 68ed3bd997fe8936bd8db5ecd76d05b42f453ab7
selected_phase: Task 0 - execution baseline
worktree_decision: user requested direct work on urbanflow-streaming, no isolated worktree
```

No implementation test is marked as passing from this baseline. Tests are
recorded only after a fresh run.

Execution evidence during Task 1 and Task 2 start:

```text
2026-09-13 foundation validator:
FOUNDATION FIXTURES = PASS

2026-09-13 streaming focused tests:
command: .\.venv\Scripts\python.exe -m pytest -q tests\test_fixture_runner.py tests\test_pipeline.py tests\test_data_access.py
result: 8 passed, 5 skipped in 14.53s

skipped reason:
PySpark is not installed in the local venv, so Spark-backed pipeline tests are
not yet a full baseline pass.

environment:
python: Python 3.12.10
java: OpenJDK 17.0.18 Temurin
pyspark: not installed

dependency setup note:
requirements/offline.txt uses -e .[offline], but editable install currently
fails because setuptools discovers multiple top-level directories in the flat
layout. Direct install of pytest, pandas, pyarrow, and PyYAML succeeded.
PySpark install from the network was too slow and was stopped before completion.
```

Working tree at snapshot time:

```text
?? urbanflow-streaming/docs/
```

That untracked folder contains one plan:

```text
urbanflow-streaming/docs/plans/2026-09-12-replace-streamlit-fastapi-frontend.md
```

## Available Documentation

Existing README files:

```text
README.md
urbanflow-foundation/README.md
urbanflow-batch/README.md
urbanflow-replay/README.md
urbanflow-streaming/README.md
urbanflow-foundation/fixtures/README.md
```

Implementation status files found on remote branches:

```text
origin/urbanflow-batch:urbanflow-batch/docs/implementation-status.md
origin/urbanflow-replay:urbanflow-replay/docs/implementation-status.md
```

Those files are not present in the current local checkout because the checkout
is on `urbanflow-streaming`.

## Foundation

Foundation baseline: `1.0.0`

Status:

- Contracts are frozen.
- Shared contracts define Silver, Kafka trip events, Historical Gold, Live Gold,
  storage paths, topic names, and time semantics.
- Application subprojects must share contracts, not source code.

Important fixed contracts:

```text
Analytics timezone: America/New_York
Kafka topic: urbanflow.trip-events
Kafka partitions: 4
Kafka key: pickup_zone_id as decimal string
Silver path: s3a://urbanflow/silver/trips/
Historical Gold path: s3a://urbanflow/gold/historical_zone_baseline/
Live Gold path: s3a://urbanflow/gold/live_zone_metrics/
Streaming checkpoint: s3a://urbanflow/checkpoints/live_trip_metrics/
```

## Branches After Fetch

```text
origin/main                2d1de2e chore: add docker
origin/urbanflow-batch     749ea2e feat(batch): migrate to YAML config, pyproject.toml, and dynamic paths
origin/urbanflow-replay    a7f2cfb feat(replay): migrate to YAML config, pyproject.toml, and dynamic paths
origin/urbanflow-streaming 68ed3bd Show TLC taxi-zone choropleth instead of two map dots.
origin/howie               daab5ca slides
```

There is no remote branch named `Dung`. This status assumes the
"data processing branch" means `origin/urbanflow-batch`. Note: `origin/howie`
also has commits named `data` and `EDA`, plus slides and notebook work.

## Batch Branch: Data Processing

Assumed branch for Dung/data work:

```text
origin/urbanflow-batch
commit 749ea2e5420b538d45280f86b48569a576ac1ad9
date   2026-09-08 23:45:42 +0700
```

Batch is the data processing project. It builds:

```text
Source Data -> Bronze -> Silver -> Historical Gold
```

Main source files on the branch:

```text
urbanflow-batch/batch/config.py
urbanflow-batch/batch/io.py
urbanflow-batch/batch/schemas.py
urbanflow-batch/batch/source_acquisition.py
urbanflow-batch/batch/transforms.py
urbanflow-batch/batch/validation.py
urbanflow-batch/batch/baseline.py
urbanflow-batch/batch/baseline_validation.py
urbanflow-batch/spark/bronze_to_silver.py
urbanflow-batch/spark/build_gold_baseline.py
urbanflow-batch/tests/run_fixture_tests.py
urbanflow-batch/tests/run_baseline_tests.py
urbanflow-batch/tests/verify_integrated_outputs.py
```

Batch implementation status from its own remote status file:

- Independent Batch Definition of Done is complete.
- It records Foundation version and passes Foundation validation.
- It has source acquisition for January and February 2023.
- January 2023 is the historical period.
- February 2023 is the replay period.
- It supports CSV and mixed-schema monthly Parquet source adapters.
- It maps Bronze to Silver with visible rejection reasons.
- It derives New York time and DST-sensitive fields.
- It writes canonical partitioned Silver.
- It builds Historical Gold with a complete zero-count grid.
- It tests schema, idempotency, S3A read-back, Spark local, and two-worker Spark.
- It documents data cleaning decisions and real-data smoke results.
- It has a direct Silver -> Replay -> Kafka boundary without an ad-hoc adapter.

Deferred integration from Batch:

- Batch canonical Historical Gold -> Streaming without an adapter.
- Environment lock with final Streaming runtime details.
- Final end-to-end integration acceptance.

Output that matters for Streaming:

```text
Silver:          s3a://urbanflow/silver/trips/
Historical Gold: s3a://urbanflow/gold/historical_zone_baseline/
```

## Replay Branch

```text
origin/urbanflow-replay
commit a7f2cfb557146033ef0991f3526ca5516300dad1
date   2026-09-08 22:57:32 +0700
```

Replay turns Silver rows into Kafka trip events:

```text
Silver -> Foundation Kafka Event -> Rate Control -> urbanflow.trip-events
```

Replay implementation status from its own remote status file:

- Independently complete.
- Kafka 3.9.1 single broker in KRaft mode is documented.
- Topic creation is idempotent with 4 partitions and replication factor 1.
- It reads the Foundation Silver fixture.
- It writes strict seven-field JSON events.
- It validates RFC 3339 event time, New York offset, DST, zones, and numeric
  fields.
- It uses pickup zone id as the Kafka key.
- It counts malformed records and prevents them from publishing.
- It has rate control and producer metrics.
- It can read canonical February Silver Parquet directly.
- Batch Silver -> Replay -> Kafka boundary test is done.

Deferred integration from Replay:

- Replay Kafka events -> Streaming without an integration-only adapter.
- Kafka -> Spark Structured Streaming acceptance.
- Final end-to-end Velocity experiment with Streaming active.

Output that matters for Streaming:

```text
Kafka topic: urbanflow.trip-events
Kafka value: Foundation kafka-trip-event.schema.json
Kafka key:   pickup_zone_id as decimal string
```

## Current Streaming Branch

```text
local branch: urbanflow-streaming
commit:       68ed3bd997fe8936bd8db5ecd76d05b42f453ab7
date:         2026-09-09 17:55:40 +0700
```

Streaming currently has:

- Spark event schema parsing with visible invalid rows.
- Spark five-minute event-time window aggregation.
- Historical Gold join.
- Demand ratio and nullable z-score rules.
- Restart-safe Parquet Live Gold writer with checkpoint path checks.
- Metrics listener for streaming/backlog measurements.
- Streamlit analytics app that reads Gold only.
- Taxi-zone GeoJSON choropleth in the app.
- Tests for fixtures, malformed events, boundary/null rules, watermark, restart
  checkpoint behavior, and data access.

Main source files:

```text
urbanflow-streaming/spark/event_schema.py
urbanflow-streaming/spark/historical_lookup.py
urbanflow-streaming/spark/live_trip_metrics.py
urbanflow-streaming/spark/streaming_metrics.py
urbanflow-streaming/analytics_app/app.py
urbanflow-streaming/analytics_app/data_access.py
urbanflow-streaming/analytics_app/zones.py
urbanflow-streaming/tests/test_pipeline.py
urbanflow-streaming/tests/test_data_access.py
```

Current app stack:

```text
Streamlit + Pandas + PyArrow + s3fs + PyDeck + Altair
```

There is an untracked implementation plan to replace Streamlit with:

```text
FastAPI + static MapLibre frontend + Chart.js + Server-Sent Events
```

That plan keeps Analytics thin: the web layer reads Gold only and does not
rebuild ETL logic.

## Your Work Area

Your part has two independent tracks.

### Track 1: Streaming Now, Before Batch Is Fully Integrated

You do not need to wait for Dung/Batch to finish integration. Streaming can run
with Foundation fixtures first, then switch paths later.

Development mode:

```text
Foundation Kafka fixture + Foundation Historical Gold fixture
-> Spark Structured Streaming transform
-> Live Gold fixture-equivalent output
-> Analytics app
```

Integration mode later:

```text
Batch Historical Gold + Replay Kafka events
-> Streaming Live Gold
-> Analytics app
```

Main thing to prove now:

- Streaming can consume the contract shape.
- Streaming can produce Live Gold.
- The app can show useful output with fixture or integrated Gold.
- When Batch/Replay are ready, only paths and runtime services should change.

### Track 2: Distributed ML Training Idea

This is not implemented yet.

Simple definition:

- Spark MLlib is Apache Spark's machine learning library.
- It trains models on large data split across many workers.
- A worker is one machine/process that handles part of the data.
- In distributed training, each worker computes part of the learning work.
- The driver or algorithm combines those partial results.

Important note:

Spark MLlib is good for classic machine learning at scale. It is not the same
as deep learning on GPUs. Some algorithms use gradient-style optimization, but
not every MLlib model is "combine gradients from nodes" in the deep-learning
sense.

Best project ideas for this dataset:

1. Demand forecasting
   - Predict taxi demand per zone and time bucket.
   - Social value: better vehicle positioning, less waiting, less wasted driving.

2. Anomaly detection
   - Detect unusual zone demand compared with history.
   - Social value: city operators can spot events, disruptions, or service gaps.

Recommended focus:

```text
Primary ML task: demand forecasting per pickup zone and time bucket.
Secondary, optional task: anomaly flag from forecast error.
```

This matches the current UrbanFlow product because Streaming already compares:

```text
current demand vs historical demand
```

The ML extension can upgrade that into:

```text
current demand vs predicted demand
```

## Next Actions

1. Keep this `STATUS.md` updated after branch switches, merges, or integration
   test runs.
2. Confirm whether "Dung branch" means `origin/urbanflow-batch` or another
   branch such as `origin/howie`.
3. For Streaming, decide whether to finish the current Streamlit version or
   implement the existing FastAPI + MapLibre plan.
4. For ML, do a focused research pass before coding. The research question
   should be: "Which one useful Spark MLlib model should UrbanFlow train from
   NYC TLC trips, and how will we prove it helps?"
