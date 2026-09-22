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

2026-09-13 runtime lock attempt:
compose config: docker-compose config --quiet exited 0
candidate package file updated:
org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0
org.apache.hadoop:hadoop-aws:3.5.0
runtime smoke status: validated after Docker Desktop was started
docker storage note:
D:\Docker\DockerDesktopWSL\disk\docker_data.vhdx is the Docker Desktop storage file
foundation-check: FOUNDATION FIXTURES = PASS
spark local smoke: SPARK = PASS, Spark 4.2.0, Java 21.0.11+10-LTS
S3A smoke: WRITE = PASS, READ = PASS, DATA = PASS
Kafka connector smoke: KAFKA CONNECTOR = PASS
evidence file: urbanflow-streaming/docs/environment-lock.md

2026-09-13 Kafka to Spark acceptance smoke:
compose profile: integration
Kafka broker: apache/kafka:3.9.1
container bootstrap: kafka:9092
host bootstrap: localhost:9094
canonical topic: urbanflow.trip-events, 4 partitions, replication factor 1
malformed smoke topic: urbanflow.trip-events.smoke-invalid, 1 partition
smoke run id: smoke-task4-final-001
result: Kafka -> Spark = PASS
valid_count: 7
invalid_count on canonical topic for this run: 0
pickup_zone_ids: 161, 236
malformed invalid_count: 1
malformed validation_error: missing_required_field
evidence file: urbanflow-streaming/docs/results/kafka-spark-smoke.md

2026-09-13 Batch Historical Gold boundary:
contract path: s3a://urbanflow/gold/historical_zone_baseline/
storage service: SeaweedFS S3A, bucket urbanflow
physical Docker storage:
D:\Docker\DockerDesktopWSL\disk\docker_data.vhdx
Batch source commit checked: 749ea2e5420b538d45280f86b48569a576ac1ad9
result: PASS after running Batch pipeline from origin/urbanflow-batch
source acquisition output:
D:\UrbanFlow\local-data\nyc-tlc
bronze source rows: 5,980,721
silver rows: 5,930,325
rejected rows: 50,396
historical rows: 534,240
historical readback rows: 534,240
duplicate historical keys: 0
Streaming verifier created:
urbanflow-streaming/tests/verify_batch_gold_boundary.py
evidence file: urbanflow-streaming/docs/results/batch-gold-boundary.md

2026-09-14 End-to-end Streaming bounded run:
result: PASS
run id: task6-20260914-003
replay source: origin/urbanflow-replay temporary image urbanflow/replay-temp:task6
replay records sent: 10,000
replay records rejected: 0
replay send errors: 0
streaming verifier:
urbanflow-streaming/tests/verify_integrated_run.py
Kafka topic: urbanflow.trip-events
Historical Gold input:
s3a://urbanflow/gold/historical_zone_baseline/
Live Gold output:
s3a://urbanflow/gold/live_zone_metrics/task6/task6_20260914_003/
Streaming checkpoint:
s3a://urbanflow/checkpoints/live_trip_metrics/task6/task6_20260914_003/
reference rows: 4,394
live rows: 4,394
mismatched rows: 0
invalid rows: 0
evidence file: urbanflow-streaming/docs/results/end-to-end-streaming.md

2026-09-14 Analytics app smoke:
result: PASS
historical input:
s3a://urbanflow/gold/historical_zone_baseline/
live input:
s3a://urbanflow/gold/live_zone_metrics/task6/task6_20260914_003/
s3 endpoint: http://localhost:8333
historical rows: 534,240
live rows: 4,394
prepared analytics view rows: 4,394
latest-window rows: 1
painted taxi-zone polygons: 1
Streamlit launch: HTTP 200
Streamlit URL: http://127.0.0.1:8501
Streamlit PID: 14016
code changes:
analytics_app/data_access.py now handles s3a:// paths through s3fs using
URBANFLOW_S3_ENDPOINT/AWS credentials.
evidence file: urbanflow-streaming/docs/results/analytics-smoke.md

2026-09-14 ML Silver DATA-GO audit:
result: PASS after loading the missing raw months on drive D and rebuilding Silver
silver input:
s3a://urbanflow/silver/trips/
total rows: 15,448,035
required months:
2022-08, 2022-09, 2022-10, 2022-11, 2022-12, 2023-01, 2023-02
observed months:
2001-01, 2002-12, 2008-12, 2009-01, 2022-07, 2022-08, 2022-09,
2022-10, 2022-11, 2022-12, 2023-01, 2023-02, 2023-03
missing required months: none
new raw source months on D:
D:\UrbanFlow\local-data\nyc-tlc\yellow_tripdata_2022-08.parquet
D:\UrbanFlow\local-data\nyc-tlc\yellow_tripdata_2022-09.parquet
D:\UrbanFlow\local-data\nyc-tlc\yellow_tripdata_2022-11.parquet
source acquisition bytes: 149,456,078
Bronze append note:
the Batch Bronze landing command wrote the three months but exited 1 because
its assertion compares selected-period local rows with the full already-appended
Bronze table. The follow-up Bronze-to-Silver job exited 0.
Bronze-to-Silver refresh:
source rows: 15,569,882
valid Silver rows: 15,448,035
rejected rows: 121,847
result: BRONZE TO SILVER = PASS
null counts on required non-null columns: all 0
invalid range counts:
dropoff_before_pickup=0
negative_fare_amount=0
negative_trip_distance=0
negative_trip_duration=0
decision:
ML DATA-GO is open. The current layer is Layer 5: build the 30-minute demand
frame, leakage-safe features, baselines, distributed Spark MLlib training, and
quality evaluation. Do not claim model quality until DISTRIBUTION-GO and
QUALITY-GO are verified.
evidence file: urbanflow-ml/docs/results/silver-audit.md

2026-09-15 ML Demand Frame smoke:
result: PASS
run id: task9-smoke-20260915-001
silver input:
s3a://urbanflow/silver/trips/
zone lookup input:
s3a://urbanflow/bronze/zone_lookup/
date range:
2023-01-02 to 2023-01-02
output path:
s3a://urbanflow/dev/ml/demand_frame/task9-smoke-20260915-001/
zones: 265
rows: 12,720
row math:
265 zones * 48 half-hour slots = 12,720 rows
test coverage:
Spark container unittest covers zero-trip zones, 00/30 minute boundaries,
unique keys, spring-forward invalid slots, and fall-back wall-clock bucket
combining.
decision:
Task 9 smoke is open. This proves the demand-frame builder can create and write
a zero-inclusive one-day development frame. It does not yet claim the full
August 2022 through February 2023 training frame or model quality.
evidence file: urbanflow-ml/docs/results/demand-frame-smoke.md

2026-09-15 ML Leakage-safe feature unit gate:
result: PASS
Spark test command:
docker-compose --profile integration run --rm spark-submit /workspace/urbanflow-ml/tests/test_features.py
test result:
Ran 5 tests in 34.127s
OK
coverage:
- target label changes do not alter lag or trailing features for that target row
- trailing_2h_mean uses the prior four valid windows and excludes the target
- previous-day lag joins only an earlier service date with matching zone and slot
- missing previous-day/week lag values are filled with 0.0 and explicit flags
- per-zone windows do not mix zones
- spring-forward gaps use the previous valid wall-clock bucket, not a future row
- cyclical time features are deterministic and do not require fitted scalers
decision:
Task 10 unit gate is open. The project can move to honest baselines and metrics.

2026-09-15 ML Baseline and evaluation metrics gate:
result: PASS
Spark evaluation test command:
docker-compose --profile integration run --rm spark-submit /workspace/urbanflow-ml/tests/test_evaluation.py
test result:
Ran 6 tests in 39.512s
OK
regression check command:
docker-compose --profile integration run --rm spark-submit /workspace/urbanflow-ml/tests/test_features.py
test result:
Ran 5 tests in 35.181s
OK
local syntax and package test commands:
.\urbanflow-streaming\.venv\Scripts\python.exe -m py_compile urbanflow-ml\forecasting\baselines.py urbanflow-ml\forecasting\evaluation.py urbanflow-ml\tests\test_evaluation.py
.\urbanflow-streaming\.venv\Scripts\python.exe -m pytest -q urbanflow-ml\tests --basetemp .tmp\pytest-ml-local
local pytest result:
4 passed, 3 skipped in 0.06s
coverage:
- MAE, RMSE, WAPE, signed bias, clamp rate use hand-calculated expected values
- WAPE returns null with a clear warning when total actual demand is zero
- top-k recall measures overlap between actual peak zones and predicted peak zones
- summary frames cover peak-window, borough, demand-decile, and per-zone slices
- seasonal naive prediction uses only the already-built same-slot previous-week feature
- historical mean baseline fits only training rows keyed by zone, day-of-week, and half-hour slot
implemented files:
urbanflow-ml/forecasting/baselines.py
urbanflow-ml/forecasting/evaluation.py
urbanflow-ml/tests/test_evaluation.py
decision:
Task 11 gate is open. The project can move to Task 12: train the distributed
Spark MLlib GBT demand model, then compare it with seasonal-naive and
train-only historical-mean baselines. ML DISTRIBUTION-GO and QUALITY-GO are
still not claimed.

2026-09-15 ML GBT model unit gate:
result: PASS
environment change:
Spark services now use local image `urbanflow/spark-ml:4.2.0-python3`, built
from `docker/spark-ml/Dockerfile`. The image installs `numpy==2.2.6` and
`PyYAML==6.0.3` because `pyspark.ml` requires numpy and the training script
reads YAML config.
compose check:
docker-compose config --quiet exited 0
docker build check:
docker-compose build spark-submit exited 0
Spark model test command:
docker-compose --profile integration run --rm spark-submit /workspace/urbanflow-ml/tests/test_model.py
test result:
Ran 3 tests in 40.495s
OK
regression check command:
docker-compose --profile integration run --rm spark-submit /workspace/urbanflow-ml/tests/test_evaluation.py
test result:
Ran 6 tests in 25.595s
OK
local syntax and package test commands:
.\urbanflow-streaming\.venv\Scripts\python.exe -m py_compile urbanflow-ml\forecasting\model.py urbanflow-ml\spark\train_demand_forecast.py urbanflow-ml\tests\test_model.py
.\urbanflow-streaming\.venv\Scripts\python.exe -m pytest -q urbanflow-ml\tests --basetemp .tmp\pytest-ml-local
local pytest result:
4 passed, 4 skipped in 0.05s
implemented files:
docker/spark-ml/Dockerfile
urbanflow-ml/forecasting/model.py
urbanflow-ml/spark/train_demand_forecast.py
urbanflow-ml/tests/test_model.py
compose.yaml
coverage:
- Spark MLlib Pipeline fits a small two-zone frame
- pipeline uses StringIndexer, OneHotEncoder, VectorAssembler, and GBTRegressor
- predictions retain key columns and finite `raw_prediction`
- `predicted_trip_count` clamps negative raw predictions at 0.0
- PipelineModel saves and loads successfully
- parameter selection evaluates validation MAE and keeps deterministic tie breakers
- training script can build demand frame/features, split train/validation/test,
  select bounded candidates, refit train+validation, and save immutable run
  paths; full distributed run is deferred to Task 13
decision:
Task 12 unit gate is open. This proves the model code and training entrypoint
are structurally runnable in Spark MLlib. It does not prove two-worker
distributed execution or final model quality. The project can move to Task 13:
event-log verification for DISTRIBUTION-GO.
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
3. For Streaming, keep the Streamlit app running only while demoing, or stop
   PID `14016` when finished.
4. For ML, choose one of two paths:
   continue from model unit gate PASS into two-worker distributed execution
   proof and final QUALITY-GO decision.
