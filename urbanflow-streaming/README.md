# UrbanFlow — `urbanflow-streaming`

**Subproject C — Live Processing and Analytics Serving**
**Foundation baseline:** `urbanflow-foundation` 1.0.0
**Primary technologies:** Apache Spark Structured Streaming, Kafka, SeaweedFS/S3A, Parquet, FastAPI, React/MapLibre (Streamlit is legacy)

## Full documentation

See
[docs/UrbanFlow_urbanflow-streaming_Subproject_Overview_Architecture_and_Development_Guide_2026-09-06.md](../docs/UrbanFlow_urbanflow-streaming_Subproject_Overview_Architecture_and_Development_Guide_2026-09-06.md)
for the functional specification, architecture, and development guide.

Cross-codebase contracts: [../urbanflow-foundation/README.md](../urbanflow-foundation/README.md).
Project entry point: [../README.md](../README.md).

## Overview

`urbanflow-streaming` is the live end of UrbanFlow. It consumes the replayed
Kafka trip events, joins them against the historical Gold baseline, and produces
live Gold metrics served through an analytics application:

```text
Kafka + Historical Gold -> Live Gold -> Analytics Application
```

- Windowed streaming aggregation of `urbanflow.trip-events` into current
  per-zone trip counts.
- Join with the historical baseline to compute the **demand ratio**
  (current count / historical mean) and a nullable z-score.
- A React workspace reads Live Gold through a thin FastAPI service. Streamlit
  is archived and is not a supported user-facing runtime.

The Kafka event contract, historical/live Gold contracts, storage paths, time
semantics, and null/join rules follow `urbanflow-foundation` 1.0.0.

The subproject is independently buildable. During independent development it
reads `urbanflow-foundation/fixtures/kafka/` and
`fixtures/gold/historical_zone_baseline.jsonl`, and diffs its output against
`fixtures/gold/expected_live_zone_metrics.jsonl`.

## Source data

`urbanflow-streaming` consumes Kafka trip events (from `urbanflow-replay`) and
the historical Gold baseline (from `urbanflow-batch`), both derived from the
public **NYC TLC Trip Record Data**
(<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>). The taxi-zone
lookup (<https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv>) and
boundary GeoJSON
(<https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON>)
are useful for labelling zones in the analytics app. Full list: the guide
below, §1.3.

## Install and runtime profiles

From `urbanflow-streaming/`, create a virtual environment and select only the
profile needed by the process:

```bash
python -m venv .venv && . .venv/bin/activate
python -m pip install -r requirements/offline.txt   # PySpark, fixtures, tests
python -m pip install -r requirements/streaming.txt # Kafka job Python side
python -m pip install -r requirements/analytics.txt # legacy data-access modules
```

Kafka and S3A are JVM connectors, not Python wheels. Candidate compatible
coordinates are in `spark-packages.conf`. SeaweedFS uses Hadoop S3A with
path-style access. `config/settings.yaml` is the offline profile; copy
`settings.example.yaml` for local overrides. Foundation 1.0.0 fixes the New
York analytics timezone, five-minute windows, canonical topic and storage
paths. This implementation uses the configured ten-minute watermark. Invalid
Kafka values retain `raw_value` and a validation reason.

## Offline deterministic mode

```bash
cd urbanflow-streaming
python ../urbanflow-foundation/checks/validate_contract_fixtures.py
pytest -q
```

The suite reads the authoritative event, historical-baseline and expected-live
JSONL fixtures. It covers malformed data, exact window boundaries, watermark
presence, late-data configuration, missing and zero baselines, ratio/z-score
null rules, left-join preservation, checkpoint restart and analytics access.

For a finite file-stream run, first copy JSONL input into a generated directory.
Use the fixture example config because its Live Gold and checkpoint paths are
writable generated paths. The expected fixture remains a read-only oracle:

```bash
mkdir -p output/fixture-events
cp ../urbanflow-foundation/fixtures/kafka/trip_events.jsonl output/fixture-events/
spark-submit spark/live_trip_metrics.py --config config/settings.fixture.example.yaml \
  --fixture-events output/fixture-events/ --available-now
```

The runner rejects equal Live Gold/checkpoint paths and rejects generated output
under `urbanflow-foundation/fixtures/`.

## Integrated Kafka, S3A, checkpoint and Live Gold

Use a copied integrated config with these canonical values:

```yaml
profile: integrated
storage:
  historical_baseline: s3a://urbanflow/gold/historical_zone_baseline/
  live_metrics: s3a://urbanflow/gold/live_zone_metrics/
  streaming_checkpoint: s3a://urbanflow/checkpoints/live_trip_metrics/
```

Export `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`; never put credentials in
configuration. Start the continuous stream with:

```bash
set -a; . ./spark-packages.conf; set +a
spark-submit --packages "$SPARK_PACKAGES" \
  --conf spark.sql.session.timeZone=America/New_York \
  --conf spark.hadoop.fs.s3a.endpoint=http://localhost:8333 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  spark/live_trip_metrics.py --config config/settings.integrated.yaml
```

The Parquet sink and stable, separate checkpoint provide restart-safe commits.
Do not delete the checkpoint between restarts, reuse it for another query, or
place it inside Gold. The historical loader automatically reads fixture JSONL
or integrated Parquet and validates the Foundation schema in both modes.

## React UI

The primary Streaming workspace is `http://127.0.0.1:5173/streaming` after
running `start-urbanflow.bat` from the repository root. The old Streamlit UI is
archived and is no longer started or supported as a user-facing interface. Its
source remains in `analytics_app/` because the API still reuses selected
data-access modules. See [the archive note](docs/archive/streamlit-ui.md).
The current configured Gold path is a bounded February 2023 replay. The
Reliability view now reads Spark Master's active-app list and reports whether
`UrbanFlowLiveMetrics` is running. This check is separate from Kafka lag and
invalid-record telemetry, which are still unavailable. See the
[release-readiness report](../docs/release-readiness-2026-09-25.md).

### Monthly playback

In **Streaming / Dispatch**, choose **Playback** to inspect a month as a map
animation. The month selector, timeline, step controls, and Play/Pause button
move only through completed five-minute windows that actually exist in Live
Gold; missing windows are never presented as zero demand. The adjacent **Taxi
pickups by hour** column chart groups the selected month by local
`America/New_York` pickup hour. It is an operational exploration of stored
Gold, not a claim that the replay is a current city feed.

```text
GET /v1/streaming/replay/catalog
GET /v1/streaming/replay/frame?window_start=<RFC-3339 timestamp with offset>
```

The frame route rejects timestamps without an explicit offset and returns 404
for a window absent from Gold, so the UI never silently fabricates a timeline.

## Read-only Streaming API

`serving/app.py` exposes a small FastAPI read model over Gold. It is for
programmatic status and overview reads; it does not run Spark transformations.
From a container on the UrbanFlow Compose network, configure all S3A paths and
the SeaweedFS endpoint explicitly:

```bash
export URBANFLOW_HISTORICAL_PATH=s3a://urbanflow/gold/historical_zone_baseline/
export URBANFLOW_LIVE_PATH=s3a://urbanflow/gold/live_zone_metrics/task6/stream_health_20260920_001/
export URBANFLOW_ZONE_LOOKUP_PATH=s3a://urbanflow/bronze/zone_lookup/taxi_zone_lookup.csv
export URBANFLOW_S3_ENDPOINT=http://seaweedfs:8333
uvicorn serving.app:app --host 0.0.0.0 --port 8001
```

The API returns `503` when Gold is unavailable or its storage configuration is
invalid. This keeps an infrastructure problem distinct from a successful but
empty analytics result.

## Streaming and backlog measurements

Attach `UrbanFlowMetricsListener` from `spark/streaming_metrics.py` with
`spark.streams.addListener(listener)`. Its `snapshot()` exposes input and
processing rates, micro-batch and processing durations, input/backlog signals,
Kafka end offsets, invalid events, and output rows. For authoritative Kafka lag
in Phase 7, correlate snapshots with:

```bash
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group urbanflow-streaming
```
