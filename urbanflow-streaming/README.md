# UrbanFlow — `urbanflow-streaming`

**Subproject C — Live Processing and Analytics Serving**
**Foundation baseline:** `urbanflow-foundation` 1.0.0
**Primary technologies:** Apache Spark Structured Streaming, Kafka, SeaweedFS/S3A, Parquet, Streamlit

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
- A Streamlit app surfaces whether current activity is unusual versus history.

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
python -m pip install -r requirements/analytics.txt # Streamlit Gold reader
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

For a file-stream run, first copy JSONL input into a directory and change
`storage.live_metrics` in a copied config to a writable Parquet directory (the
expected fixture is a read-only oracle):

```bash
mkdir -p /tmp/urbanflow-events
cp ../urbanflow-foundation/fixtures/kafka/trip_events.jsonl /tmp/urbanflow-events/
spark-submit spark/live_trip_metrics.py --config config/settings.local.yaml \
  --fixture-events /tmp/urbanflow-events/
```

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

## Streamlit analytics

Fixture data is the default:

```bash
streamlit run analytics_app/app.py
```

Integrated data is selected by paths only; the app remains a thin Gold reader:

```bash
export URBANFLOW_HISTORICAL_PATH=s3://urbanflow/gold/historical_zone_baseline/
export URBANFLOW_LIVE_PATH=s3://urbanflow/gold/live_zone_metrics/
streamlit run analytics_app/app.py
```

Configure `s3fs` for the SeaweedFS endpoint when reading integrated storage.
The app includes overview KPIs, current-versus-historical charts, zone/date
filters, a zone explorer, unusual-zone ranking, and Live/Historical tables.

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
