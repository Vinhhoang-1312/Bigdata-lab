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
