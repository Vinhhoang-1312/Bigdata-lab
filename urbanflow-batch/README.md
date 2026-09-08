# UrbanFlow — `urbanflow-batch`

**Subproject A — Historical Batch Analytics**
**Foundation baseline:** `urbanflow-foundation` 1.0.0
**Primary technologies:** Apache Spark, SeaweedFS/S3A, Parquet

## Full documentation

See
[docs/UrbanFlow_urbanflow-batch_Subproject_Overview_Architecture_and_Development_Guide_2026-09-06.md](../docs/UrbanFlow_urbanflow-batch_Subproject_Overview_Architecture_and_Development_Guide_2026-09-06.md)
for the functional specification, architecture, and development guide.

Cross-codebase contracts: [../urbanflow-foundation/README.md](../urbanflow-foundation/README.md).
Project entry point: [../README.md](../README.md).

## Overview

`urbanflow-batch` builds the historical, at-rest side of UrbanFlow. It ingests
raw NYC taxi-trip source data and refines it through a medallion pipeline:

```text
Source Data -> Bronze -> Silver -> Historical Gold
```

- **Bronze** — raw records landed as-is.
- **Silver** — cleaned and typed trips conforming to the Foundation Silver
  contract. Consumed by `urbanflow-replay`.
- **Historical Gold** — per-zone, per-time-bucket baseline statistics (mean trip
  count, and inputs for a nullable z-score). Consumed by `urbanflow-streaming`.

It answers the question *"what normally happens in a particular city zone at a
particular time?"* — the historical half of UrbanFlow's demand-ratio comparison.

The subproject is independently buildable. During independent development it
reads `urbanflow-foundation/fixtures/bronze/` and diffs its Silver output
against `fixtures/silver/expected_silver_from_bronze.jsonl`.

## Implementation status

The independent Batch Definition of Done is complete. Implemented and validated:
managed source acquisition, byte-preserving Bronze landing, mixed-schema
monthly Parquet ingestion, reason-coded Bronze → Silver cleaning, canonical
partitioned Silver, RDD/DataFrame/SQL equivalence, zero-inclusive Historical
Gold with `stddev_pop`, S3A read-back, and two-worker Volume execution.

January 2023 is the frozen historical period; February 2023 is the held-out
replay period. The direct Silver → Replay boundary now passes; Historical Gold
→ Streaming acceptance remains deferred until Streaming exists. See
[docs/implementation-status.md](docs/implementation-status.md).

## Run the fixture implementation

From the repository root:

```bash
docker compose up -d
docker compose run --rm spark-submit \
  --master 'local[2]' \
  /workspace/urbanflow-batch/tests/run_fixture_tests.py

docker compose run --rm spark-submit \
  --master 'local[2]' \
  /workspace/urbanflow-batch/spark/bronze_to_silver.py \
  --config /workspace/urbanflow-batch/config/settings.fixture.yaml
```

Expected results include:

```text
BATCH FIXTURE TESTS = PASS
source_rows=15
valid_rows=10
rejected_rows=5
silver_rows=10
silver_readback_rows=10
BRONZE TO SILVER = PASS
```

Detailed instructions: [runbooks/run_fixture_smoke_test.md](runbooks/run_fixture_smoke_test.md).
The complete real-data pipeline is in [runbooks/run_full_batch.md](runbooks/run_full_batch.md).

## Source data

Public **NYC TLC Trip Record Data** — Yellow-taxi monthly Parquet
(`https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`,
landing page <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>),
plus the taxi-zone lookup
(<https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv>) and
taxi-zone boundaries
(<https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip> /
<https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON>).
Full list and usage: the guide below, §1.3 and §3.1.
