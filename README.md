# UrbanFlow — Big Data Analytics Demo

A teaching demo of one full Big Data pipeline, built on public
[**NYC TLC taxi-trip records (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)**](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

```text
Data Sources -> Ingestion -> Storage -> Processing -> Analytics
```

**Foundation version:** 1.0.0

## What it does

Two kinds of data, one pipeline:

```text
Big history at rest   -> Volume
Fast event stream     -> Velocity
Parquet/JSON/CSV/GeoJSON -> Variety
```

Three questions:

```text
History:  what normally happens in this zone at this time?
Live:     what is happening there right now?
Compare:  is right now unusual vs history?
```

The score is a simple **demand ratio = current trip count / historical mean**
(z-score kept as a secondary, nullable metric).

## The four codebases

Data flows through A -> B -> C. `urbanflow-foundation` sits under all three: it
holds the contracts they share but is not a step in the pipeline.

```text
                 NYC taxi source data
                          |
                   [ urbanflow-batch ]  (Spark)            = A
                    Bronze -> Silver -> Gold
                    |                     |
             Silver trips           Gold historical
                    |                     |
          [ urbanflow-replay ]            |                 = B
           Silver -> Kafka                |
           (Python + Kafka)               |
                    |                     |
                  Kafka  --------> [ urbanflow-streaming ]  = C
                                    Kafka + Gold historical   (Spark Structured
                                    -> Gold live -> app        Streaming + Streamlit)

  ------------------------------------------------------------------
  [ urbanflow-foundation ]  shared contracts + fixtures under A, B, C
  (schemas, time/path meaning, acceptance tests — imported by none)
```

| Codebase | What it is |
|---|---|
| [urbanflow-foundation/](urbanflow-foundation/README.md) | Shared rules: schemas, time/path meaning, small fixtures, acceptance tests. No code the others import. |
| [urbanflow-batch/](urbanflow-batch/README.md) | A — cleans raw taxi data into Silver trips and the historical Gold baseline. |
| [urbanflow-replay/](urbanflow-replay/README.md) | B — replays Silver trips as a paced Kafka event stream. |
| [urbanflow-streaming/](urbanflow-streaming/README.md) | C — joins the live stream with history into live Gold metrics + an analytics app. |

A, B and C are built and tested on their own machines. They never import each
other; they meet only at **Parquet/S3A datasets and Kafka events** whose meaning
is frozen in `urbanflow-foundation` 1.0.0. Before an upstream codebase is ready,
each one runs on Foundation fixtures instead.

More detail: each subproject README, the guides in [docs/](docs/), and
contracts in [urbanflow-foundation/README.md](urbanflow-foundation/README.md).

## Data

All source data is public NYC TLC data — no account, free to download.

| What | Link | Format |
|---|---|---|
| Dataset home + docs | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page | — |
| Yellow taxi trips (monthly) | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet` | Parquet |
| Taxi-zone lookup | https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv | CSV |
| Taxi-zone shapes | https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip | Shapefile (ZIP) |
| Taxi-zone shapes (GeoJSON) | https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON | GeoJSON |
| Field meanings (yellow) | https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf | PDF |

```text
yellow_tripdata_YYYY-MM.parquet  one file per month, e.g. yellow_tripdata_2023-01.parquet
   tpep_pickup_datetime, tpep_dropoff_datetime   trip start / end time
   PULocationID, DOLocationID                    pickup / dropoff zone id (join to taxi_zone_lookup.csv)
   trip_distance, passenger_count, fare_amount, total_amount
```

Start small: 1–3 months (e.g. `2023-01` to `2023-03`) is enough to build and
test the whole pipeline. The frozen integrated split uses **January 2023 for
history** and **February 2023 for replay**, so the ranges do not overlap.
Usage details: [urbanflow-batch/README.md](urbanflow-batch/README.md) and
`docs/UrbanFlow_urbanflow-batch_Subproject_Overview_Architecture_and_Development_Guide_2026-09-06.md`.

## Setup

### Recommended: Docker Compose

The reproducible development environment includes Python 3.13, Spark 4.2.0
with two workers, Hadoop S3A, SeaweedFS, Kafka 3.9.1, and AWS CLI:

```bash
cp .env.example .env
docker compose up -d --build
docker compose run --rm foundation-check
docker compose run --rm spark-submit
docker compose run --rm kafka-cli
```

Expect `FOUNDATION FIXTURES = PASS`, `WRITE/READ/DATA = PASS`, and a four-partition
`urbanflow.trip-events` topic. See
[docs/docker-environment.md](docs/docker-environment.md) for service addresses,
version locks, cluster submission, logs, and shutdown commands.

The component-level setup below remains useful as a manual reference.

### 0. Foundation check — no infrastructure needed

Needs only Python 3.13 and the standard library. Run this first.

```bash
cd urbanflow-foundation
python checks/validate_contract_fixtures.py
```

Expect:

```text
FOUNDATION FIXTURES = PASS
```

### 1. SeaweedFS — S3-compatible storage (Docker, Windows)

Enable the S3 API explicitly — the image does not. Ports: 8333 S3, 9333 master,
8888 filer.

```powershell
docker run -d --name urbanflow-seaweedfs `
  -p 9333:9333 -p 8888:8888 -p 8333:8333 `
  -v weed-data:/data `
  chrislusf/seaweedfs:4.45 `
  server -dir=/data -s3
```

Create the bucket (`urbanflow`, per
`urbanflow-foundation/contracts/storage-paths.yaml`):

```bash
aws s3api create-bucket --bucket urbanflow --endpoint-url http://localhost:8333
```

The Compose environment locks the validated `4.45` tag and records it with the
Hadoop / `hadoop-aws` / AWS SDK versions in `docs/environment-lock.md`. Keep
credentials in environment variables (see
`urbanflow-foundation/config/config-reference.yaml`), never in source.

### 2. Kafka

Compose starts the locked single-broker Kafka 3.9.1 KRaft service and creates
the canonical topic idempotently:

```bash
docker compose up -d kafka kafka-topic-init
docker compose run --rm kafka-cli
```

Use `kafka:9092` from Compose containers and `localhost:9094` from the host.
Topic name, partitions, and replication factor remain frozen in
`urbanflow-foundation/contracts/kafka-topic.yaml`.

### 3. Record the environment lock

After the smoke tests in `urbanflow-foundation/docs/acceptance-tests.md` pass,
fill `docs/environment-lock.md` from the template in
`urbanflow-foundation/docs/environment-baseline.md`. Record what actually
worked, not what was intended.

## Gates

```text
FOUNDATION-GO   Test 0 passes, contracts frozen
GO-A/B/C        that subproject's local setup + fixtures work
INTEGRATION-GO  tests H, I, J, K pass and the environment lock is filled
```

Definitions: `urbanflow-foundation/docs/acceptance-tests.md`.

## Change control

Foundation contracts are frozen. A change touching more than one subproject goes
into `urbanflow-foundation` first, gets a new version there, then is adopted by
the producer and consumer. See `urbanflow-foundation/docs/change-control.md`.
