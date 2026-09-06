# UrbanFlow — `urbanflow-replay`
## Subproject Overview, Functional Specification, Architecture, and Development Guide

**Date:** 2026-09-06  
**Subproject:** B — Kafka Replay  
**Foundation baseline:** `urbanflow-foundation` 1.0.0  
**Primary technologies:** Python, Apache Kafka  
**Primary pipeline:** Silver → Foundation Kafka Event → Rate Control → `urbanflow.trip-events`  
**Document purpose:** Self-contained implementation and development guide for the `urbanflow-replay` subproject.

> **Authority note:** The Silver logical contract, Kafka event schema, topic configuration, message-key rule, and time semantics are governed by `urbanflow-foundation` 1.0.0. Internal producer design may evolve without changing those shared contracts.


## 1. UrbanFlow Overall Demo Project

### 1.1 What UrbanFlow is

**UrbanFlow** is an educational **Big Data engineering and analytics demonstration** built around public urban-mobility data, primarily NYC taxi-trip records.

The project is designed to demonstrate one understandable end-to-end data story rather than a collection of unrelated technology examples:

```text
Data Sources
    ↓
Ingestion
    ↓
Storage
    ↓
Processing
    ↓
Analytics
```

UrbanFlow works with two complementary forms of data:

- a **large historical dataset at rest**, used to demonstrate **Volume**; and
- a **high-rate replayed event stream**, used to demonstrate **Velocity**.

It also includes controlled **Variety** through Parquet, JSON, CSV, and GeoJSON/geographic data.

UrbanFlow is **not** intended to be a production smart-city platform, a machine-learning system, or a large microservice architecture. Its main purpose is to demonstrate how classic Big Data technologies work together in one coherent system.

### 1.2 Core analytical story

Historical data answers:

> **What normally happens in a particular city zone at a particular time?**

The replayed live-style stream answers:

> **What is happening there right now?**

Combining them allows the demo to answer:

> **Is what is happening right now unusual compared with historical behavior?**

The primary comparison metric is deliberately simple:

```text
Demand Ratio =
Current Trip Count
------------------
Historical Mean Trip Count
```

A nullable z-score is also retained as a secondary metric.

The analytical formula is not the main complexity of the project. The main learning value comes from the **data architecture, distributed processing, batch/stream integration, contract-based development, and analytics-serving pipeline** around it.

### 1.3 Main datasets

UrbanFlow uses several related data representations.

| Dataset | Purpose | Typical format |
|---|---|---|
| NYC taxi-trip records | Main historical transactional dataset | Parquet |
| Taxi-zone lookup | Zone reference data | CSV |
| Taxi-zone boundaries | Optional geographic/reference data | GeoJSON or geographic Parquet |
| Kafka trip events | Simulated live trip stream | JSON |
| Silver trips | Cleaned reusable trip dataset | Parquet |
| Historical Gold baseline | Expected historical activity by zone/time | Parquet |
| Live Gold metrics | Current-vs-historical comparison | Parquet |

#### Where to get the source data

All source datasets are public and require no account. The concrete download
links (used at Gate 7 — data acquisition) are:

| Dataset | Download | Format |
|---|---|---|
| NYC TLC Yellow Taxi trip records | Landing page: <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page> · Monthly files: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet` | Parquet |
| Taxi-zone lookup table | <https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv> | CSV |
| Taxi-zone boundaries (shapefile) | <https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip> | Shapefile (ZIP) |
| Taxi-zone boundaries (GeoJSON) | <https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON> | GeoJSON |
| Yellow trips data dictionary | <https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf> | PDF |
| Working with the Parquet format (TLC note) | <https://www.nyc.gov/assets/tlc/downloads/pdf/working_parquet_format.pdf> | PDF |

`urbanflow-replay` consumes the Silver dataset produced by `urbanflow-batch`
from these sources; it does not read the raw files directly (it uses the
Foundation Silver fixture during independent development).

The integrated periods were frozen after real-data inspection: January 1–31, 2023 is historical, and February 1–28, 2023 is held out for replay. They do not overlap; the authoritative lock is `urbanflow-foundation/config/data-periods-reference.yaml`.

### 1.4 Overall architecture

```text
                         PUBLIC NYC MOBILITY DATA
                                  │
                                  ▼
                    ┌─────────────────────────┐
                    │ urbanflow-batch         │
                    │ Bronze → Silver         │
                    │ Historical Gold         │
                    └────────────┬────────────┘
                                 │
                     ┌───────────┴───────────┐
                     │                       │
                     ▼                       ▼
                Silver trips          Historical Gold
                     │                       │
                     ▼                       │
              ┌─────────────────┐            │
              │ urbanflow-replay│            │
              │ Silver → Kafka  │            │
              └────────┬────────┘            │
                       │                     │
                       ▼                     │
                     Kafka                   │
                       │                     │
                       └──────────┬──────────┘
                                  ▼
                    ┌─────────────────────────┐
                    │ urbanflow-streaming     │
                    │ Structured Streaming    │
                    │ Historical comparison   │
                    └────────────┬────────────┘
                                 │
                                 ▼
                         Live Gold metrics
                                 │
                                 ▼
                     Streamlit / web analytics
```

### 1.5 Technology roles

| Technology | Main role |
|---|---|
| Apache Spark | Distributed batch processing, DataFrame/RDD/SQL demonstrations, Structured Streaming |
| Apache Kafka | Streaming event transport and replay |
| SeaweedFS | Self-hosted S3-compatible object storage |
| Hadoop S3A | Spark access to S3-compatible storage |
| Parquet | Primary persistent analytical format |
| Streamlit | Preferred lightweight analytics-serving layer |

### 1.6 Development organization and Foundation rule

UrbanFlow uses four codebases:

```text
urbanflow-foundation/
urbanflow-batch/
urbanflow-replay/
urbanflow-streaming/
```

`urbanflow-foundation` is the shared governance and contract layer. It defines schemas, time semantics, storage paths, configuration conventions, fixtures, ownership, and acceptance tests.

The central development rule is:

> **Share contracts, not application implementation.**

The three application subprojects should not import implementation code from one another. They integrate through **Parquet/S3A datasets and Kafka events**, with Foundation 1.0.0 defining the shared meanings and contracts.

During independent development, Foundation fixtures substitute for unfinished upstream subprojects. Final integration replaces fixtures with real upstream outputs.


---

## 2. Subproject Overview

### 2.1 What `urbanflow-replay` is

`urbanflow-replay` is the event-replay and Kafka-producer subsystem of UrbanFlow.

It converts held-out Silver trip records into a controlled stream of Foundation-compliant Kafka events so the project can demonstrate **Velocity** without requiring a real live taxi feed.

High level:

```text
Held-out Silver trips
        ↓
Replay reader
        ↓
Silver row → Kafka event
        ↓
Rate controller
        ↓
Kafka producer
        ↓
urbanflow.trip-events
```

### 2.2 Why this subproject exists

The project needs a streaming workload that:

- is related to the same mobility domain as the historical analytics;
- has known semantics;
- can be replayed repeatedly;
- can be accelerated to demonstrate high event rates;
- remains separate from the historical baseline period.

Replay therefore turns later/held-out Silver records into “live-style” traffic.

The stream is simulated, but the streaming behavior is real: Kafka receives actual events at controlled rates and downstream Structured Streaming processes them continuously.

### 2.3 Primary responsibilities

`urbanflow-replay` is responsible for:

- consuming canonical Silver records;
- selecting the fields required by the Kafka contract;
- generating a unique `event_id`;
- serializing strict JSON events;
- preserving pickup time as `event_time` with an explicit UTC offset;
- using `pickup_zone_id` as the Kafka message key;
- publishing to `urbanflow.trip-events`;
- supporting configurable replay rates;
- measuring requested versus achieved rate;
- exposing useful producer metrics;
- passing Foundation event-contract tests.

It is not responsible for cleaning raw taxi source data, building the historical baseline, Structured Streaming aggregation, live Gold, or dashboarding.

---

## 3. Data This Subproject Works With

### 3.1 Independent-development input

Foundation fixture:

```text
urbanflow-foundation/fixtures/silver/silver_trips.jsonl
```

This allows Replay development before the Batch subsystem is finished.

### 3.2 Integrated input

Canonical Silver dataset produced by Batch:

```text
s3a://urbanflow/silver/trips/
```

Physical integrated format: Parquet.

The Replay implementation only needs fields required to construct the strict event contract; it should not depend on undocumented Batch-specific columns.

### 3.3 Output Kafka event

Foundation 1.0.0 requires exactly seven fields:

```json
{
  "event_id": "trip-...",
  "event_time": "2025-01-06T08:21:44-05:00",
  "pickup_zone_id": 161,
  "dropoff_zone_id": 236,
  "trip_distance": 3.5,
  "trip_duration_minutes": 17.4,
  "fare_amount": 21.50
}
```

Required semantics:

- `event_id`: non-empty and unique;
- `event_time`: pickup time represented as RFC 3339 with an explicit UTC offset;
- `pickup_zone_id`, `dropoff_zone_id`: positive integers;
- distance, duration, and fare: numeric and non-negative;
- additional properties are not allowed.

### 3.4 Kafka topic contract

```text
topic:              urbanflow.trip-events
partitions:         4
replication factor: 1
message key:        decimal string form of pickup_zone_id
```

The key creates stable zone-based partitioning.

The consumer may rely on ordering within a Kafka partition but must not assume a global ordering across all partitions.

---

## 4. High-Level Data Flow

```text
             SILVER INPUT
                 │
                 ▼
         Silver source adapter
                 │
                 ▼
        Contract field projection
                 │
                 ▼
         Event-ID generation
                 │
                 ▼
       Time serialization / offset
                 │
                 ▼
        Strict JSON serialization
                 │
                 ▼
           Rate controller
                 │
                 ▼
          Kafka producer
                 │
                 ▼
       urbanflow.trip-events
                 │
                 ▼
      downstream Streaming subsystem
```

---

## 5. Functionalities and Features

### 5.1 Silver input support

The producer should support two logical modes:

#### Fixture mode

Read the Foundation Silver JSONL fixture for independent tests.

#### Integrated mode

Read the real Batch-produced Silver Parquet dataset for final integration.

The exact Parquet-reading library is an internal implementation choice and should be selected from the locked environment. It must not change the Silver or Kafka contracts.

### 5.2 Event projection

Replay should behave mainly as a **projection and serialization layer**, not a second ETL system.

From Silver it needs:

```text
pickup_datetime        → event_time
pickup_zone_id
dropoff_zone_id
trip_distance
trip_duration_minutes
fare_amount
```

plus a generated unique `event_id`.

It should not re-derive business semantics that belong to Batch unless required for contract-preserving serialization.

### 5.3 Event ID generation

Foundation requires a unique, non-empty string but does not prescribe the algorithm.

A proposed implementation should generate deterministic or traceable IDs where practical, for example from:

```text
pickup timestamp
pickup zone
sequence number
```

The exact representation is internal so long as uniqueness and non-empty validation are guaranteed.

### 5.4 Event-time handling

This is a critical feature.

Rules:

- `event_time` represents the trip’s pickup time;
- serialize as RFC 3339;
- include an explicit UTC offset;
- do not infer meaning from the producer machine’s local timezone;
- preserve the project’s canonical analytics interpretation: `America/New_York`.

Time handling should have explicit automated tests.

### 5.5 Strict event validation before send

Before publishing, validate:

```text
exact field set
required values present
positive zone IDs
non-negative numeric values
event_id uniqueness/non-blank
event_time parses with explicit offset
```

Invalid records should fail clearly or be counted/reported. They should not silently produce malformed Kafka traffic.

### 5.6 Kafka message key

Set the message key to:

```text
str(pickup_zone_id)
```

This is a contract-level behavior, not an optional producer tuning.

### 5.7 Configurable replay rate

Minimum planned demo rates:

```text
100 events/sec
500 events/sec
1,000 events/sec
2,500 events/sec
5,000 events/sec
```

Higher rates can be tested if hardware supports them.

The purpose is not to claim an arbitrary peak throughput. The purpose is to provide controlled input for a sustainable Kafka + Spark streaming experiment.

### 5.8 Rate control behavior

A practical rate controller should:

- accept a target events-per-second value;
- pace sends across time rather than emitting an uncontrolled burst;
- report requested and achieved rates;
- work for both low and high demo rates;
- avoid changing event-time semantics merely to achieve speed.

The replay clock and the event timestamp are separate concerns: the producer can replay historical event timestamps faster than wall clock while keeping those event timestamps intact.

### 5.9 Producer-side measurements

Expose at least:

```text
records read
records serialized
records rejected
records sent
requested events/sec
achieved events/sec
send duration
send errors
```

Optional useful measurements include delivery callback counts or latency summaries if the selected Kafka client exposes them cleanly.

### 5.10 Replay controls

Proposed CLI behavior:

```text
python replay_trips.py --rate 100
python replay_trips.py --rate 1000
python replay_trips.py --rate 5000
```

Useful additional options may include:

```text
--input <fixture-or-integrated-source>
--max-events <n>
--bootstrap-servers <...>
--topic <...>
--dry-run
```

Shared topic semantics must remain Foundation-compliant.

---

## 6. Proposed Internal Architecture

### 6.1 Proposed codebase structure

```text
urbanflow-replay/
│
├── README.md
├── FOUNDATION_VERSION
│
├── config/
│   ├── settings.yaml
│   └── settings.example.yaml
│
├── producer/
│   ├── replay_trips.py
│   ├── input_reader.py
│   ├── event_mapper.py
│   ├── event_validator.py
│   ├── rate_controller.py
│   └── producer_metrics.py
│
├── tests/
│   ├── test_fixture_reader.py
│   ├── test_event_contract.py
│   ├── test_event_time.py
│   ├── test_message_key.py
│   └── test_rate_controller.py
│
├── runbooks/
│   ├── run_fixture_replay.md
│   ├── run_kafka_smoke_test.md
│   └── run_velocity_experiment.md
│
└── docs/
    ├── architecture.md
    └── results/
```

The main proposal freezes only the basic codebase boundary. The extra modules above are a **proposed internal design** to keep source reading, mapping, contract validation, pacing, and metrics independently testable.

### 6.2 Component responsibilities

#### `input_reader.py`

Provides a consistent iterator/stream of Silver logical records from:

- Foundation JSONL fixture; or
- integrated Silver Parquet.

#### `event_mapper.py`

Maps one Silver record to the seven-field Kafka event.

#### `event_validator.py`

Applies the Foundation event checks before send.

#### `rate_controller.py`

Paces sends according to the requested rate.

#### `replay_trips.py`

Coordinates reading, mapping, validation, pacing, Kafka sending, and metrics.

#### `producer_metrics.py`

Tracks and prints/saves run-level replay evidence.

---

## 7. Kafka Producer Design

### 7.1 Producer contract

For each valid Silver row:

```text
Silver record
    ↓
project required fields
    ↓
strict JSON object
    ↓
key = pickup_zone_id as string
    ↓
Kafka send
```

### 7.2 Partitioning semantics

Because the key is based on pickup zone, events for the same zone should be consistently partitioned according to the Kafka client’s partitioner behavior.

The project demonstrates partitions and producer/consumer behavior, but does not require production-level Kafka HA.

### 7.3 Delivery behavior

The producer should expose send failures clearly and flush outstanding records before clean shutdown.

Exact Kafka-client acknowledgement and retry settings should be recorded in the local environment/configuration if they materially affect test results.

### 7.4 Graceful shutdown

On Ctrl+C or normal completion:

- stop reading new records;
- flush outstanding Kafka sends;
- print final metrics;
- return a meaningful exit status.

---

## 8. Development Guide

### Phase 0 — Preconditions

Before substantive work:

- Foundation 1.0.0 is available;
- Foundation validator passes;
- Python 3.13 works;
- Kafka broker works;
- producer/consumer smoke test works;
- Foundation Silver fixture is readable.

This is the `GO-B` condition.

### Phase 1 — Fixture reader

Read the Foundation Silver fixture and verify every row exposes the fields needed for event construction.

### Phase 2 — Event mapper and validator

Implement one-row transformation:

```text
Silver logical record
      ↓
Foundation Kafka event
```

Test exact field set, types, values, and event-time formatting.

### Phase 3 — Kafka connectivity

Publish a small number of fixture-derived events to the test/integration topic.

Consume them with a generic consumer and compare them to expected serialized values.

### Phase 4 — Rate controller

Implement rate-controlled replay.

Test progressively:

```text
100
500
1,000
2,500
5,000 events/sec
```

Measure requested versus achieved rate.

### Phase 5 — Integrated Silver reader

Switch from fixture input to real Batch-produced Silver Parquet without changing the event mapper or event contract.

This is an important architectural check: the input source adapter changes, while the event contract does not.

### Phase 6 — Contract and failure tests

Add tests for:

- extra event fields rejected;
- missing required fields rejected;
- invalid zone IDs rejected;
- negative numeric values rejected;
- missing timezone offset rejected;
- duplicate/non-empty event-ID rules;
- message key correctness;
- producer shutdown behavior.

### Phase 7 — Velocity experiment support

Prepare reproducible run commands and metrics output for the final Kafka + Streaming scale tests.

---

## 9. Testing and Acceptance Criteria

`urbanflow-replay` is independently acceptable when:

```text
[ ] GO-B prerequisites pass
[ ] Foundation version is recorded
[ ] Foundation Silver fixture is readable
[ ] every valid row maps to exactly seven Kafka fields
[ ] event_id is unique and non-empty
[ ] event_time is RFC 3339 with explicit offset
[ ] numeric constraints are enforced
[ ] Kafka key is str(pickup_zone_id)
[ ] events publish to urbanflow.trip-events
[ ] generic consumer receives valid events
[ ] rate control works at the planned demo rates
[ ] requested vs achieved rate is measurable
[ ] malformed events are not silently published
```

Integration acceptance additionally requires:

```text
A-produced Silver Parquet → Replay → valid Kafka events
```

without an integration-only schema adapter.

---

## 10. Demo Outputs

The Replay subsystem should visibly demonstrate:

- source Silver records;
- the seven-field Kafka JSON message;
- topic name and partition count;
- pickup-zone message key;
- requested replay rate;
- achieved replay rate;
- sent record count;
- Kafka consumer receiving the events.

This is the main UrbanFlow **Velocity producer** demonstration.

---

## 11. Non-Goals

This subproject should not become:

- a replacement for Batch ETL;
- a schema-registry project;
- a Kafka HA/operations project;
- a streaming analytics engine;
- a live Gold writer;
- a dashboard;
- a general-purpose event simulator unrelated to UrbanFlow data.

---

## 12. Definition of Done

`urbanflow-replay` is complete when it can independently read Foundation-compliant Silver data, convert each qualifying record into the strict Foundation Kafka event contract, publish those events to the canonical topic at controlled measurable rates, and integrate with Streaming without requiring contract-changing adapters.
