# UrbanFlow — `urbanflow-streaming`
## Subproject Overview, Functional Specification, Architecture, and Development Guide

**Date:** 2026-09-06  
**Subproject:** C — Live Processing and Analytics Serving  
**Foundation baseline:** `urbanflow-foundation` 1.0.0  
**Primary technologies:** Apache Spark Structured Streaming, Kafka, SeaweedFS/S3A, Parquet, Streamlit  
**Primary pipeline:** Kafka + Historical Gold → Live Gold → Analytics Application  
**Document purpose:** Self-contained implementation and development guide for the `urbanflow-streaming` subproject.

> **Authority note:** The Kafka event contract, historical Gold contract, live Gold contract, time semantics, storage paths, and null/join rules are governed by `urbanflow-foundation` 1.0.0. This document proposes internal implementation organization without changing those contracts.


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

`urbanflow-streaming` consumes the Kafka trip events (from `urbanflow-replay`)
and the historical Gold baseline (from `urbanflow-batch`), both derived from
these sources; the taxi-zone lookup/GeoJSON is useful for labelling zones in the
analytics app.

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

### 2.1 What `urbanflow-streaming` is

`urbanflow-streaming` is the live-processing and analytics-serving subsystem of UrbanFlow.

It consumes Kafka trip events continuously, aggregates current activity into five-minute event-time windows, compares those current counts with the historical baseline produced by Batch, writes Live Gold metrics, and serves the resulting Gold data through a lightweight analytics application.

High level:

```text
Kafka trip events
      +
Historical Gold baseline
      ↓
Spark Structured Streaming
      ↓
five-minute zone windows
      ↓
historical comparison
      ↓
Live Gold metrics
      ↓
Streamlit / web analytics app
```

### 2.2 Why this subproject exists

This subsystem closes the UrbanFlow end-to-end story.

Batch answers:

> **What normally happened?**

Replay provides:

> **What is arriving now?**

Streaming combines them to answer:

> **How does current activity compare with historical behavior?**

It also implements the final user-facing **Analytics** stage of the conceptual pipeline.

### 2.3 Primary responsibilities

`urbanflow-streaming` is responsible for:

- consuming Foundation-compliant Kafka events;
- declaring and enforcing the expected event schema;
- parsing event time correctly;
- applying five-minute tumbling event-time windows;
- aggregating by pickup zone;
- deriving historical lookup keys using New York time semantics;
- left-joining current windows to Historical Gold;
- calculating demand ratio;
- calculating nullable z-score;
- preserving windows without historical matches;
- writing Live Gold Parquet;
- writing streaming checkpoints separately;
- exposing streaming processing metrics where practical;
- serving Historical Gold and Live Gold through a thin analytics application;
- validating outputs against Foundation fixtures.

It is not responsible for creating Bronze/Silver, building the historical baseline, or producing the Kafka traffic.

---

## 3. Data This Subproject Works With

### 3.1 Independent-development event input

Foundation fixture:

```text
urbanflow-foundation/fixtures/kafka/trip_events.jsonl
```

This allows logic development before a real Replay producer exists.

### 3.2 Integrated streaming input

Kafka topic:

```text
urbanflow.trip-events
```

Foundation event fields:

```text
event_id
event_time
pickup_zone_id
dropoff_zone_id
trip_distance
trip_duration_minutes
fare_amount
```

The Spark consumer must declare this schema explicitly.

### 3.3 Historical baseline input

Independent fixture:

```text
urbanflow-foundation/fixtures/gold/historical_zone_baseline.jsonl
```

Integrated path:

```text
s3a://urbanflow/gold/historical_zone_baseline/
```

Lookup key:

```text
pickup_zone_id
day_of_week
hour
five_minute_slot
```

### 3.4 Live Gold output

Integrated path:

```text
s3a://urbanflow/gold/live_zone_metrics/
```

Fields:

```text
window_start
window_end
pickup_zone_id
current_trip_count
historical_mean_trip_count
demand_ratio
z_score
```

### 3.5 Checkpoint path

```text
s3a://urbanflow/checkpoints/live_trip_metrics/
```

Checkpoint data must remain separate from Gold analytics data.

---

## 4. High-Level Data Flow

```text
                   Kafka
                     │
                     ▼
          Spark Structured Streaming
                     │
             explicit JSON schema
                     │
                     ▼
              event_time parsing
                     │
                     ▼
             5-minute windows
                     │
                     ▼
           pickup-zone aggregation
                     │
                     ▼
       derive historical lookup key
                     │
                     │
 Historical Gold ────┘
       │
       ▼
 left join from current window
       │
       ├── demand_ratio
       └── z_score
       │
       ▼
   Live Gold Parquet
       │
       ▼
 Streamlit / web analytics app
```

---

## 5. Functionalities and Features

### 5.1 Kafka ingestion

The Structured Streaming job should:

- connect to the canonical topic;
- read Kafka value as JSON;
- declare the expected event schema explicitly;
- parse required fields;
- reject or quarantine malformed messages rather than silently expanding/changing the schema;
- make invalid-message counts visible where practical.

No Schema Registry is required within project scope.

### 5.2 Event-time semantics

`event_time` represents pickup time.

The consumer must:

- parse the explicit RFC 3339 offset;
- use `America/New_York` as the canonical analytics timezone;
- derive analytical time keys consistently with Batch;
- never infer event meaning from the host machine timezone.

This is essential because Batch and Streaming must construct the same historical lookup key from different data paths.

### 5.3 Five-minute tumbling windows

Use event-time windows such as:

```text
08:00–08:05
08:05–08:10
08:10–08:15
08:15–08:20
```

Aggregate by:

```text
window
pickup_zone_id
```

The Foundation Live Gold contract requires `current_trip_count`. Additional current-window metrics such as average distance/duration/fare may be calculated for educational inspection if they do not alter the shared contract output.

### 5.4 Historical lookup-key derivation

From `window_start`, derive:

```text
day_of_week
hour
five_minute_slot
```

using the same New York semantics as Batch.

Join key:

```text
pickup_zone_id
day_of_week
hour
five_minute_slot
```

### 5.5 Historical left join

The join is contractually a **left join from the current streaming window**.

Therefore:

- every current observation is preserved;
- missing historical baseline values produce nullable comparison metrics;
- the streaming job must not drop current windows simply because historical data is absent.

### 5.6 Demand ratio

```text
demand_ratio =
current_trip_count / historical_mean_trip_count
```

If historical mean is missing or `<= 0`:

```text
demand_ratio = null
```

Interpretation examples:

```text
1.0 ≈ historical level
1.5 ≈ 50% higher
2.0 ≈ approximately double
```

### 5.7 z-score

```text
z_score =
(current_trip_count - historical_mean_trip_count)
-------------------------------------------------
historical_std_trip_count
```

If historical standard deviation is missing or `<= 0`:

```text
z_score = null
```

The demo may choose not to emphasize z-score, but the Live Gold contract remains stable.

### 5.8 Live Gold persistence

Write contract-compliant Parquet to:

```text
s3a://urbanflow/gold/live_zone_metrics/
```

The exact write mode and partitioning strategy are internal implementation decisions and should be chosen to avoid corrupting/repeatedly duplicating output during demo runs.

### 5.9 Checkpointing

Structured Streaming checkpoints go to:

```text
s3a://urbanflow/checkpoints/live_trip_metrics/
```

They must never be mixed into the analytical Gold dataset.

### 5.10 Streaming measurements

Expose where practical:

```text
Spark input rows/sec
Spark processed rows/sec
micro-batch duration
processing latency
query status/progress
```

Consumer lag may be measured through Kafka-side tooling or another lightweight mechanism in the final Velocity experiment.

### 5.11 Analytics application

The Streamlit/web application is a **thin consumer of Gold outputs**.

It must not duplicate the ETL or stream-processing logic.

Inputs:

```text
Historical Gold
+
Live Gold
```

Minimum capabilities:

- current activity by zone;
- historical expected activity by zone;
- current-vs-historical comparison;
- demand ratio;
- top unusual/high-demand zones;
- zone filtering;
- time filtering;
- simple tables;
- simple charts;
- practical streaming/processing status;
- optional geographic view.

The app should be runnable against Foundation Gold fixtures before integrated Gold data exists.

---

## 6. Proposed Internal Architecture

### 6.1 Proposed codebase structure

```text
urbanflow-streaming/
│
├── README.md
├── FOUNDATION_VERSION
│
├── config/
│   ├── settings.yaml
│   └── settings.example.yaml
│
├── spark/
│   ├── live_trip_metrics.py
│   ├── event_schema.py
│   ├── historical_lookup.py
│   └── streaming_metrics.py
│
├── analytics_app/
│   ├── app.py
│   ├── data_access.py
│   └── views.py
│
├── tests/
│   ├── test_event_schema.py
│   ├── test_time_semantics.py
│   ├── test_window_aggregation.py
│   ├── test_historical_join.py
│   ├── test_live_metrics_contract.py
│   └── test_analytics_fixture_read.py
│
├── runbooks/
│   ├── run_fixture_streaming_test.md
│   ├── run_integrated_stream.md
│   └── run_analytics_app.md
│
└── docs/
    ├── architecture.md
    ├── streaming-semantics.md
    └── results/
```

The additional internal modules are a **proposed design**. The contract-level behavior remains Foundation-defined.

### 6.2 Component responsibilities

#### `event_schema.py`

Defines the exact Spark schema corresponding to the Foundation Kafka JSON contract.

#### `historical_lookup.py`

Loads Historical Gold and exposes the lookup keys/columns needed by the stream transformation.

#### `live_trip_metrics.py`

Orchestrates:

```text
Kafka read
→ parse
→ validate
→ event-time window
→ aggregate
→ historical join
→ derived metrics
→ Gold write
```

#### `streaming_metrics.py`

Extracts and reports query-progress measurements for the Velocity demo.

#### `analytics_app/data_access.py`

Reads Gold datasets without modifying them.

#### `analytics_app/app.py`

Renders the user-facing analytical experience.

---

## 7. Structured Streaming Design

### 7.1 Logical transformation

```text
Kafka key/value
      ↓
value JSON string
      ↓
from_json(explicit_schema)
      ↓
valid event rows
      ↓
event-time timestamp
      ↓
window(event_time, "5 minutes")
      ↓
groupBy(window, pickup_zone_id)
      ↓
count
      ↓
derive weekday/hour/slot from window_start
      ↓
left join Historical Gold
      ↓
demand_ratio + z_score
      ↓
Live Gold projection
```

### 7.2 Validation path

A robust design should logically separate:

```text
valid events   → main streaming pipeline
invalid events → counted/logged/quarantined
```

The exact quarantine mechanism is an internal implementation choice, but malformed inputs should not silently become valid rows.

### 7.3 Current metric grain

One Live Gold row represents:

```text
one pickup zone
within one five-minute window
```

plus the historical comparison values for the same zone/time key.

### 7.4 Historical comparison semantics

For each current window:

```text
window_start
    ↓ New York time semantics
day_of_week + hour + five_minute_slot
    ↓
+ pickup_zone_id
    ↓
historical baseline key
```

This cross-subproject semantic alignment is one of the most important Foundation guarantees.

---

## 8. Analytics Application Design

### 8.1 Architectural rule

The application consumes Gold; it does not rebuild it.

```text
Gold historical
      +
Gold live
      ↓
Data-access layer
      ↓
presentation / filtering
      ↓
dashboard
```

### 8.2 Proposed primary views

#### Overview

Show:

```text
streaming status
latest window
total current trips
zones observed
top demand ratios
```

#### Current vs historical

Table/chart with:

```text
zone
current_trip_count
historical_mean_trip_count
demand_ratio
z_score
```

#### Zone explorer

Filter by pickup zone and optionally time/window.

Show historical expectation versus current values.

#### Top unusual zones

Sort valid demand ratios descending and clearly distinguish rows whose historical comparison is unavailable.

#### Optional map

If zone geometry is available and core work is stable, add a simple geographic view. This remains optional.

### 8.3 Data refresh

The app should refresh Gold-derived data in a lightweight, controlled way suitable for a local demo.

It should not depend on internal state inside the Structured Streaming process.

This keeps the analytics app independently testable.

---

## 9. Development Guide

### Phase 0 — Preconditions

Before substantive work:

- Foundation 1.0.0 is available;
- Foundation validator passes;
- Spark works;
- Kafka test topic is reachable;
- Spark ↔ Kafka smoke test works;
- Foundation Kafka fixture is available;
- Foundation historical-baseline fixture is available.

This is the `GO-C` condition.

### Phase 1 — Offline fixture transformation

Before continuous Kafka integration, use the Foundation event and baseline fixtures to prove:

```text
event parsing
time derivation
5-minute aggregation
historical join
demand ratio
z-score
```

Compare against the expected Foundation live fixture.

### Phase 2 — Kafka Structured Streaming

Connect the same transformation logic to Kafka.

Confirm explicit schema parsing and correct event-time values.

### Phase 3 — Historical Gold load

Load the historical baseline from fixture first, then integrated Parquet.

Ensure the real Batch output requires no special integration-only casts or renames.

### Phase 4 — Live Gold write

Write live metrics to the canonical Gold path.

Verify:

- schema;
- null rules;
- left-join preservation;
- checkpoint separation.

### Phase 5 — Analytics app against fixtures

Implement the web app using Foundation Historical Gold + expected Live Gold fixtures.

This allows UI development to proceed even before the real stream is stable.

### Phase 6 — Analytics app against integrated Gold

Replace fixture paths with the real Gold paths through configuration only.

No ETL logic should be added to the app to make integration work.

### Phase 7 — Velocity measurements

Run the integrated Replay → Kafka → Structured Streaming path at planned rates.

Record:

```text
requested producer rate
achieved producer rate
Spark input rows/sec
Spark processed rows/sec
micro-batch duration
consumer lag
processing latency
```

The objective is to identify sustainable behavior, not to advertise an arbitrary maximum.

---

## 10. Foundation Reference Test Scenario

The supplied Foundation fixture scenario provides a deterministic cross-contract test.

For the fixture five-minute window:

```text
Zone 161:
current count = 5
historical mean = 2
demand ratio = 2.5
z-score = 3.0

Zone 236:
current count = 2
historical mean = 1
demand ratio = 2.0
z-score = 1.0
```

The Streaming implementation should reproduce these values exactly under the Foundation rules.

This is the strongest independent proof that:

```text
event schema
+
time semantics
+
window aggregation
+
historical baseline
+
derived metric formulas
```

compose correctly.

---

## 11. Testing and Acceptance Criteria

`urbanflow-streaming` is independently acceptable when:

```text
[ ] GO-C prerequisites pass
[ ] Foundation version is recorded
[ ] Kafka event schema is explicit
[ ] malformed events are rejected/quarantined visibly
[ ] event_time semantics are correct
[ ] five-minute event-time windows work
[ ] current counts by pickup zone are correct
[ ] historical lookup key uses New York semantics
[ ] left join preserves unmatched current windows
[ ] demand-ratio null rule works
[ ] z-score null rule works
[ ] Foundation expected live fixture matches exactly
[ ] Live Gold writes to canonical path
[ ] checkpoints use the separate checkpoint path
[ ] analytics app reads Historical Gold
[ ] analytics app reads Live Gold
[ ] current-vs-historical view works
[ ] zone/time filtering works
[ ] basic charts/tables work
```

Integration acceptance additionally requires:

```text
B real Kafka events → C without schema adapter
A real Historical Gold → C without key/type adapter
C real Live Gold → analytics app without integration-only transformation
```

---

## 12. Demo Outputs

The Streaming subsystem should visibly demonstrate:

- Kafka events entering Spark;
- five-minute event-time windowing;
- current trip counts by zone;
- historical expected counts;
- demand ratio;
- optional z-score;
- Spark processing-rate information;
- written Live Gold rows;
- dashboard/table/chart views;
- filters and top unusual zones.

This is where the audience sees the complete question answered:

> **What is happening now compared with what normally happens?**

---

## 13. Non-Goals

This subproject should not become:

- a replacement for Batch ETL;
- the Kafka event producer;
- a full frontend application;
- a general-purpose BI platform;
- a geospatial platform;
- a machine-learning anomaly detector;
- a complex stream-processing laboratory with late-event/duplicate experiments that delay the core demo.

---

## 14. Definition of Done

`urbanflow-streaming` is complete when it can independently consume Foundation-compliant events and historical baselines, reproduce the Foundation expected live metrics, run continuously through Spark Structured Streaming, persist Foundation-compliant Live Gold with separate checkpoints, and expose Historical/Live Gold through a thin usable analytics application.
