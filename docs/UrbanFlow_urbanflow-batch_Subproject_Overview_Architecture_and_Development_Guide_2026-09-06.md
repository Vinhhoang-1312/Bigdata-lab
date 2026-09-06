# UrbanFlow — `urbanflow-batch`
## Subproject Overview, Functional Specification, Architecture, and Development Guide

**Date:** 2026-09-06  
**Subproject:** A — Historical Batch Analytics  
**Foundation baseline:** `urbanflow-foundation` 1.0.0  
**Primary technologies:** Apache Spark, SeaweedFS/S3A, Parquet  
**Primary pipeline:** Source Data → Bronze → Silver → Historical Gold  
**Document purpose:** Self-contained implementation and development guide for the `urbanflow-batch` subproject.

> **Authority note:** Cross-subproject schemas, paths, time semantics, producer/consumer ownership, and contract rules are governed by `urbanflow-foundation` 1.0.0. This document proposes implementation structure inside `urbanflow-batch`; it does not authorize changes to shared contracts.


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

The `tpep_*` field prefix in the source schema denotes Yellow-taxi (TPEP)
records. Green-taxi (`lpep_*`) files are available from the same landing page if
a second trip type is wanted for the Variety demonstration.

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

### 2.1 What `urbanflow-batch` is

`urbanflow-batch` is the historical-data engineering and batch-analytics subsystem of UrbanFlow.

Its main responsibility is to transform public urban-mobility source data into trustworthy, reusable, analytics-ready datasets.

At a high level:

```text
Public source data
      ↓
Batch acquisition / ingestion
      ↓
Bronze
      ↓
Spark cleaning and standardization
      ↓
Silver
      ↓
Historical aggregation
      ↓
Historical Gold baseline
```

It also provides the project’s focused demonstration of the three classic Spark interfaces:

```text
RDD
DataFrame
Spark SQL
```

using at least one equivalent analytical question so the audience can see how the abstractions relate.

### 2.2 Why this subproject exists

The Batch subsystem provides the **historical half** of the UrbanFlow analytical story.

It establishes what “normal” activity looks like before the replay/streaming path asks whether current activity is unusual.

It also produces the two shared datasets consumed by downstream subprojects:

1. **Silver trips** → consumed by `urbanflow-replay`.
2. **Historical zone baseline** → consumed by `urbanflow-streaming`.

### 2.3 Primary responsibilities

`urbanflow-batch` is responsible for:

- acquiring or organizing source NYC taxi data;
- maintaining the Bronze source-oriented layer;
- reading Bronze data with Spark;
- validating, cleaning, standardizing, and enriching trip records;
- deriving canonical time fields;
- writing canonical Silver Parquet;
- demonstrating RDD/DataFrame/Spark SQL equivalence;
- constructing the historical zone/time baseline;
- writing Historical Gold Parquet;
- reporting useful batch/Volume evidence;
- passing Foundation contract tests for its outputs.

It is **not** responsible for Kafka replay, live streaming, live Gold metrics, or the web dashboard.

---

## 3. Data This Subproject Works With

### 3.1 Inputs

#### A. NYC taxi-trip records

Primary transactional source data, expected in Parquet.

Source: **NYC TLC Trip Record Data** — landing page
<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>. Yellow-taxi
monthly files follow the pattern
`https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
(e.g. `yellow_tripdata_2023-01.parquet`). Field definitions: yellow trips data
dictionary
<https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf>.

Relevant source-style fields include:

```text
tpep_pickup_datetime
tpep_dropoff_datetime
PULocationID
DOLocationID
trip_distance
passenger_count
fare_amount
total_amount
```

The concrete source schema must be inspected before final cleaning rules are frozen.

#### B. Taxi-zone lookup

Small reference dataset, typically CSV.

Source: <https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv>
(maps `LocationID` to borough, zone name, and service zone).

Used to validate zone identifiers and optionally enrich output with zone metadata for analytics.

#### C. Taxi-zone geometry

Optional geographic reference data, typically GeoJSON or geographic Parquet.

Source: TLC taxi-zone shapefile
<https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip>, or the same
boundaries as GeoJSON from NYC Open Data
<https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON>.

This supports the project’s Variety demonstration and possible later visualization, but advanced geographic work must not block the core Batch pipeline.

#### D. Foundation Bronze fixture

During independent development, the subproject can use the Foundation Bronze fixture instead of waiting for a large real dataset.

### 3.2 Silver output contract

The canonical integrated Silver format is:

```text
Parquet
s3a://urbanflow/silver/trips/
partitioned by year and month
```

Canonical logical fields:

```text
pickup_datetime
dropoff_datetime
pickup_zone_id
dropoff_zone_id
trip_distance
passenger_count          (nullable)
fare_amount
total_amount             (nullable)
trip_duration_minutes
year
month
day_of_week
hour
five_minute_slot
```

Important semantics include:

- analytics timezone: `America/New_York`;
- `day_of_week`: ISO convention, Monday = 1 through Sunday = 7;
- `five_minute_slot = floor(minute / 5)`, yielding 0–11;
- non-null pickup/dropoff timestamps;
- `dropoff_datetime >= pickup_datetime`;
- non-negative distance, duration, and fare;
- zone IDs must be semantically valid against the taxi-zone lookup.

Additional cleaning rules should be added only when inspection of real data justifies them.

### 3.3 Historical Gold output contract

Integrated path:

```text
s3a://urbanflow/gold/historical_zone_baseline/
```

Grain:

```text
one row per
pickup_zone_id
+ day_of_week
+ hour
+ five_minute_slot
```

Metrics:

```text
historical_mean_trip_count
historical_std_trip_count
historical_mean_distance
historical_mean_duration
```

The count baseline must include **zero-count eligible zone/date/slot combinations** before calculating historical means and population standard deviations. This prevents the baseline from being biased upward by omitting quiet periods.

---

## 4. High-Level Data Flow

```text
          NYC TAXI / REFERENCE DATA
                    │
                    ▼
              Source Acquisition
                    │
                    ▼
             ┌──────────────┐
             │    Bronze    │
             │ source-like  │
             └──────┬───────┘
                    │
                    ▼
          Spark DataFrame ETL
        ┌───────────┼───────────┐
        │           │           │
        ▼           ▼           ▼
   validation    cleaning    derivations
        │           │           │
        └───────────┴───────────┘
                    │
                    ▼
             ┌──────────────┐
             │    Silver    │
             │ standardized │
             └──────┬───────┘
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
 RDD/DF/SQL demo      Historical aggregation
                              │
                              ▼
                     ┌───────────────────┐
                     │ Historical Gold   │
                     │ zone/time baseline│
                     └───────────────────┘
```

---

## 5. Functionalities and Features

### 5.1 Source acquisition and Bronze management

The subsystem should:

- acquire a manageable real NYC taxi subset first;
- preserve source-oriented files in Bronze;
- organize source files under the Foundation-defined storage layout;
- report source file count, row count, and approximate storage size;
- keep Bronze close to the received source representation.

Bronze answers:

> **What data did we receive?**

### 5.2 Bronze → Silver ETL

The core ETL should use Spark DataFrames.

Processing responsibilities include:

- selecting required source columns;
- renaming fields into canonical names;
- casting to canonical types;
- validating non-null required fields;
- validating pickup/dropoff ordering;
- validating non-negative numeric values;
- validating pickup/dropoff zone IDs;
- calculating trip duration;
- deriving `year`, `month`, `day_of_week`, `hour`, and `five_minute_slot`;
- recording accepted and rejected row counts;
- writing partitioned Silver Parquet.

Silver answers:

> **What trustworthy, standardized data can downstream systems use?**

### 5.3 Rejected/invalid-data visibility

A practical implementation should make invalid-data handling visible rather than silently discarding rows.

At minimum, the job should report:

```text
source_rows
valid_rows
rejected_rows
silver_rows
```

A lightweight rejected-record output may be added if useful, provided it does not change the Foundation Silver contract.

### 5.4 Spark API equivalence demonstration

Implement one analytical question using:

```text
RDD        → map / reduceByKey
DataFrame  → groupBy / count
Spark SQL  → GROUP BY / COUNT
```

Recommended question:

> **How many trips originate from each pickup zone?**

The three outputs should be normalized and compared for equivalence.

This is an educational feature; RDD should not replace DataFrame as the primary ETL API.

### 5.5 Historical baseline construction

The baseline builder should:

1. identify eligible historical calendar dates;
2. identify eligible pickup zones;
3. calculate trip counts per date/zone/hour/five-minute slot;
4. include zero-count eligible combinations;
5. group by zone + ISO weekday + hour + five-minute slot;
6. calculate arithmetic mean trip count;
7. calculate population standard deviation with Spark `stddev_pop`;
8. calculate mean trip distance and mean trip duration;
9. write the Historical Gold dataset.

### 5.6 Volume demonstration and measurements

After correctness is proven on a small subset, scale the same pipeline through progressively larger historical periods where hardware permits.

Measure:

```text
source files
record count
raw storage size
Silver storage size
Spark partitions
batch-processing duration
worker count
```

The objective is to demonstrate real distributed processing on a meaningful multi-month dataset, not to artificially duplicate a small dataset.

### 5.7 Configuration and environment profiles

The codebase should own its local configuration while preserving Foundation semantics.

Configuration should cover at least:

```text
Foundation version
Spark master
Spark session timezone
SeaweedFS/S3 endpoint
bucket
Bronze paths
Silver path
Historical Gold path
local-vs-integration profile
```

Credentials should be passed through environment variables or another local secret mechanism, not hard-coded.

---

## 6. Proposed Internal Architecture

### 6.1 Proposed codebase structure

```text
urbanflow-batch/
│
├── README.md
├── FOUNDATION_VERSION
│
├── config/
│   ├── settings.yaml
│   └── settings.example.yaml
│
├── spark/
│   ├── bronze_to_silver.py
│   ├── spark_api_demo.py
│   └── build_gold_baseline.py
│
├── batch/
│   ├── source_acquisition.py
│   ├── schemas.py
│   ├── validation.py
│   └── metrics.py
│
├── tests/
│   ├── test_bronze_to_silver_contract.py
│   ├── test_time_derivations.py
│   ├── test_spark_api_equivalence.py
│   └── test_historical_baseline_contract.py
│
├── runbooks/
│   ├── run_fixture_smoke_test.md
│   ├── run_real_subset.md
│   └── run_volume_experiment.md
│
└── docs/
    ├── architecture.md
    ├── data-cleaning-decisions.md
    └── results/
```

Only the high-level repository shape is frozen by the main proposal. The extra internal modules above are a **proposed implementation organization** intended to keep ingestion, validation, metrics, and Spark jobs separate.

### 6.2 Component responsibilities

#### `source_acquisition.py`

- obtain or copy source datasets;
- record source metadata;
- populate Bronze paths;
- avoid modifying source semantics unnecessarily.

#### `bronze_to_silver.py`

- create Spark session with `America/New_York`;
- read Bronze Parquet;
- normalize schema;
- apply validity rules;
- derive canonical fields;
- write partitioned Silver Parquet;
- emit row-count metrics.

#### `spark_api_demo.py`

- read the same Silver dataset;
- solve the same analytical task with RDD, DataFrame, and SQL;
- normalize outputs;
- assert result equivalence.

#### `build_gold_baseline.py`

- read historical Silver;
- construct complete eligible date/zone/time combinations;
- join actual counts onto the complete grid;
- fill missing counts with zero;
- aggregate to the historical baseline grain;
- calculate required metrics;
- write Historical Gold.

### 6.3 Dependency boundary

`urbanflow-batch` should not import code from Replay or Streaming.

Its outward interfaces are data contracts only:

```text
Silver Parquet
      ├────────────► urbanflow-replay

Historical Gold Parquet
      └────────────► urbanflow-streaming
```

---

## 7. Detailed Processing Design

### 7.1 Spark session requirements

At minimum:

```text
spark.sql.session.timeZone = America/New_York
```

The S3A configuration must be compatible with the locked Spark/Hadoop/SeaweedFS environment.

### 7.2 Bronze → Silver transformation order

Recommended order:

```text
Read source
  ↓
Select/rename columns
  ↓
Cast types
  ↓
Validate timestamps
  ↓
Validate zone IDs
  ↓
Validate numeric fields
  ↓
Derive trip_duration_minutes
  ↓
Derive time fields
  ↓
Final Silver projection
  ↓
Contract assertions
  ↓
Partitioned Parquet write
```

This ordering keeps contract-relevant transformations explicit and testable.

### 7.3 Historical baseline algorithm

Conceptual algorithm:

```text
Historical Silver
      │
      ├── derive eligible dates
      ├── obtain eligible zones
      └── enumerate hour/5-minute slots
                │
                ▼
     Complete eligible comparison grid
                │
                ▼
     Left join actual per-date counts
                │
                ▼
         missing count → 0
                │
                ▼
 group by zone + weekday + hour + slot
                │
        ┌───────┼────────┐
        ▼       ▼        ▼
      mean   stddev_pop  trip means
                │
                ▼
        Historical Gold
```

---

## 8. Development Guide

### Phase 0 — Preconditions

Before substantive work:

- Foundation 1.0.0 is available;
- Foundation fixture validator passes;
- Spark works;
- SeaweedFS S3 endpoint works;
- Spark ↔ S3A ↔ SeaweedFS Parquet round-trip works;
- Bronze fixture or small real source subset is readable.

This is the `GO-A` condition.

### Phase 1 — Fixture-first Bronze → Silver

Implement the ETL against the Foundation Bronze fixture first.

Verify:

- canonical field names;
- canonical types;
- time derivations;
- validity rules;
- deterministic row counts;
- no undocumented fields required by downstream consumers.

### Phase 2 — Real small-subset ETL

Acquire approximately 1–3 months or another manageable source subset.

Inspect:

- source schema;
- null patterns;
- timestamps;
- zone IDs;
- obvious invalid numeric values;
- approximate row count and storage size.

Only then add justified cleaning rules.

### Phase 3 — Silver write/read integration

Write Silver to:

```text
s3a://urbanflow/silver/trips/
```

Read it back with Spark and verify:

- schema;
- partition fields;
- row count;
- sampled values;
- downstream contract compatibility.

### Phase 4 — Spark API demonstration

Implement the chosen equivalent analytical task through RDD, DataFrame, and Spark SQL.

Assert equivalent results.

### Phase 5 — Historical Gold

Build the baseline with zero-count eligible combinations and `stddev_pop`.

Write:

```text
s3a://urbanflow/gold/historical_zone_baseline/
```

Validate grain uniqueness and schema.

### Phase 6 — Contract tests

Required tests should cover:

- Silver contract;
- time-field derivation;
- path/partition expectations;
- RDD/DataFrame/SQL equivalence;
- historical baseline contract;
- zero-count inclusion behavior;
- population standard deviation behavior.

### Phase 7 — Volume experiment

Scale through larger historical periods only after correctness is stable.

Record performance evidence for the final demo.

---

## 9. Testing and Acceptance Criteria

`urbanflow-batch` is independently acceptable when:

```text
[ ] GO-A prerequisites pass
[ ] Foundation version is recorded
[ ] Bronze/source data is readable
[ ] Bronze → Silver ETL works
[ ] Silver fields/types/time semantics match Foundation 1.0.0
[ ] invalid rows are measurable
[ ] Silver Parquet writes to the canonical path
[ ] RDD/DataFrame/SQL demo returns equivalent results
[ ] Historical Gold baseline is produced
[ ] zero-count eligible combinations are included
[ ] historical stddev uses population semantics
[ ] Historical Gold contract passes
[ ] outputs can be consumed without application-specific adapters
```

Integration acceptance additionally requires:

```text
A real Silver → Replay works
A real Historical Gold → Streaming works
```

without changing the shared contracts.

---

## 10. Operational and Demo Outputs

The Batch subsystem should make the following visible in the final demonstration:

- number of source files;
- number of source and Silver records;
- Bronze/Silver/Gold storage paths;
- sample Silver schema;
- Spark partition count;
- batch runtime;
- RDD/DataFrame/SQL equivalent result;
- sample Historical Gold rows;
- explanation of how the historical baseline represents “normal” activity.

---

## 11. Non-Goals

This subproject should not grow into:

- a streaming subsystem;
- Kafka producer logic;
- live demand-ratio processing;
- frontend/dashboard implementation;
- a full data warehouse platform;
- a lakehouse/catalog project;
- a machine-learning pipeline;
- advanced geospatial engineering that delays the core path.

---

## 12. Definition of Done

`urbanflow-batch` is complete when it can independently and repeatably transform valid UrbanFlow source data into Foundation-compliant Silver and Historical Gold outputs, demonstrate the selected Spark abstractions, and provide the historical analytical baseline required by the Replay and Streaming subprojects.
