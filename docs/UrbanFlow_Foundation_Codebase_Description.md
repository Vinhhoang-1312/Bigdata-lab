# UrbanFlow Shared Foundation — Codebase Description

**Foundation version:** 1.0.0
**Date:** September 2026
**Scope:** This document describes the current contents, design rationale, and internal relationships of the `urbanflow-foundation` codebase. It is intended for any developer who needs to understand what the Foundation contains, how its parts connect, and how to use it as the governance layer for the three UrbanFlow application subprojects.

---

## 1. What the Foundation Is

The UrbanFlow project is organized as four codebases: one shared Foundation and three independently buildable application subprojects (Batch, Replay, and Streaming). The Foundation is the governance layer. It defines the stable agreements — schemas, path semantics, time conventions, configuration baselines, reference fixtures, and validation tooling — that allow the three application codebases to be developed on separate machines by separate developers without requiring live connectivity to one another.

The Foundation is emphatically *not* a shared application library. No subproject imports Python modules, utility classes, or runtime code from it. The independence rule is structural: subprojects share contracts, not implementation. This distinction keeps the three codebases loosely coupled and eliminates hidden transitive dependencies that would undermine the parallel-development model.

The Foundation currently contains 26 files across six top-level directories. Every file serves one of five roles: contract definition, configuration reference, governance documentation, reference fixture data, or automated validation. The following sections walk through each directory in turn.

---

## 2. Root-Level Files

Three files sit at the repository root.

`VERSION` contains the single string `1.0.0`. It is the machine-readable Foundation version. The validator script reads this file programmatically and confirms it matches the expected version constant, providing a simple integrity check that the correct Foundation release is in use. Subprojects record this version to declare which contract generation they implement against.

`README.md` is the entry point for any developer encountering the Foundation for the first time. It explains the Foundation's purpose, lists the authoritative contract files, states the independence rule, describes the fixture substitution model, defines the semantic versioning policy (patch for wording corrections, minor for backward-compatible additions, major for breaking changes), and points to the validator as the first validation step.

`CHANGELOG.md` records every change to the Foundation with the version that introduced it. At 1.0.0, it contains a single release entry listing all 26 files and their purposes. As the Foundation evolves through future versions, each change will be documented here with its semantic version bump and the rationale for the change.

---

## 3. Contracts Directory

The `contracts/` directory is the heart of the Foundation. It contains seven files that together define every cross-codebase interface in the system. No subproject may invent its own interpretation of a shared schema, storage path, or time convention — the contracts directory is the single source of truth.

### 3.1 Contract Manifest

`contract-manifest.yaml` is the master index. It lists all seven contracts with their authoritative file, physical format, producer, consumer(s), and the Foundation version at which each was frozen. A developer can read this one file to understand the full contract surface of the project: which subproject writes what, which subproject reads it, and where the detailed definition lives.

The manifest currently tracks five data contracts (Silver trips, Kafka trip event, Kafka topic configuration, historical zone baseline, and live zone metrics) and two semantic contracts (time semantics and storage paths).

### 3.2 Silver Trips Contract

`silver-trips.yaml` defines the canonical Silver trip dataset — the cleaned, standardized, and enriched version of raw taxi-trip data that the Batch subproject produces and the Replay subproject consumes.

The contract specifies fourteen fields. Ten are required and non-nullable: `pickup_datetime`, `dropoff_datetime`, `pickup_zone_id`, `dropoff_zone_id`, `trip_distance`, `fare_amount`, `trip_duration_minutes`, `year`, `month`, `day_of_week`, `hour`, and `five_minute_slot`. Two are optional: `passenger_count` and `total_amount`. The contract defines precise types (timestamp, integer, double), nullability, and value constraints — for example, `trip_distance >= 0`, `month` in the range 1–12, `day_of_week` using ISO weekday convention (Monday = 1, Sunday = 7), and `five_minute_slot` computed as `floor(minute / 5)`, yielding a range of 0–11.

The physical format is Parquet, partitioned by year and month, stored at `s3a://urbanflow/silver/trips/`. The contract also lists minimum validity rules that the Batch pipeline's cleaning logic must enforce, such as non-null timestamps, dropoff occurring at or after pickup, and non-negative distances and fares. The explicit statement that "additional data-cleaning rules may be introduced only when real-data inspection justifies them" prevents scope creep while permitting evidence-based evolution.

### 3.3 Kafka Trip Event Schema

`kafka-trip-event.schema.json` is a formal JSON Schema (Draft 2020-12) that defines the Kafka event message. It is the most precisely machine-enforceable contract in the Foundation — a compliant JSON Schema validator can verify any message against it directly.

The schema requires exactly seven fields: `event_id` (non-empty string), `event_time` (RFC 3339 date-time string with explicit UTC offset), `pickup_zone_id` and `dropoff_zone_id` (positive integers), and `trip_distance`, `trip_duration_minutes`, and `fare_amount` (non-negative numbers). The schema sets `additionalProperties: false`, meaning any message containing extra fields is non-compliant. This strictness is deliberate: in a contract-first system, the producer must not silently add fields that the consumer might accidentally depend on without a Foundation version bump.

The `event_time` field deserves special attention. It represents pickup time and must carry an explicit UTC offset (e.g., `-05:00` for Eastern Standard Time). Neither the producer nor the consumer may infer its meaning from the host machine's time zone. This rule is the Kafka-side expression of the project-wide time-semantics contract.

### 3.4 Kafka Topic Contract

`kafka-topic.yaml` defines the topic-level configuration: the topic name (`urbanflow.trip-events`), partition count (4), replication factor (1), and the message key convention. The message key is the decimal string form of `pickup_zone_id` (e.g., `"161"`), which provides stable zone-based partitioning. Consumers may rely on ordering within a single partition but must not assume global ordering across all four partitions.

The contract also documents that the project uses a single Kafka broker in KRaft mode and that no Schema Registry is required — contract versioning is handled entirely by the Foundation itself. This keeps the infrastructure footprint minimal while still providing formal schema governance.

### 3.5 Historical Zone Baseline Contract

`historical-zone-baseline.yaml` defines the Gold-layer historical mobility baseline that the Batch subproject produces and the Streaming subproject consumes for its real-time comparison.

The grain is one row per unique combination of `pickup_zone_id`, `day_of_week`, `hour`, and `five_minute_slot`. This means the baseline captures typical trip activity for each zone at each five-minute slot of each weekday — for example, "zone 161 on Mondays between 08:00 and 08:05 historically averages 2 trips with a standard deviation of 1."

The schema includes four key columns (`pickup_zone_id`, `day_of_week`, `hour`, `five_minute_slot`) and four metric columns (`historical_mean_trip_count`, `historical_std_trip_count`, `historical_mean_distance`, `historical_mean_duration`). The construction rules are explicit and non-trivial: per-date trip counts must include zero-count combinations for eligible zones and dates, so that the mean and standard deviation reflect reality rather than being upward-biased by silently omitting quiet periods. The standard deviation uses Spark's `stddev_pop` (population standard deviation), not the sample variant.

### 3.6 Live Zone Metrics Contract

`live-zone-metrics.yaml` defines the Gold-layer streaming output — the comparison of current activity against the historical baseline. The Streaming subproject produces this; the demo or an optional dashboard consumes it.

The schema has seven fields. Three are always present: `window_start`, `window_end` (the five-minute tumbling window boundaries), and `pickup_zone_id`. `current_trip_count` (long, non-nullable) is the number of trips observed in the current window. `historical_mean_trip_count`, `demand_ratio`, and `z_score` are all nullable doubles.

The contract precisely defines the null rules for the derived metrics. `demand_ratio` is `current_trip_count / historical_mean_trip_count`, but if the historical mean is null or less than or equal to zero, the ratio is null. `z_score` is `(current_trip_count - historical_mean_trip_count) / historical_std_trip_count`, but if the historical standard deviation is null or less than or equal to zero, the z-score is null. These rules prevent division-by-zero errors and ensure that the absence of historical data results in explicit nulls rather than misleading numbers.

The join semantics are also specified: the historical lookup is a left join from the current streaming window. This means a current window is preserved even if no historical baseline exists for that zone and time slot — the streaming subsystem never silently drops observations.

### 3.7 Time Semantics

`time-semantics.md` is a prose contract (Markdown rather than YAML) that establishes the project's canonical analytics time zone as `America/New_York`. All time-derived analytical fields, window boundaries, and day-of-week calculations use this zone. Spark sessions must set `spark.sql.session.timeZone` to `America/New_York`. Kafka event times must carry explicit UTC offsets and must not be interpreted using the host machine's local clock.

The document also defines the derived time fields (`year`, `month`, `day_of_week`, `hour`, `five_minute_slot`) with their exact computation rules and valid ranges, the five-minute tumbling window alignment, and the historical baseline join key derivation from `window_start`. This contract exists as prose because its content is primarily definitional — it establishes meanings rather than a schema — but it is just as authoritative as the YAML contracts.

### 3.8 Storage Paths

`storage-paths.yaml` defines the integrated S3A storage layout within the single `urbanflow` bucket. It maps every persistent path to its owning writer and, where applicable, its readers. For example, `s3a://urbanflow/silver/trips/` is written by `urbanflow-batch` and read by `urbanflow-replay`. `s3a://urbanflow/gold/live_zone_metrics/` is written by `urbanflow-streaming` and has no designated reader beyond the demo.

The single-writer rule is a structural invariant: each shared path has exactly one owning writer, and consumers must not rewrite another subsystem's outputs. The contract also enforces that streaming checkpoints (`s3a://urbanflow/checkpoints/live_trip_metrics/`) remain separate from Gold data, and that independent development may substitute local Foundation fixtures without changing the integrated path contract.

---

## 4. Config Directory

The `config/` directory contains two reference files that define shared configuration conventions without imposing a single mutable configuration file on all subprojects.

### 4.1 Config Reference

`config-reference.yaml` defines the canonical field names and baseline values for settings that span multiple subprojects: the analytics timezone, Kafka topic and partition settings, Spark session timezone, SeaweedFS bucket name, and all integrated storage paths. Values that vary by machine or environment (Kafka bootstrap servers, Spark master URL, SeaweedFS endpoint) are marked `<profile-specific>` — each subproject fills these in its own local `config/settings.yaml`.

The file also lists the frozen values that must not change without a Foundation version bump: the Kafka topic name, partition count, replication factor, analytics timezone, and Foundation version itself.

### 4.2 Data Periods Reference

`data-periods-reference.yaml` is a template for freezing the historical baseline period and the streaming replay period. The invariant — that the two periods must not overlap — preserves the analytical story: historical data represents "what normally happened before" and replay data represents "what is happening now." The exact dates are left as `<TO BE DETERMINED>` placeholders until real NYC taxi data is acquired and inspected. Once filled and frozen, the same period lock must be used by all subprojects and the demo documentation.

---

## 5. Docs Directory

The `docs/` directory contains five governance documents that define the rules, procedures, and acceptance criteria for the four-codebase development model.

### 5.1 Subproject Boundaries

`subproject-boundaries.md` is the definitive statement of the independence rule and producer/consumer ownership. It lists the four codebases, restates the "share contracts, not application implementation" principle, maps each contract to its producer and consumer in a table, and details the responsibilities of each subproject. It also describes the fixture substitution model — how each subproject uses Foundation fixtures in place of unfinished upstream data during independent development — and the integration step that replaces fixtures with real upstream outputs.

### 5.2 Integration Contracts

`integration-contracts.md` defines the requirements for cross-subproject integration after independent development is complete. It specifies the three integration paths (A→B via Silver Parquet, B→C via Kafka events, A→C via historical Gold) and the pass criteria: no Foundation breaking change needed, no ad-hoc field renames or casts at subproject boundaries, real data passing the same contract tests as fixtures, and sensible end-to-end live metrics. It also lists the address contract (Kafka bootstrap servers, Spark master URL, SeaweedFS endpoint) that must be frozen before integration.

### 5.3 Acceptance Tests

`acceptance-tests.md` defines eleven smoke tests (Tests 0 through J) spanning the Foundation, runtime infrastructure, and individual subprojects. Test 0 is the Foundation fixture validator. Tests A through G verify the runtime prerequisites (Java, Python, Spark, SeaweedFS, Spark↔SeaweedFS round-trip, Kafka, Spark↔Kafka). Tests H through J are the independent contract smoke tests for Batch, Replay, and Streaming respectively. The document also includes a GO decision table mapping each gate (FOUNDATION-GO, GO-A, GO-B, GO-C, INTEGRATION-GO) to its prerequisite tests.

### 5.4 Environment Baseline

`environment-baseline.md` provides a template for the environment lock record — the document that captures the actual working combination of software versions after smoke tests pass. It lists every component that must be pinned (Foundation version, Java, Python, Spark, PySpark, Scala, Hadoop, hadoop-aws, AWS SDK, Confluent Platform, Kafka broker, SeaweedFS) and the rule that locked dependencies must not be upgraded during implementation simply because a newer release appears.

### 5.5 Change Control

`change-control.md` defines the cross-codebase change procedure: propose, update Foundation, update fixtures and validator, re-run Foundation tests, update affected producer, update affected consumer, re-run contract and integration tests. It requires a change record for each modification documenting what changed, why, version numbers, impact on producers and consumers, fixture changes, and scope implications. It also categorizes project elements into three stability tiers: things that must remain stable after freeze, things that may vary by independent machine, and things that may change with evidence.

---

## 6. Fixtures Directory

The `fixtures/` directory contains the reference datasets that make independent subproject development possible. Without these fixtures, each subproject would need live upstream data before it could begin development or testing — defeating the parallel-development model entirely.

### 6.1 Fixture Design Principles

All fixtures use a single coherent scenario: Monday, January 6, 2025 (ISO weekday 1), in the 08:00–08:05 window (hour 8, five-minute slot 0), in the America/New_York timezone (UTC offset -05:00 in January). Two NYC taxi zones are used: zone 161 (Midtown Center) and zone 236 (Upper East Side North). This consistency means a developer can trace a data value from the Bronze fixture through Silver, Kafka, the historical baseline, and into the expected live metrics, verifying that the contracts compose correctly.

The fixtures use CSV and JSONL (one JSON object per line) rather than Parquet. This is a deliberate portability choice: any subproject can load these files with standard-library tools, without requiring Spark or specialized Parquet readers. The integrated production format remains Parquet; the fixtures express the logical contracts, not the physical ones.

### 6.2 Fixture Manifest

`fixture-manifest.yaml` is the index file. It records the scenario parameters (date, day of week, window boundaries, hour, slot, timezone, zones), and for each fixture file it lists the path, format, contract reference, intended consumer, row count, and a description. This manifest allows automated tooling to discover and iterate over all fixtures without hardcoding paths.

### 6.3 Bronze Fixture

`bronze/source_trips.csv` contains 10 rows of raw trip data in the format that the NYC TLC Parquet files would provide. The column names follow the TLC convention (`tpep_pickup_datetime`, `tpep_dropoff_datetime`, `PULocationID`, `DOLocationID`, `trip_distance`, `passenger_count`, `fare_amount`, `total_amount`). The rows span three dates in the historical period (two Mondays in March 2024, a Monday in June 2024, and a Monday in September 2024), providing material for the Batch subproject to practice its cleaning, schema normalization, and time-field derivation logic.

### 6.4 Silver Fixture

`silver/silver_trips.jsonl` contains 7 rows of cleaned Silver trips in the replay period. All 7 trips occur on Monday, January 6, 2025, between 08:00 and 08:04, in the America/New_York timezone. Five trips have `pickup_zone_id` 161 and two have `pickup_zone_id` 236. Each row contains all 14 Silver contract fields with correctly derived time components (`year=2025`, `month=1`, `day_of_week=1`, `hour=8`, `five_minute_slot=0`). This fixture is what the Replay subproject reads when developing independently — it converts each row into a Kafka event.

### 6.5 Kafka Fixture

`kafka/trip_events.jsonl` contains 7 events that correspond to the 7 Silver fixture rows, serialized according to the Kafka trip event schema. Each event has a unique `event_id` following the pattern `trip-YYYYMMDD-HHMMSS-ZONE-SEQ`, an `event_time` in RFC 3339 with explicit `-05:00` offset, and the five numeric fields from the contract. The Streaming subproject uses this fixture in place of a live Kafka topic during independent development.

### 6.6 Historical Baseline Fixture

`gold/historical_zone_baseline.jsonl` contains 2 rows — one for each zone (161 and 236) at the fixture scenario's time slot (day_of_week=1, hour=8, five_minute_slot=0). Zone 161 has a historical mean trip count of 2.0 with a standard deviation of 1.0. Zone 236 has a historical mean of 1.0 and a standard deviation of 1.0. These values are chosen to produce clean, verifiable demand ratios and z-scores when combined with the Kafka event counts.

### 6.7 Expected Live Metrics Fixture

`gold/expected_live_zone_metrics.jsonl` contains 2 rows — the deterministic expected output when the Kafka fixture events are windowed and joined against the historical baseline fixture. Zone 161 has 5 current trips against a historical mean of 2.0, giving a demand ratio of 2.5 (150% above normal) and a z-score of 3.0. Zone 236 has 2 current trips against a historical mean of 1.0, giving a demand ratio of 2.0 (100% above normal) and a z-score of 1.0.

These reference values are not arbitrary — they are the mathematically necessary result of the contract computation rules applied to the fixture inputs. The validator script confirms this by independently computing the expected output and comparing it against the fixture file.

---

## 7. Checks Directory

The `checks/` directory contains a single file: `validate_contract_fixtures.py`, a 666-line Python script that uses only the standard library (no Spark, no third-party packages). This design choice ensures that every Python 3.13 environment can run the first Foundation check without installing additional dependencies.

### 7.1 What the Validator Checks

The validator performs seven categories of checks in sequence.

First, it verifies the `VERSION` file exists and contains `1.0.0`.

Second, it confirms that all five fixture files exist on disk.

Third through sixth, it validates each fixture individually. For the Bronze CSV, it checks that the required source columns are present. For the Silver JSONL, it verifies all 12 required fields are present and non-null, that only recognized fields appear (no unknown extras), that numeric fields are non-negative, that integer fields are integer-valued, and that range-constrained fields (`month`, `day_of_week`, `hour`, `five_minute_slot`) fall within their allowed ranges. For the Kafka JSONL, it enforces all seven required fields, rejects additional properties, validates event-ID uniqueness and non-emptiness, parses `event_time` as RFC 3339 with an explicit UTC offset, and checks positive zone IDs and non-negative numerics. For the historical baseline, it checks required fields, grain uniqueness (no duplicate zone/day/hour/slot combinations), and value ranges. For the expected live metrics, it checks required and nullable fields, integer types, and non-negative counts.

Seventh, the validator performs the cross-contract invariant check — the most important test. It counts Kafka events per pickup zone, builds a lookup from the historical baseline, derives the join key (day of week, hour, slot) from the first event's timestamp, and then for each expected live metric row, it verifies that the current trip count matches the Kafka event count for that zone, that the historical mean matches the baseline lookup, and that the demand ratio and z-score are the correct computations (including the null rules for zero or missing denominators). If any of these values disagree, the validator reports the exact discrepancy.

### 7.2 Validator Output

On success, the validator prints a line-by-line report of each check and ends with:

```
FOUNDATION FIXTURES = PASS
```

On failure, it reports the first error encountered in each check phase and exits with a nonzero status code and the line:

```
FOUNDATION FIXTURES = FAIL (N error(s))
```

This binary pass/fail result is the Foundation GO gate — no subproject should begin independent development until this check passes.

---

## 8. How the Contracts Compose

The Foundation's contracts are not independent documents — they form a directed graph of producer/consumer relationships with shared semantic anchors. Understanding this composition is essential for working with the Foundation.

The Silver contract is the starting material. The Batch subproject produces Silver from Bronze; the Replay subproject consumes Silver and transforms each qualifying row into a Kafka event. The Kafka event schema is deliberately a strict subset of the Silver schema: it drops the timestamps, the nullable optional fields (`passenger_count`, `total_amount`), and the derived time fields (`year`, `month`, `day_of_week`, `hour`, `five_minute_slot`), keeping only the seven fields needed for streaming analytics. This means the Replay producer's job is essentially a projection and format conversion, not a transformation.

The historical baseline contract shares the same time-key structure as the Silver schema's derived fields: `pickup_zone_id`, `day_of_week`, `hour`, `five_minute_slot`. This is not a coincidence — the Streaming subproject derives these same fields from the Kafka event's `event_time` to construct the join key for the historical lookup. The time semantics contract ensures that both the Batch subproject (which builds the baseline) and the Streaming subproject (which queries it) use the same timezone and the same derivation rules.

The live metrics contract is the final output. It combines the streaming window counts with the historical baseline via the join key and applies the demand ratio and z-score formulas with their null rules. The expected live metrics fixture closes the loop: it proves that the fixture inputs, processed according to the contract rules, produce a known deterministic result.

The storage paths contract provides the physical addressing layer beneath all of this — it maps each logical dataset to its S3A path and ensures single-writer ownership, so no two subprojects can accidentally overwrite each other's output.

---

## 9. File Inventory

For quick reference, all 26 files in the Foundation:

| Directory | File | Role |
|---|---|---|
| (root) | `VERSION` | Machine-readable version: `1.0.0` |
| (root) | `README.md` | Entry point documentation |
| (root) | `CHANGELOG.md` | Version history |
| contracts/ | `contract-manifest.yaml` | Master index of all contracts |
| contracts/ | `silver-trips.yaml` | Silver trip schema and validity rules |
| contracts/ | `kafka-trip-event.schema.json` | Kafka event JSON Schema |
| contracts/ | `kafka-topic.yaml` | Kafka topic configuration |
| contracts/ | `historical-zone-baseline.yaml` | Historical Gold baseline schema |
| contracts/ | `live-zone-metrics.yaml` | Live Gold metrics schema |
| contracts/ | `time-semantics.md` | Canonical timezone and derived fields |
| contracts/ | `storage-paths.yaml` | S3A storage layout and ownership |
| config/ | `config-reference.yaml` | Configuration field names and baselines |
| config/ | `data-periods-reference.yaml` | Historical/replay period template |
| docs/ | `subproject-boundaries.md` | Independence rule and ownership |
| docs/ | `integration-contracts.md` | Cross-subproject integration requirements |
| docs/ | `acceptance-tests.md` | Smoke tests and GO gate definitions |
| docs/ | `environment-baseline.md` | Environment lock template |
| docs/ | `change-control.md` | Change procedure and stability tiers |
| fixtures/ | `README.md` | Fixture purpose and scenario description |
| fixtures/ | `fixture-manifest.yaml` | Fixture file index |
| fixtures/ | `bronze/source_trips.csv` | Bronze reference data (10 rows) |
| fixtures/ | `silver/silver_trips.jsonl` | Silver reference data (7 rows) |
| fixtures/ | `kafka/trip_events.jsonl` | Kafka event reference data (7 events) |
| fixtures/ | `gold/historical_zone_baseline.jsonl` | Historical baseline (2 rows) |
| fixtures/ | `gold/expected_live_zone_metrics.jsonl` | Expected output (2 rows) |
| checks/ | `validate_contract_fixtures.py` | Standard-library-only validator |
