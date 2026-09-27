# UrbanFlow — `urbanflow-replay`

**Subproject B — Kafka Replay**

**Foundation baseline:** `urbanflow-foundation` 1.0.0
**Primary technologies:** Python 3.13, PyArrow, Apache Kafka

## Overview

Replay converts held-out Silver trips into a controlled live-style event stream:

```text
Foundation fixture JSONL ─┐
                          ├─> seven-field event → rate control → Kafka
Canonical Silver Parquet ─┘
```

It owns event projection, validation, serialization, pacing, Kafka delivery,
and producer metrics. It does not clean trips or implement downstream streaming
analytics.

Authoritative contracts are in [`../urbanflow-foundation/`](../urbanflow-foundation/README.md).
The full specification is
[`../docs/UrbanFlow_urbanflow-replay_Subproject_Overview_Architecture_and_Development_Guide_2026-09-06.md`](../docs/UrbanFlow_urbanflow-replay_Subproject_Overview_Architecture_and_Development_Guide_2026-09-06.md).

## Contract

Every Kafka value contains exactly:

```text
event_id
event_time
pickup_zone_id
dropoff_zone_id
trip_distance
trip_duration_minutes
fare_amount
```

Events use pickup time with an explicit date-correct New York offset. The Kafka
key is the decimal pickup-zone ID. The canonical topic is
`urbanflow.trip-events` with four partitions and replication factor one.

Fixture mode reads Foundation's seven Silver JSONL rows. Integrated mode reads
Batch's canonical Parquet directly and selects the frozen February 1–28, 2023
period containing 2,888,729 rows.

## Implementation status

Independent Replay and Batch → Replay integration are complete. Tests cover
exact fixture mapping, strict field/type/value validation, DST offsets, IDs,
message keys, malformed-event exclusion, pacing, metrics, graceful flush,
canonical topic metadata, generic consumption, and direct S3 Parquet input.
Measured rates from 100 through 5,000 events/sec passed.

The Kafka → Spark Foundation-event boundary and bounded full Replay CLI → Kafka
→ Spark Structured Streaming → Live Gold acceptance are validated. On September
14 and September 20, separate 10,000-event canonical Replay runs had zero
invalid rows and zero mismatches against run-scoped Live Gold references (4,394
and 2,863 rows respectively). Replay finished before each `availableNow` Spark
query. The remaining cross-subproject gate is a rate-controlled velocity run
while Streaming is actively consuming, with downstream progress and Gold
correctness recorded. The prior producer-only rate tests do not satisfy that
gate. These finite runs do not establish continuous production readiness. See
[`docs/implementation-status.md`](docs/implementation-status.md) and the
[Streaming results](../urbanflow-streaming/docs/results/end-to-end-streaming-health-20260920.md).

## Start and test

Run from the repository root:

```bash
docker compose up -d --build
docker compose run --rm foundation-check
docker compose run --rm kafka-cli

docker compose run --rm replay-python \
  python /workspace/urbanflow-replay/tests/run_contract_tests.py

docker compose run --rm replay-python \
  python /workspace/urbanflow-replay/tests/run_kafka_integration.py

docker compose run --rm replay-python \
  python /workspace/urbanflow-replay/tests/verify_batch_boundary.py
```

Preview fixture events without publishing:

```bash
docker compose run --rm replay-python \
  python /workspace/urbanflow-replay/producer/replay_trips.py \
  --mode fixture --dry-run --print-events 7
```

Publish 10,000 real February events at 1,000 events/sec (fixed-rate mode):

```bash
docker compose run --rm replay-python \
  python /workspace/urbanflow-replay/producer/replay_trips.py \
  --mode integrated --rate 1000 --max-events 10000 --print-events 1
```

### Finite virtual-time replay

Accelerate the frozen February event sequence proportionally while preserving every
historical `event_time` exactly:

```bash
# 60x acceleration (1s wall time = 1m event time) with a dynamic burst limiter
docker compose run --rm replay-python \
  python /workspace/urbanflow-replay/producer/replay_trips.py \
  --mode integrated --speedup 60
```

- `--speedup <N>`: Event-time acceleration factor (for example, 60 or 300).
- `--max-rate <N>`: Optional peak events-per-second limiter. When omitted, Replay
  uses `max_rate = max(500.0, 4.0 * speedup)`.
- Replay stops at the end of the frozen source period. At 60×, the complete
  February sequence lasts approximately 11.2 hours. Start a clean demo lifecycle
  for another run; Replay never rebases historical timestamps.

#### Unified `MAX_RATE` Policy & Empirical Basis

Python (`replay.py`) serves as the single source of truth for replay rate limiting:

$$\text{max\_rate} = \max(500.0,\, 4.0 \times \text{speedup})$$

**Empirical Derivation (February 2023 NYC TLC Workload):**
- Total trips: 2,888,729 over 28 days (2,419,200 seconds) $\rightarrow$ average baseline rate of $\approx 1.194\text{ trips/sec}$ in 1x real time.
- **At 60x acceleration:**
  - Mean rate: $1.194 \times 60 \approx 71.6\text{ eps}$.
  - 99th percentile peak rate: $\approx 148\text{ eps}$.
  - Maximum 1-second burst: $\approx 200\text{ eps}$.
  - Default limiter: $\max(500.0, 240.0) = 500\text{ eps}$ ($>2.5\times$ headroom over p99 peak, preventing unnatural smoothing of real traffic bursts).
- **At 300x acceleration:**
  - Mean rate: $1.194 \times 300 \approx 358.2\text{ eps}$.
  - 99th percentile peak rate: $\approx 713\text{ eps}$.
  - Maximum 1-second burst: $\approx 960\text{ eps}$.
  - Default limiter: $\max(500.0, 1200.0) = 1200\text{ eps}$ (accommodates 960 eps burst while shielding local Kafka/Spark executors from overload).

#### Bounded-Memory Batch Streaming Architecture

Replay implements bounded-memory batch streaming via the PyArrow Dataset Scanner:
- **Chunk size (`batch_size`):** 8,192 rows per `RecordBatch`.
- **Readahead parameters:** `batch_readahead=4`, `fragment_readahead=2`.
- **Empirical validation:** Scanned all 2,888,729 rows of February 2023 Silver in SeaweedFS:
  - Initial RSS: ~16.6 MB.
  - Steady RSS: ~404 MB with default scanner readahead (~215 MB with single readahead).
  - RSS remained flat across all 2.89M rows ($<0.04\text{ MB}$ drift between row 200,000 and row 2,888,729), proving strict memory boundedness without materializing the dataset in Python.

#### Timezone Semantics & Rush Hour Preservation

NYC TLC trip logs record timestamps in naive local New York wall-clock time (`America/New_York`):
1. **Bronze $\rightarrow$ Silver ETL:** PySpark parses naive timestamps under session timezone `America/New_York`. Parquet files store timestamps as standard UTC instants (e.g. `2023-02-01 00:32:53 EST` is stored as `2023-02-01 05:32:53 UTC`).
2. **Replay Reader Normalization:** PyArrow reads the naive UTC timestamp. `input_reader.py` normalizes it:
   ```python
   if pickup.tzinfo is None:
       pickup = pickup.replace(tzinfo=UTC)
   row["pickup_datetime"] = pickup.astimezone(NYC).isoformat()
   ```
   - Standard Time (Feb): `2023-02-15 13:30:00 UTC` $\rightarrow$ `2023-02-15T08:30:00-05:00` (hour=8 morning rush hour).
   - Daylight Saving Time (July): `2023-07-15 12:30:00 UTC` $\rightarrow$ `2023-07-15T08:30:00-04:00` (hour=8 morning rush hour).
   *(Note: treating naive Parquet timestamps as local via `replace(tzinfo=NYC)` would be incorrect, as it would distort an 8:30 AM morning rush hour to 1:30 PM).*
3. **Kafka Delivery & Streaming Ingestion:** Kafka payload carries the explicit RFC 3339 offset (`-05:00` / `-04:00`), which Spark Structured Streaming parses cleanly, ensuring Live Gold metrics align with Historical Gold baseline slots.

The old `make demo`/Streamlit lifecycle from the merge branch is not part of
the React integration. Use `start-urbanflow.bat` for the user-facing React UI,
and the bounded Replay commands in the runbooks below for producer tests.

## Evidence and operations

- Architecture: [`docs/architecture.md`](docs/architecture.md)
- Fixture replay: [`runbooks/run_fixture_replay.md`](runbooks/run_fixture_replay.md)
- Kafka smoke test: [`runbooks/run_kafka_smoke_test.md`](runbooks/run_kafka_smoke_test.md)
- Integrated replay: [`runbooks/run_integrated_replay.md`](runbooks/run_integrated_replay.md)
- Velocity experiment: [`runbooks/run_velocity_experiment.md`](runbooks/run_velocity_experiment.md)
- Fixture Kafka result: [`docs/results/fixture-kafka-smoke.md`](docs/results/fixture-kafka-smoke.md)
- Batch boundary result: [`docs/results/batch-boundary.md`](docs/results/batch-boundary.md)
- Velocity result: [`docs/results/velocity-experiment.md`](docs/results/velocity-experiment.md)
