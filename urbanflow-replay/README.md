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

Replay → Streaming and Kafka → Spark tests remain deferred until Streaming is
implemented. See [`docs/implementation-status.md`](docs/implementation-status.md).

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

Publish 10,000 real February events at 1,000 events/sec:

```bash
docker compose run --rm replay-python \
  python /workspace/urbanflow-replay/producer/replay_trips.py \
  --mode integrated --rate 1000 --max-events 10000 --print-events 1
```

## Evidence and operations

- Architecture: [`docs/architecture.md`](docs/architecture.md)
- Fixture replay: [`runbooks/run_fixture_replay.md`](runbooks/run_fixture_replay.md)
- Kafka smoke test: [`runbooks/run_kafka_smoke_test.md`](runbooks/run_kafka_smoke_test.md)
- Integrated replay: [`runbooks/run_integrated_replay.md`](runbooks/run_integrated_replay.md)
- Velocity experiment: [`runbooks/run_velocity_experiment.md`](runbooks/run_velocity_experiment.md)
- Fixture Kafka result: [`docs/results/fixture-kafka-smoke.md`](docs/results/fixture-kafka-smoke.md)
- Batch boundary result: [`docs/results/batch-boundary.md`](docs/results/batch-boundary.md)
- Velocity result: [`docs/results/velocity-experiment.md`](docs/results/velocity-experiment.md)
