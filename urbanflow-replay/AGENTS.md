# AGENTS.md — `urbanflow-replay`

The repository-root `AGENTS.md` also applies.

## Scope

Replay owns the Python path:

```text
Foundation Silver → strict seven-field JSON → paced Kafka producer
```

It does not clean Bronze data, rebuild Silver semantics, perform Structured
Streaming aggregation, write Gold, or implement analytics UI. Never import
Batch or Streaming application code.

Authoritative inputs and outputs:

- `../urbanflow-foundation/contracts/silver-trips.yaml`
- `../urbanflow-foundation/contracts/kafka-trip-event.schema.json`
- `../urbanflow-foundation/contracts/kafka-topic.yaml`
- `../urbanflow-foundation/contracts/time-semantics.md`
- `../urbanflow-foundation/config/data-periods-reference.yaml`

The integrated replay period is February 1–28, 2023. Read canonical Silver
straight from `s3a://urbanflow/silver/trips/` and publish to
`urbanflow.trip-events` with `str(pickup_zone_id)` as the key.

## Invariants

Each value contains exactly:

```text
event_id, event_time, pickup_zone_id, dropoff_zone_id,
trip_distance, trip_duration_minutes, fare_amount
```

- `event_time` is pickup time in RFC 3339 with the date-correct New York offset.
- IDs are non-empty and unique within a run; CLI run suffixes distinguish runs.
- Zones are positive integers; numeric measurements are finite and non-negative.
- Invalid events are counted and never sent.
- Delivery errors are visible; outstanding sends are flushed before exit.
- Replay wall-clock pacing never changes historical `event_time`.
- Topic configuration is 4 partitions and replication factor 1 in KRaft mode.

## Code layout

- `producer/input_reader.py` — Foundation JSONL and canonical S3 Parquet.
- `producer/event_mapper.py` — Silver projection and traceable event IDs.
- `producer/event_validator.py` — strict contract checks.
- `producer/event_time.py` — New York offset and DST validation.
- `producer/rate_controller.py` — absolute-deadline pacing.
- `producer/kafka_producer.py` — Confluent Kafka delivery adapter.
- `producer/replay.py` — source-independent orchestration and metrics.
- `producer/replay_trips.py` — CLI only.

PyArrow exposes Spark Parquet timestamps as naive UTC values. Convert that
physical representation to `America/New_York` RFC 3339 in the source adapter;
this is serialization of the Foundation timestamp, not new Batch ETL.

## Required checks

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

docker compose run --rm replay-python \
  python /workspace/urbanflow-replay/tests/run_velocity_experiment.py
```

Never weaken fixture equality, malformed-event, key, topic, delivery, direct
Parquet, or rate-tolerance checks. Velocity evidence must report requested and
achieved rates. Update runbooks, implementation status, measured results, and
the environment lock when behavior or dependencies change.

Replay → Streaming acceptance remains deferred until Streaming exists; then
verify a generic Foundation event is consumed without an integration adapter.
