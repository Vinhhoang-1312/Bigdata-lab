# UrbanFlow Replay Implementation Status

**Foundation baseline:** 1.0.0

## Independently complete

- [x] Foundation version and frozen configuration checks.
- [x] Compose-managed Python 3.13 Replay image.
- [x] Compose-managed single-broker Kafka 3.9.1 in KRaft mode.
- [x] Idempotent canonical topic creation: 4 partitions, replication factor 1.
- [x] Seven-row Foundation Silver fixture reader.
- [x] Deterministic fixture mapping and strict seven-field JSON serialization.
- [x] Traceable, unique event IDs and per-CLI-run suffixes.
- [x] RFC 3339, New York offset, and DST validation.
- [x] Positive zone and finite/non-negative numeric validation.
- [x] Decimal pickup-zone message keys and stable keyed partitioning.
- [x] Malformed records counted and prevented from publishing.
- [x] Asynchronous acknowledged sends, errors, flush, and graceful interruption.
- [x] Absolute-deadline rate control and run-level metrics.
- [x] Generic Kafka consumer fixture equality test.
- [x] Direct canonical February Silver Parquet reader using PyArrow/S3.
- [x] Batch Silver → Replay → Kafka boundary test without an ad-hoc adapter.
- [x] Measured 100, 500, 1,000, 2,500, and 5,000 event/sec runs.

## Cross-subproject acceptance

- [x] Bounded full Replay CLI → Kafka → Spark Structured Streaming → Live Gold acceptance without an integration-only adapter (2026-09-14 and fresh 2026-09-20 runs; 10,000 events each, zero invalid rows and zero Gold mismatches).
- [x] Kafka → Spark Structured Streaming acceptance Test G using the authoritative Foundation event shape.
- [ ] Final end-to-end Velocity experiment with Streaming active during rate-controlled publication. The earlier 100–5,000 events/sec producer-only measurements and the bounded Live Gold runs do not establish this concurrent condition.

Replay is ready to provide fixture or canonical February events to Streaming. The bounded verifier ran Replay before Spark's `availableNow` query; see `../urbanflow-streaming/docs/results/end-to-end-streaming.md` and `end-to-end-streaming-health-20260920.md`. The only deferred Replay cross-subproject gate is the concurrent streaming-active velocity experiment, defined in `../urbanflow-replay/runbooks/run_velocity_experiment.md`. Passing a bounded run does not establish continuous production operation.
