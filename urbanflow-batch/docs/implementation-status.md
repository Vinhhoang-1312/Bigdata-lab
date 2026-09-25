# UrbanFlow Batch Implementation Status

**Foundation baseline:** 1.0.0

## Independent Batch Definition of Done — complete

- [x] Foundation version recorded and validator passing.
- [x] Managed January/February 2023 acquisition and original-file Bronze landing.
- [x] Source file, byte, row, rejection, output, and runtime metrics.
- [x] January 2023 historical and February 2023 replay period lock.
- [x] CSV and mixed-schema monthly Parquet source adapters.
- [x] Bronze → Silver mapping, validity rules, reason-coded rejections, and
  New York/DST derivations.
- [x] Silver schema, exact fixture, idempotency, partition, S3A, and read-back
  tests.
- [x] Local Spark and two-worker Spark execution.
- [x] RDD/DataFrame/Spark SQL result equivalence.
- [x] Historical Gold builder with complete eligible zero-count grid.
- [x] `stddev_pop`, DST slot eligibility, metric, schema, unique-grain,
  idempotency, and S3A read-back tests.
- [x] Canonical integrated Silver and Historical Gold outputs.
- [x] Two-month Volume experiment with recorded evidence.
- [x] Real-data cleaning observations documented without adding unsupported
  rules.
- [x] Batch canonical February Silver → Replay → Kafka without an ad-hoc
  boundary adapter.

## Cross-subproject acceptance

These are integration gates:

- [x] Batch canonical Historical Gold → Streaming without an adapter.
- [x] Complete environment lock with Streaming runtime details.
- [ ] Final end-to-end integration acceptance (latest recorded end-to-end verifier result is `FAIL`).

Replay now consumes Batch's canonical February Silver partition directly and
publishes Foundation-compliant Kafka events.
