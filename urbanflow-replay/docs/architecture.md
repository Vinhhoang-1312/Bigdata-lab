# Replay Architecture

```text
Fixture JSONL ─┐
               ├─> Silver iterator ─> event mapper ─> strict validator
S3 Parquet ────┘                                      │
                                                      ▼
metrics <── delivery callback <── Kafka producer <── rate controller
                                      │
                                      ▼
                         urbanflow.trip-events
```

Source adapters yield the same six Silver logical fields. Mapping, validation,
serialization, pacing, and Kafka delivery therefore do not depend on input
format.

The mapper generates a monotonic per-zone sequence and includes pickup time and
zone in each ID. CLI runs add a unique run suffix. This guarantees run-level ID
uniqueness without retaining millions of IDs in memory while allowing the
seven Foundation fixture rows to map deterministically to reference events.

The rate controller uses absolute monotonic deadlines, avoiding cumulative
sleep drift. Kafka sends are asynchronous, use delivery callbacks, and flush on
completion or interruption. Metrics count successful acknowledgements rather
than merely queued messages.

Spark stores Silver timestamps as Parquet timestamps representing UTC instants.
PyArrow exposes these as naive UTC datetimes; the integrated adapter attaches
UTC and converts to `America/New_York` before RFC 3339 serialization. No Silver
business field is renamed, recast, or re-derived.
