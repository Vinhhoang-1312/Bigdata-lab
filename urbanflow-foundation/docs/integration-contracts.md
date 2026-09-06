# Integration Contracts

**Foundation version:** 1.0.0

## Purpose

This document defines the requirements for cross-subproject integration after independent development is complete.

## Integration paths

```text
A real Silver output ─────► B
B real Kafka events ──────► C
A real historical Gold ───► C
C real live Gold ─────────► final demo
```

## A → B integration

```text
urbanflow-batch Silver Parquet
        ↓
urbanflow-replay
```

Requirements:

- B must accept A's Silver output using only the fields and types defined in `contracts/silver-trips.yaml`.
- B must not require Batch-specific undocumented fields.
- B serializes each qualifying Silver row into a Kafka event matching `contracts/kafka-trip-event.schema.json`.

## B → C integration

```text
urbanflow-replay Kafka events
        ↓
urbanflow-streaming
```

Requirements:

- C must accept the event stream without an integration-only schema adapter.
- C declares the expected JSON schema explicitly.
- C rejects or quarantines malformed messages.

## A → C integration

```text
urbanflow-batch historical Gold
        ↓
urbanflow-streaming
```

Requirements:

- C must accept A's historical Gold baseline using the exact key and type structure defined in `contracts/historical-zone-baseline.yaml`.
- The join key (`pickup_zone_id`, `day_of_week`, `hour`, `five_minute_slot`) must be compatible.

## Integration PASS criteria

Integration is successful when:

1. No Foundation breaking change is needed.
2. No ad-hoc field rename or cast exists only at a subproject boundary.
3. Real data passes the same logical contract tests as fixtures.
4. The end-to-end comparison produces sensible live metrics.

## Full integrated path

```text
real source
   ↓
A
   ├── Silver ─────► B ─────► Kafka ─────► C
   └── Historical Gold ───────────────────► C
                                             ↓
                                         Live Gold
```

## Address contract

Before integration, freeze:

```text
Kafka bootstrap_servers
Spark master URL
SeaweedFS S3 endpoint
SeaweedFS bucket
Spark → Kafka reachable address
Replay producer → Kafka reachable address
```

These addresses belong in configuration profiles, not embedded in source code.
