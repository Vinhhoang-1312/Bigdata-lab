# UrbanFlow — `urbanflow-replay`

**Subproject B — Kafka Replay**
**Foundation baseline:** `urbanflow-foundation` 1.0.0
**Primary technologies:** Python, Apache Kafka

## Overview

`urbanflow-replay` turns the historical Silver dataset back into a high-rate
"live" event stream, demonstrating **Velocity**. It reads Silver trips, maps
each to the Foundation Kafka event schema, applies rate control, and publishes
to the frozen topic:

```text
Silver -> Foundation Kafka Event -> Rate Control -> urbanflow.trip-events
```

- Message key, event schema, topic name, partition count, and time semantics
  follow `urbanflow-foundation` 1.0.0; only the internal producer design is
  local to this subproject.
- Rate control replays records on an accelerated, controlled schedule so the
  downstream streaming job sees a realistic ongoing flow.

Its output feeds `urbanflow-streaming`, which answers *"what is happening in
this zone right now?"*

The subproject is independently buildable. During independent development it
reads `urbanflow-foundation/fixtures/silver/silver_trips.jsonl`.

## Source data

`urbanflow-replay` reads the Silver dataset produced by `urbanflow-batch`, not
raw files. That Silver derives from the public **NYC TLC Trip Record Data**
(<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>; Yellow-taxi
monthly Parquet at
`https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`).
Full list: the guide below, §1.3.

## Full documentation

See
[docs/UrbanFlow_urbanflow-replay_Subproject_Overview_Architecture_and_Development_Guide_2026-09-06.md](../docs/UrbanFlow_urbanflow-replay_Subproject_Overview_Architecture_and_Development_Guide_2026-09-06.md)
for the functional specification, architecture, and development guide.

Cross-codebase contracts: [../urbanflow-foundation/README.md](../urbanflow-foundation/README.md).
Project entry point: [../README.md](../README.md).
