# Acceptance Tests

**Foundation version:** 1.0.0

## Overview

This document defines the smoke tests and acceptance criteria at two levels:

1. Tests that enable independent subproject development.
2. Tests required before final integration.

---

## Test 0 — Foundation

Run from `urbanflow-foundation`:

```bash
python checks/validate_contract_fixtures.py
```

Required result:

```text
FOUNDATION FIXTURES = PASS
```

---

## Test A — Java

Windows and, where applicable, WSL:

```bash
java -version
```

Expected: Java 21.x.

---

## Test B — Python

```bash
python --version
```

Expected: Python 3.13.x.

---

## Test C — Spark

Verify:

- Spark master starts.
- Spark worker 1 registers.
- Spark worker 2 registers.
- Simple PySpark/Spark job succeeds.
- `spark.sql.session.timeZone = America/New_York`.

---

## Test D — SeaweedFS

Verify:

- SeaweedFS starts.
- S3 endpoint is reachable.
- `urbanflow` bucket can be created/accessed.

---

## Test E — Spark ↔ SeaweedFS

This remains a hard blocker for the real Batch/Gold storage path.

Write a small DataFrame as Parquet to `s3a://urbanflow/test/`, then read it back and compare schema and rows.

Required:

```text
WRITE = PASS
READ  = PASS
DATA  = PASS
```

---

## Test F — Kafka in WSL

Using the installed Confluent/Kafka environment:

- Broker starts.
- `urbanflow.trip-events` can be created.
- Test producer sends.
- Test consumer receives.

---

## Test G — Spark ↔ Kafka

Before final streaming integration:

```text
fixture/dummy producer → Kafka in WSL → Spark Structured Streaming → console/test output
```

Required:

```text
Kafka → Spark = PASS
```

---

## Test H — Independent Batch contract smoke test

Using Foundation Bronze fixture:

```text
source fixture → urbanflow-batch → Silver logical contract
```

Required: fields, types, and time derivations comply with Foundation 1.0.0.

---

## Test I — Independent Replay contract smoke test

Using Foundation Silver fixture:

```text
silver_trips.jsonl → urbanflow-replay → Kafka → generic consumer
```

Required: events comply exactly with Foundation Kafka contract.

---

## Test J — Independent Streaming contract smoke test

Using Foundation Kafka and historical fixtures:

```text
trip_events.jsonl + historical_zone_baseline.jsonl
        → urbanflow-streaming
        → compare to expected_live_zone_metrics.jsonl
```

Required: current counts, demand ratios, z-scores, and null rules match the Foundation reference.

Reference expected results for the supplied fixtures:

```text
Zone 161:
    current_trip_count         = 5
    historical_mean_trip_count = 2.0
    demand_ratio               = 2.5
    z_score                    = 3.0

Zone 236:
    current_trip_count         = 2
    historical_mean_trip_count = 1.0
    demand_ratio               = 2.0
    z_score                    = 1.0
```

---

## GO decisions

| Gate | Preconditions |
|---|---|
| FOUNDATION-GO | Test 0 passes, all contracts frozen |
| GO-A | FOUNDATION-GO + Tests C, D, E pass |
| GO-B | FOUNDATION-GO + Tests B, F pass |
| GO-C | FOUNDATION-GO + Tests C, F, G pass |
| INTEGRATION-GO | A, B, C independent contract tests pass + environment lock completed |
