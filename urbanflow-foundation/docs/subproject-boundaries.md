# Subproject Boundaries

**Foundation version:** 1.0.0

## Four codebases

```text
urbanflow-foundation/    contract-first governance
urbanflow-batch/         Subproject A — historical batch analytics
urbanflow-replay/        Subproject B — Kafka replay producer
urbanflow-streaming/     Subproject C — live streaming analytics
```

## Independence rule

```text
SHARE CONTRACTS, NOT APPLICATION IMPLEMENTATION.
```

No application subproject imports Python modules or implementation classes from another application subproject.

Each codebase records the Foundation version it implements against.

## Producer / consumer ownership

| Contract | Producer | Consumer |
|---|---|---|
| Silver trips | `urbanflow-batch` | `urbanflow-replay` |
| Kafka trip event | `urbanflow-replay` | `urbanflow-streaming` |
| Historical zone baseline | `urbanflow-batch` | `urbanflow-streaming` |
| Live zone metrics | `urbanflow-streaming` | `urbanflow-streaming` analytics app / demo |

Each persistent shared path has one owning writer. Consumers must not rewrite another subproject's contract output.

## Subproject responsibilities

### A — urbanflow-batch

- Acquire and ingest source data.
- Maintain Bronze layer.
- Produce canonical Silver trips (Parquet).
- Demonstrate RDD / DataFrame / Spark SQL equivalence.
- Produce historical Gold baseline (Parquet).

### B — urbanflow-replay

- Consume canonical Silver trips.
- Serialize Foundation-compliant Kafka events (JSON).
- Apply configurable replay rates.
- Publish to `urbanflow.trip-events`.
- Expose producer-side measurements.

### C — urbanflow-streaming

- Consume Foundation-compliant Kafka events.
- Perform five-minute event-time aggregation.
- Join the historical Gold baseline.
- Calculate demand ratio and optional z-score.
- Write Gold live metrics and checkpoints.
- Serve Gold historical and live outputs through a lightweight web analytics
  application (Streamlit preferred).
- Provide dashboard, visualization, filtering, comparison, and simple
  interactive analytical tools.

The analytics app is a thin consumer of Gold data. It belongs to Subproject C
for implementation convenience, but it must stay decoupled from the Structured
Streaming job so it can be tested independently against Foundation Gold
fixtures. The Analytics stage is mandatory project scope; only the choice of
web framework is negotiable.

## Independent development

During independent development, each subproject substitutes Foundation fixtures for unfinished upstream data:

```text
urbanflow-batch       uses fixtures/bronze/
urbanflow-replay      uses fixtures/silver/
urbanflow-streaming   uses fixtures/kafka/ and fixtures/gold/historical_zone_baseline.jsonl
```

No live connectivity between subprojects is required during ordinary development.

## Integration

After each subproject passes its own contract tests, integration replaces fixtures with real upstream outputs:

```text
A real Silver    → B
B real events    → C
A real Gold      → C
```

No new adapter should be required. If integration requires a contract-changing adapter, that is evidence of Foundation or implementation inconsistency.
