# Change Control

**Foundation version:** 1.0.0

## Principle

Once a decision or Foundation contract is marked **FROZEN** or **LOCKED**, do not change it casually during implementation. Change it only when:

- the current decision creates a concrete blocker;
- a required bug fix or compatibility issue justifies the change; or
- new evidence shows the frozen decision prevents the project from meeting its stated success criteria.

## Cross-codebase change procedure

If a change affects a producer/consumer interface:

```text
Propose change
     ↓
Update/version Foundation
     ↓
Update fixtures + validator
     ↓
Re-run Foundation tests
     ↓
Update affected producer
     ↓
Update affected consumer
     ↓
Re-run independent contract tests
     ↓
Re-run integration test
```

A subproject cannot self-authorize a breaking shared contract change.

## Change record

For each change, record:

```text
What changed?
Why?
Foundation old/new version?
Producer impact?
Consumer impact?
Fixtures changed?
Environment changed?
Which tests were re-run?
Did scope or success criteria change?
```

## What should remain stable

After Foundation/Integration freeze:

- Project scope and non-goals.
- Four-codebase organization.
- Foundation contract major version.
- Bronze / Silver / Gold terminology.
- Silver logical contract.
- Kafka event contract.
- Historical/live Gold contracts.
- New York time semantics.
- Producer/consumer ownership.
- Kafka topic and partitioning baseline.
- Integrated storage paths.
- Tested software/dependency versions.
- Primary success criteria.
- The five-stage Data Sources -> Ingestion -> Storage -> Processing ->
  Analytics conceptual pipeline.
- A lightweight user-facing Analytics serving layer consuming Gold data.
  The specific web framework may change; the requirement may not.

## What may vary by independent machine

These may differ without changing Foundation:

- Machine hostname/IP.
- Local Kafka/Spark/SeaweedFS endpoint.
- Temporary local fixture path.
- CPU/memory allocation.
- Development-only logging.
- Local test topic suffix if clearly separate from the integration topic.

Those differences belong in local profiles, not in shared semantics.

## What may change with evidence

The following may be adjusted if measurements justify it:

- Number of months processed.
- Spark memory/CPU settings.
- Spark partition count.
- Replay rate.
- Optional visualization.
- Additional documented cleaning rules.
- Whether z-score is displayed in the demo.
