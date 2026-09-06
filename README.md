# Urbanflow Big Data Project

## Prerequisites 

```powershell
docker run -p 8333:8333 -v weed-data:/data `
  -d --name local-s3-storage `
  -e AWS_ACCESS_KEY_ID=admin `
  -e AWS_SECRET_ACCESS_KEY=******* `
  -e S3_BUCKET=my-bucket `
  chrislusf/seaweedfs
```

```bash
$ confluent local services kafka start
```

## UrbanFlow Shared Foundation

**Foundation version:** 1.0.0  
**Purpose:** A versioned, contract-first package shared by the independently developed UrbanFlow subprojects.

The foundation contains **no application implementation that the subprojects must import**. Its job is to define the stable agreements that let three separate codebases be developed and tested on separate machines:

1. `urbanflow-batch` — historical batch analytics;
2. `urbanflow-replay` — Kafka replay producer;
3. `urbanflow-streaming` — live streaming analytics.

The integrated system remains:

```text
NYC taxi source data
        ↓
urbanflow-batch
        ↓
Silver trips ───────────────→ urbanflow-replay
        │                            ↓
        │                          Kafka
        │                            ↓
        └─ Gold historical ─────→ urbanflow-streaming
                                      ↓
                                  Gold live metrics
```

### What is authoritative

The following files are the cross-codebase source of truth:

```text
contracts/contract-manifest.yaml
contracts/silver-trips.yaml
contracts/kafka-trip-event.schema.json
contracts/kafka-topic.yaml
contracts/historical-zone-baseline.yaml
contracts/live-zone-metrics.yaml
contracts/time-semantics.md
contracts/storage-paths.yaml
config/config-reference.yaml
config/data-periods-reference.yaml
docs/subproject-boundaries.md
docs/integration-contracts.md
docs/acceptance-tests.md
```

Each subproject may have its own internal structure and implementation choices, but it MUST conform to the applicable contracts.

### Independence rule

A subproject MUST NOT need source-code imports from another subproject.

Independent development is enabled by the fixtures under `fixtures/`:

```text
urbanflow-batch
    uses fixtures/bronze/

urbanflow-replay
    uses fixtures/silver/

urbanflow-streaming
    uses fixtures/kafka/
    and fixtures/gold/historical_zone_baseline.jsonl
```

The fixtures are deliberately small reference datasets. The production/integration physical formats remain those defined by the contracts (notably Parquet for Silver/Gold and JSON for Kafka events).

### Contract versioning

Foundation contract versioning uses semantic intent:

- **PATCH** — wording/fixture correction that does not change a producer or consumer interface.
- **MINOR** — backward-compatible addition.
- **MAJOR** — breaking schema, semantic, path, or protocol change.

Subprojects should record the foundation version they implement against.

### First validation

With Python 3.13:

```bash
python checks/validate_contract_fixtures.py
```

This uses only the Python standard library. It validates the supplied JSONL fixtures and core cross-contract invariants.

### Change control

After Foundation 1.0.0 is frozen, a contract is changed only for a concrete blocker, required correction, or evidence-based need. See `docs/change-control.md`.
