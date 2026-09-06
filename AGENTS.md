# AGENTS.md

Guidance for coding agents working in this repository.

## Project

UrbanFlow is a contract-first Big Data teaching project built on NYC TLC taxi
trip data. It has four independent codebases:

- `urbanflow-foundation/` — versioned contracts, fixtures, and validation.
- `urbanflow-batch/` — Spark Bronze → Silver → historical Gold processing.
- `urbanflow-replay/` — Silver → rate-controlled Kafka events.
- `urbanflow-streaming/` — Kafka + historical Gold → live Gold and analytics app.

The core rule is: **share contracts, not application implementation**.
Application subprojects must not import source code from one another.

## Sources of truth

Before changing a cross-subproject interface, read the applicable files under:

- `urbanflow-foundation/contracts/`
- `urbanflow-foundation/config/`
- `urbanflow-foundation/docs/`

Foundation version 1.0.0 is frozen. Do not change a shared schema, path, topic,
time meaning, join rule, or ownership rule inside an application subproject.
Cross-codebase changes must follow
`urbanflow-foundation/docs/change-control.md` and be versioned in Foundation
first.

## Frozen conventions

- Analytics timezone: `America/New_York`.
- Spark session timezone: `America/New_York`.
- Kafka timestamps: RFC 3339 with an explicit UTC offset.
- Kafka topic: `urbanflow.trip-events`.
- Kafka partitions: 4; replication factor: 1.
- Kafka key: decimal string form of `pickup_zone_id`.
- Bucket: `urbanflow`.
- Integrated Silver and Gold format: Parquet over S3A.
- Silver path: `s3a://urbanflow/silver/trips/`.
- Historical Gold path: `s3a://urbanflow/gold/historical_zone_baseline/`.
- Live Gold path: `s3a://urbanflow/gold/live_zone_metrics/`.
- Streaming checkpoint path:
  `s3a://urbanflow/checkpoints/live_trip_metrics/`.

Never infer timestamps from the host timezone. Follow the DST and derived-field
rules in `urbanflow-foundation/contracts/time-semantics.md`.

## Development approach

1. Work fixture-first.
2. Preserve exact Foundation field names, types, nullability, and formulas.
3. Validate malformed or rejected data visibly; do not silently discard it.
4. Keep credentials and machine-specific addresses in environment/configuration.
5. Do not commit `.env`, secrets, generated data, Spark output, or checkpoints.
6. Keep analytics serving thin: it reads Gold and does not reproduce ETL.
7. Add real-data cleaning rules only when inspection provides evidence.
8. Historical and replay periods must not overlap.

## Docker environment

The reproducible environment is defined in `compose.yaml`. See
`docs/docker-environment.md` for details.

Initial setup:

```bash
cp .env.example .env
docker compose up -d --build
```

Useful checks:

```bash
docker compose config --quiet
docker compose run --rm foundation-check
docker compose run --rm spark-submit
docker compose run --rm aws
docker compose ps
```

Expected Foundation result:

```text
FOUNDATION FIXTURES = PASS
```

Expected S3A round-trip result:

```text
WRITE = PASS
READ  = PASS
DATA  = PASS
```

Inside Docker, use Compose service addresses:

- Spark master: `spark://spark-master:7077`
- SeaweedFS S3: `http://seaweedfs:8333`

Do not use `localhost` for container-to-container communication.

## Testing requirements

Run the smallest relevant tests after each change. Before declaring work done:

- Run the Foundation validator after any Foundation or contract-related change.
- Run `docker compose config --quiet` after Compose changes.
- Run the Spark local/S3A smoke test after Spark, storage, or dependency changes.
- Compare fixture-derived outputs exactly with Foundation expected fixtures.
- Add tests for schema, time derivation, null rules, invalid data, and contract
  boundaries when implementing a subproject.

Do not weaken tests or modify expected fixtures merely to make an implementation
pass.

## Documentation

Update relevant README/runbook documentation when commands, configuration, or
operational behavior changes. Record actual validated dependency versions, not
intended or newest versions.
