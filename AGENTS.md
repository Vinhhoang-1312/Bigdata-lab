# AGENTS.md

Repository-wide guidance for coding agents. A subproject may provide a nested
`AGENTS.md` with additional rules for files under that directory.

## Architecture

UrbanFlow is a contract-first Big Data teaching project using NYC TLC taxi data:

- `urbanflow-foundation/` — shared contracts, fixtures, and acceptance rules.
- `urbanflow-batch/` — Spark Bronze → Silver → Historical Gold.
- `urbanflow-replay/` — Silver → rate-controlled Kafka events.
- `urbanflow-streaming/` — Kafka + Historical Gold → Live Gold and analytics.

**Share contracts, not application implementation.** Application subprojects
must not import source code from one another.

## Authority and change control

Cross-project sources of truth are under `urbanflow-foundation/contracts/`,
`config/`, and `docs/`. Foundation version 1.0.0 governs the current system.
Never change a shared schema, path, topic, time meaning, formula, join rule, or
ownership rule inside an application subproject. Follow
`urbanflow-foundation/docs/change-control.md` for cross-codebase changes.

Core conventions:

- Analytics and Spark session timezone: `America/New_York`.
- Boundary timestamps carry explicit RFC 3339 UTC offsets.
- Kafka topic: `urbanflow.trip-events`; 4 partitions; zone ID message key.
- Storage bucket: `urbanflow`; integrated Silver and Gold use Parquet/S3A.
- Historical and replay periods are non-overlapping and frozen in Foundation.

Read the applicable Foundation contract instead of relying only on this summary.

## Working rules

1. Work fixture-first and compare exact expected outputs.
2. Preserve contract names, types, nullability, paths, and formulas.
3. Make malformed/rejected data visible; never silently discard it.
4. Keep credentials and machine addresses in environment/configuration.
5. Never commit `.env`, secrets, generated data, outputs, or checkpoints.
6. Add cleaning rules only when real-data evidence is documented.
7. Keep jobs, tests, configuration, and presentation concerns separated.
8. Do not weaken tests or rewrite expected fixtures to make code pass.

## Environment and validation

The reproducible environment is in `compose.yaml`; operational details are in
`docs/docker-environment.md`.

```bash
cp .env.example .env
docker compose up -d --build
docker compose run --rm foundation-check
docker compose run --rm spark-submit
docker compose run --rm kafka-cli
docker compose config --quiet
```

Inside Docker, use Compose service names such as `seaweedfs` and
`spark-master`, not `localhost`, for container-to-container connections.

Run the smallest relevant tests after each change. Re-run Foundation validation
for contract-related changes and the S3A smoke test for storage/runtime changes.
Update relevant READMEs, runbooks, results, and the environment lock when
validated behavior or versions change.

## System Navigation Links

Sidebar navigation links with accurate port descriptors:

• Streamlit Analytics Dashboard: http://localhost:8501
• FastAPI Health API: http://localhost:8000/v1/streaming/health
• FastAPI OpenAPI Documentation: http://localhost:8000/docs
• Spark Driver Web UI: http://localhost:4040 (active during streaming job)
• Spark Master Web UI: http://localhost:8080 (idle background infrastructure)
• Spark Worker 1 Web UI: http://localhost:8081 (idle background infrastructure)
• Spark Worker 2 Web UI: http://localhost:8082 (idle background infrastructure)
• SeaweedFS S3 API: http://localhost:8333 (explicitly noted as an S3 API endpoint, not a web console)
• SeaweedFS Filer: http://localhost:8888
• SeaweedFS Master UI: http://localhost:9333

## One-Command Demo Lifecycle (`make demo`)

UrbanFlow features an automated One-Command Demo orchestration for continuous ingestion, virtual time replay, streaming sliding-window aggregations, and the Glass Cockpit observability plane.

Primary control commands:

- `make demo` — Starts the complete stack (SeaweedFS, Kafka, Spark cluster, bucket and topic initialization, Spark Structured Streaming driver on port `4040`, virtual time replay producer at 60x acceleration in continuous loop mode, FastAPI observability service on port `8000`, and Streamlit dashboard on port `8501`).
- `make demo-status` — Queries live component health, FastAPI telemetry, Streamlit status, Spark heartbeat (`.telemetry/streaming_health.json`), Kafka partition backlogs, and virtual time replay clock.
- `make demo-stop` — Cleanly and idempotently stops demo containers and reclaims host ports `8000` and `8501` without orphaned processes.
- `make test` — Runs the end-to-end test suite across all subprojects (contract fixtures, replay serialization, streaming smoke, observability health state machine, and global chronological monotonicity scans).

Runtime overrides:
`make demo SPEEDUP=300` (e.g. 300x acceleration), `make demo DEMO_MODE=fixture`, `make demo STREAMLIT_PORT=8502 FASTAPI_PORT=8001`.

Detailed operator instructions, architecture diagrams, and health state semantics are documented in `docs/demo-runbook.md`.