# Changelog — urbanflow-foundation

All notable changes to this package are documented in this file.

Versioning follows **semantic intent**:

- **PATCH** — wording or fixture correction that does not change a producer or consumer interface.
- **MINOR** — backward-compatible addition (new optional field, new fixture file, new documentation).
- **MAJOR** — breaking schema, semantic, path, or protocol change.

---

## [1.0.0] — 2026-09-06

### Added

- Initial Foundation release.
- `contracts/contract-manifest.yaml` — master index of all cross-codebase contracts.
- `contracts/silver-trips.yaml` — Silver trip logical schema and validity rules.
- `contracts/kafka-trip-event.schema.json` — Kafka event JSON Schema (Draft 2020-12).
- `contracts/kafka-topic.yaml` — Kafka topic-level configuration and semantics.
- `contracts/historical-zone-baseline.yaml` — Historical Gold baseline schema and construction rules.
- `contracts/live-zone-metrics.yaml` — Live Gold metrics schema, demand-ratio, and z-score rules.
- `contracts/time-semantics.md` — Canonical analytics time zone and derived-field definitions.
- `contracts/storage-paths.yaml` — Integrated S3A storage layout and ownership map.
- `config/config-reference.yaml` — Reference configuration field names and baseline values.
- `config/data-periods-reference.yaml` — Template for freezing historical and replay period dates.
- `docs/subproject-boundaries.md` — Producer/consumer ownership and independence rule.
- `docs/integration-contracts.md` — Cross-subproject integration requirements.
- `docs/acceptance-tests.md` — Foundation, infrastructure, and subproject smoke-test definitions.
- `docs/environment-baseline.md` — Template for the environment lock record.
- `docs/change-control.md` — Cross-codebase change-control procedure.
- `fixtures/README.md` — Fixture purpose, scope, and usage guide.
- `fixtures/fixture-manifest.yaml` — Fixture file index with contract references.
- `fixtures/bronze/source_trips.csv` — Bronze reference fixture (10 rows).
- `fixtures/silver/silver_trips.jsonl` — Silver reference fixture (7 rows, replay-period trips).
- `fixtures/kafka/trip_events.jsonl` — Kafka event reference fixture (7 events).
- `fixtures/gold/historical_zone_baseline.jsonl` — Historical baseline reference fixture.
- `fixtures/gold/expected_live_zone_metrics.jsonl` — Expected live metrics reference fixture.
- `checks/validate_contract_fixtures.py` — Standard-library-only fixture and cross-contract validator.
