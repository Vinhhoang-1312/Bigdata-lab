# Changelog — urbanflow-foundation

All notable changes to this package are documented in this file.

Versioning follows **semantic intent**:

- **PATCH** — wording or fixture correction that does not change a producer or consumer interface.
- **MINOR** — backward-compatible addition (new optional field, new fixture file, new documentation).
- **MAJOR** — breaking schema, semantic, path, or protocol change.

---

## [1.0.0] — 2026-09-06

Foundation 1.0.0 is recorded as a single release. The hardening below was
applied before any application subproject pinned the version, so it is part of
1.0.0 rather than a separate release. No producer or consumer interface
changed: every contract schema, path, frozen value, and null rule is unchanged.

### Hardened before first use

- `checks/validate_contract_fixtures.py`
  - Cross-contract counts are now grouped by `(pickup_zone_id, window_start)`
    instead of by zone alone, and the window key is derived per event instead
    of from the first event. Multi-window fixtures are now validated correctly.
  - `window_start` / `window_end` are validated: RFC 3339, correct
    America/New_York offset, aligned to a five-minute boundary, exactly five
    minutes apart, unique per zone.
  - Every `(zone, window)` produced by the events must appear in the expected
    live metrics, so a window can no longer be silently dropped.
  - Silver rows are checked against their timestamps: `dropoff >= pickup`,
    `trip_duration_minutes` matches the two timestamps, and `year`, `month`,
    `day_of_week`, `hour`, `five_minute_slot` are actually derived from
    `pickup_datetime` in America/New_York.
  - Timestamp offsets are checked against the US DST rule, computed from the
    standard library so the validator keeps working without `tzdata`.
  - RFC 3339 is enforced by pattern; `fromisoformat` alone accepted a space
    separator and a missing offset.
  - Zone IDs in the Silver and Kafka fixtures must exist in the zone lookup.
  - Silver and Kafka fixtures must describe the same trips.
  - The bronze fixture must contain at least one rejectable row.
- `fixtures/bronze/zone_lookup.csv` — added. The Silver and baseline contracts
  already made the zone lookup normative, but no fixture supplied it.
- `fixtures/bronze/source_trips.csv` — added five rows, each invalid under
  exactly one Silver validity rule, so the cleaning path cannot go untested.
- `fixtures/silver/expected_silver_from_bronze.jsonl` — added. Gives
  urbanflow-batch the reference output that urbanflow-streaming already had.
- `fixtures/silver/silver_trips.jsonl`, `fixtures/kafka/trip_events.jsonl` —
  `trip_duration_minutes` corrected to exact values. Six of seven rows were
  rounded to one decimal and one (18.2 against timestamps giving 18.25) was
  inconsistent with its own timestamps.
- `contracts/time-semantics.md` — added the frozen daylight-saving rules.
- `contracts/contract-manifest.yaml`, `docs/subproject-boundaries.md`,
  `docs/change-control.md` — the live-metrics consumer is the analytics app,
  which is mandatory scope, not an "optional dashboard".
- `docs/acceptance-tests.md` — added Test K for the analytics app and restored
  the fixture-readable preconditions to the GO table.

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
- `fixtures/bronze/source_trips.csv` — Bronze reference fixture (15 rows: 10 valid, 5 rejectable).
- `fixtures/bronze/zone_lookup.csv` — taxi-zone lookup fixture (8 zones).
- `fixtures/silver/expected_silver_from_bronze.jsonl` — Bronze-derived Silver reference output (10 rows).
- `fixtures/silver/silver_trips.jsonl` — Silver reference fixture (7 rows, replay-period trips).
- `fixtures/kafka/trip_events.jsonl` — Kafka event reference fixture (7 events).
- `fixtures/gold/historical_zone_baseline.jsonl` — Historical baseline reference fixture.
- `fixtures/gold/expected_live_zone_metrics.jsonl` — Expected live metrics reference fixture.
- `checks/validate_contract_fixtures.py` — Standard-library-only fixture and cross-contract validator.
