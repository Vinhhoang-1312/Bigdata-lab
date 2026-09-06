# UrbanFlow Foundation Fixtures

**Foundation version:** 1.0.0

## Purpose

These fixtures are deliberately small reference datasets that enable independent subproject development and contract testing without requiring live upstream data or infrastructure.

They are **not** representative of production volume. Their purpose is to verify that:

1. each subproject correctly interprets the Foundation contracts;
2. schema, types, and derived fields match the canonical definitions;
3. cross-contract invariants hold (e.g., live metrics match the computation from events + baseline).

## Fixture map

```text
urbanflow-batch       uses fixtures/bronze/source_trips.csv
urbanflow-replay      uses fixtures/silver/silver_trips.jsonl
urbanflow-streaming   uses fixtures/kafka/trip_events.jsonl
                      and fixtures/gold/historical_zone_baseline.jsonl
```

## Physical formats

The fixture files use CSV and JSONL (one JSON object per line) for portability and readability. These lightweight formats allow any subproject to load them without specialized dependencies.

The integrated production/integration physical formats remain:

- **Silver / Gold:** Parquet on S3A.
- **Kafka events:** JSON messages on the `urbanflow.trip-events` topic.

## Reference expected result

The Kafka event fixture and historical baseline fixture together produce a deterministic expected live-metrics result stored in:

```text
fixtures/gold/expected_live_zone_metrics.jsonl
```

Reference values:

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

## Fixture scenario

All fixtures use a consistent scenario:

- **Date:** Monday, January 6, 2025 (ISO weekday = 1).
- **Window:** 08:00–08:05 (hour=8, five_minute_slot=0).
- **Analytics timezone:** America/New_York (UTC offset -05:00 in January).
- **Zones:** 161 (Midtown Center) and 236 (Upper East Side North).

The bronze fixture contains representative historical-period rows (2024) to test the batch cleaning pipeline.

## Validation

```bash
python checks/validate_contract_fixtures.py
```

This standard-library-only script validates fixture structure, field presence, types, value ranges, and cross-contract invariants.
