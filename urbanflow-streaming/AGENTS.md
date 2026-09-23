# AGENTS.md — `urbanflow-streaming`

The root `AGENTS.md` also applies. This file adds Streaming- and Analytics-specific guidance.

## Scope and boundaries

Streaming owns:

```text
Kafka (`urbanflow.trip-events`) + Historical Gold baseline → Structured Streaming → Live Gold (S3 Parquet) → Streamlit Analytics UI & Serving API
```

It does not own Bronze source landing, cleaning, or Historical Gold construction (`urbanflow-batch`), nor Silver reading or Kafka event replay pacing (`urbanflow-replay`), nor model training (`urbanflow-ml`). Do not import Batch, Replay, or ML application code.

Authoritative contracts:

- `../urbanflow-foundation/contracts/kafka-trip-event.schema.json`
- `../urbanflow-foundation/contracts/kafka-topic.yaml`
- `../urbanflow-foundation/contracts/historical-zone-baseline.yaml`
- `../urbanflow-foundation/contracts/live-zone-metrics.yaml`
- `../urbanflow-foundation/contracts/time-semantics.md`
- `../urbanflow-foundation/contracts/storage-paths.yaml`

## Contract inputs and outputs

- **Kafka topic:** `urbanflow.trip-events` (4 partitions, string key = `pickup_zone_id`).
- **Historical Gold input:** `s3a://urbanflow/gold/historical_zone_baseline/` (read via broadcast join).
- **Live Gold output:** `s3a://urbanflow/gold/live_zone_metrics/` (append mode, Parquet format).
- **Streaming checkpoint:** `s3a://urbanflow/checkpoints/live_trip_metrics/`.
  - *Critical constraint:* Checkpoint directory must NEVER match or reside beneath Live Gold paths.

## Core invariants and calculation rules

1. **Tumbling event-time windows:** Exactly 5-minute clock-aligned tumbling event-time windows (`F.window("event_timestamp", "5 minutes")`, grouped by `pickup_zone_id`).
2. **Timezone:** Session timezone must be `America/New_York`. Window boundaries are derived with explicit New York local clock semantics.
3. **Lookup keys (in America/New_York):**
   - `day_of_week`: 1 = Monday ... 7 = Sunday (`F.weekday(window_start) + 1`)
   - `hour`: 0 .. 23 (`F.hour(window_start)`)
   - `five_minute_slot`: 0 .. 11 (`floor(minute(window_start) / 5)`)
4. **Historical baseline join:** Broadcast `left_outer` join on `[pickup_zone_id, day_of_week, hour, five_minute_slot]`. Current streaming windows are ALWAYS preserved even if the historical baseline row is missing.
5. **Formulas & Null rules:**
   - `demand_ratio = current_trip_count / historical_mean_trip_count` (MUST be `null` if historical mean is null or `<= 0`).
   - `z_score = (current_trip_count - historical_mean_trip_count) / historical_std_trip_count` (MUST be `null` if historical std is null or `<= 0`).
   - `HIGH_DEMAND_RATIO = 1.5`
6. **Visible rejection:** Malformed Kafka JSON messages (missing required fields, negative values, missing explicit RFC 3339 timezone offset, invalid types) MUST be routed visibly to the `invalid_events` sink with `raw_value` and `validation_error`. Never silently drop events.

## Code organization

- `spark/live_trip_metrics.py` — Structured Streaming runner with Kafka consumer, window aggregation, and S3 Parquet append sink.
- `spark/event_schema.py` — Kafka JSON event parsing, schema enforcement, regex offset verification, and invalid event separation.
- `spark/historical_lookup.py` — Historical Gold baseline loader (Parquet/JSON) and broadcast join validation.
- `analytics_app/app.py` — Streamlit live mobility analytics dashboard (4-layer performance-optimized UI).
- `analytics_app/data_access.py` — Resilient S3 reader with in-memory delta file cache (`_S3_FILE_CACHE`) and view preparation.
- `analytics_app/zones.py` — Zone lookup loader, GeoJSON polygon loader with coordinate precision optimization, and demand property painting.
- `serving/` — REST API / serving endpoints for live metrics.
- `config/` — Profile YAML configurations (`settings.example.yaml`, `settings.fixture.example.yaml`, `settings.integrated.yaml`).
- `tests/` — Unit, contract, and integration tests.

## UI performance standards (Analytics App)

The Streamlit UI auto-refreshes every 10 seconds. To maintain smooth 60 FPS in the browser without freezing WebGL or crashing memory:

1. **Lazy view navigation:** Use active tab branching (`if active_tab == ...`) instead of eager `st.tabs`. Only compute and transmit elements for the currently visible tab.
2. **Conditional map caching:** Cache `nyc_map` in `st.session_state` keyed on `(window_start, selected_zones)`. Do NOT re-render or re-transmit the 9.6 MB PyDeck map on 10s ticks unless the underlying data window has advanced.
3. **GeoJSON coordinate precision:** Keep GeoJSON coordinates rounded to 4 decimal places in `load_taxi_zone_geojson()` with `@lru_cache(maxsize=1)`.
4. **Collapsible tables:** Wrap data tables in `st.expander("📋 ...", expanded=False)` with a 500-row safety limit for in-DOM display.

## Required validation

From the repository root:

```bash
# 1. Foundation fixture validation
docker compose run --rm foundation-check

# 2. Python unit & contract tests
uv run --project urbanflow-streaming pytest

# 3. Analytics smoke test
uv run --project urbanflow-streaming python urbanflow-streaming/tests/verify_analytics_smoke.py

# 4. End-to-end streaming job execution
docker compose run --rm spark-submit \
  --master 'local[2]' \
  /workspace/urbanflow-streaming/spark/live_trip_metrics.py \
  --config /workspace/urbanflow-streaming/config/settings.integrated.yaml
```

Expected terminal markers:

```text
FOUNDATION FIXTURES = PASS
9 passed in ...s
ANALYTICS SMOKE = PASS
```
