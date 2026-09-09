# UrbanFlow streaming dashboard — demo UI

**Goal:** Streamlit app a defense audience can read without opening code: New York time, named taxi zones, map, charts that still render with one fixture window.

## Defaults

- Display timezone: `America/New_York` (Foundation). Never browser-local.
- Zone labels from `urbanflow-foundation/fixtures/bronze/zone_lookup.csv`.
- High demand: `demand_ratio > 1.5`.
- One time window → grouped/bar charts (not line). Two+ windows → line in NY time.
- Map: pydeck scatter on NYC (centroids), color by demand ratio.
- File paths stay in a sidebar expander. Auto-refresh ~5s.
- App still only *reads* Gold; no ETL.

## Files

- `analytics_app/data_access.py` — view helpers
- `analytics_app/zones.py` — lookup + centroids
- `analytics_app/app.py` — UI
- `tests/test_data_access.py` — view helpers
