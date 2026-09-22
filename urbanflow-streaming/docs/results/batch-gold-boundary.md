# Batch Historical Gold Boundary Result

Date: 2026-09-20T13:17:00Z
Status: `PASS`
Path: `s3a://urbanflow/gold/historical_zone_baseline/`
Batch source commit: `unknown`

## Summary

- Reason: Historical Gold loaded successfully.
- Row count: `534240`
- Unique key count: `534240`
- Duplicate key count: `0`

## Schema

| name | type | nullable |
|---|---|---|
| `pickup_zone_id` | `int` | `True` |
| `day_of_week` | `int` | `True` |
| `hour` | `int` | `True` |
| `five_minute_slot` | `int` | `True` |
| `historical_mean_trip_count` | `double` | `True` |
| `historical_std_trip_count` | `double` | `True` |
| `historical_mean_distance` | `double` | `True` |
| `historical_mean_duration` | `double` | `True` |

## Sample Join

```json
{
  "current_trip_count": 1,
  "demand_ratio": 5.0,
  "historical_mean_trip_count": 0.2,
  "pickup_zone_id": 1,
  "window_start": "2025-01-06T09:15:00",
  "z_score": 2.0
}
```

## Notes

- This verifier reads the canonical S3A path directly.
- It does not import Batch application source code.
- It validates Streaming can consume Historical Gold without an adapter.
