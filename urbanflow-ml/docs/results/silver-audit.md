# ML Silver Audit

Date: 2026-09-14T13:21:29Z
Status: `FAIL`
Silver path: `s3a://urbanflow/silver/trips/`

## Summary

- Total rows: `5930325`
- Pickup datetime min: `2009-01-01 04:01:42`
- Pickup datetime max: `2023-03-07 18:01:28`
- Required months: `2022-08, 2022-09, 2022-10, 2022-11, 2022-12, 2023-01, 2023-02`
- Observed months: `2008-12, 2009-01, 2022-10, 2022-12, 2023-01, 2023-02, 2023-03`
- Missing months: `2022-08, 2022-09, 2022-11`

## Monthly Rows

| Month | Rows |
|---|---:|
| `2008-12` | `6` |
| `2009-01` | `1` |
| `2022-10` | `11` |
| `2022-12` | `25` |
| `2023-01` | `3041511` |
| `2023-02` | `2888729` |
| `2023-03` | `42` |

## Failed Reasons

```json
[
  "Missing required months: ['2022-08', '2022-09', '2022-11']"
]
```

## Null Counts

```json
{
  "day_of_week": 0,
  "dropoff_datetime": 0,
  "dropoff_zone_id": 0,
  "fare_amount": 0,
  "five_minute_slot": 0,
  "hour": 0,
  "month": 0,
  "pickup_datetime": 0,
  "pickup_zone_id": 0,
  "trip_distance": 0,
  "trip_duration_minutes": 0,
  "year": 0
}
```

## Invalid Range Counts

```json
{
  "dropoff_before_pickup": 0,
  "negative_fare_amount": 0,
  "negative_trip_distance": 0,
  "negative_trip_duration": 0
}
```
