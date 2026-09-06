# Time Semantics

**Foundation version:** 1.0.0

## Canonical analytics time zone

```text
America/New_York
```

All time-derived analytical fields, window boundaries, and day-of-week calculations use this zone.

Producer and consumer machines **must not** infer event-time meaning from their host time zone.

## Spark session timezone

```text
spark.sql.session.timeZone = America/New_York
```

## Kafka event time

`event_time` in Kafka trip events represents **pickup time** and **must** be RFC 3339 with an explicit UTC offset.

Example:

```text
2025-01-06T08:21:44-05:00
```

The offset is part of the value. Consumers parse the offset from the string rather than assuming a fixed offset.

## Derived time fields

These fields are derived from `pickup_datetime` (Silver) or `event_time` (Kafka), interpreted in `America/New_York`:

| Field | Definition | Range |
|---|---|---|
| `year` | Calendar year | e.g. 2023, 2024, 2025 |
| `month` | Calendar month | 1..12 |
| `day_of_week` | ISO weekday | Monday=1 .. Sunday=7 |
| `hour` | Hour of day | 0..23 |
| `five_minute_slot` | `floor(minute / 5)` | 0..11 |

## Five-minute windows

Streaming windows are **tumbling** five-minute intervals aligned to clock minutes:

```text
08:00:00 – 08:04:59  →  slot 0
08:05:00 – 08:09:59  →  slot 1
08:10:00 – 08:14:59  →  slot 2
...
08:55:00 – 08:59:59  →  slot 11
```

The `window_start` and `window_end` in the live metrics contract use the window boundaries (e.g. `08:00:00` and `08:05:00`).

## Historical baseline join key

When the streaming subsystem joins current window counts against the historical baseline, it derives the join key from `window_start` in `America/New_York`:

```text
day_of_week  = ISO weekday of window_start
hour         = hour of window_start
five_minute_slot = floor(minute of window_start / 5)
```

This ensures the streaming and batch subsystems agree on the meaning of each time slot.
