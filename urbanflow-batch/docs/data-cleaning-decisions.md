# Batch Data-Cleaning Decisions

**Foundation baseline:** 1.0.0
**Inspected data:** NYC TLC Yellow Taxi, January and February 2023

## Applied rules

Only Foundation validity rules are applied:

- pickup and dropoff timestamps must parse in `America/New_York`;
- spring-forward nonexistent local pickup times are rejected;
- dropoff must not precede pickup;
- pickup and dropoff zones must exist in the TLC zone lookup;
- distance, fare, optional passenger count, and optional total amount must be
  non-negative when required/present;
- required numeric fields must parse;
- optional blank values remain null, while malformed optional values reject.

Every rejected row retains source values and one or more visible reason codes.

## Real-data observations

The January and February monthly files differ physically:
`passenger_count` is represented as double in January and INT64 in February.
The Parquet input adapter reads each monthly file separately, casts source
columns to a stable source-level representation, and unions by name. This is a
physical compatibility normalization, not a change to Silver semantics.

Across both files:

```text
source_rows=5,980,721
valid_rows=5,930,325
rejected_rows=50,396
dropoff_before_pickup=279
fare_amount_invalid=49,813
total_amount_invalid=50,104
```

Reason counts overlap because a row may violate more than one rule.

## Decisions not added

No evidence-based additional cleaning threshold was added. In particular, the
pipeline does not invent maximum distance, duration, fare, or passenger limits,
and it does not silently deduplicate source rows. Such rules require a separate
documented decision and Foundation impact review when applicable.
