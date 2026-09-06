# NYC TLC January 2023 Batch Smoke Result

This records the first real-data compatibility run. The subsequent two-month
inspection selected January 2023 as history and February 2023 as replay in the
Foundation period lock.

## Input

- Dataset: NYC TLC Yellow Taxi January 2023 Parquet
- Source URL: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet`
- Source files: 1
- Source size: 47,673,370 bytes
- Zone lookup rows: 265 plus header
- Execution: Spark 4.2.0 standalone master with two workers and four total cores

## Result

```text
source_rows=3066766
valid_rows=3041551
rejected_rows=25215
silver_rows=3041551
silver_readback_rows=3041551
rejection_reason[dropoff_before_pickup]=3
rejection_reason[fare_amount_invalid]=25049
rejection_reason[total_amount_invalid]=25204
BRONZE TO SILVER = PASS
```

A row may have more than one rejection reason, so reason counts do not sum to
the rejected-row count.

- Silver Parquet objects: 6
- Silver Parquet size: 66,714,508 bytes
- Rejected JSON objects: 3
- Rejected JSON size: 7,280,812 bytes
- Wall-clock runtime: 3 minutes 21 seconds

## Decision

The real Parquet schema is compatible with the existing input adapter and no
integration-only cast or rename was required. The observed negative fare/total
values and three reversed timestamp rows are rejected by existing Foundation
rules. No additional cleaning rule is introduced from this smoke test.
