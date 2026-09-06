# Two-Month Batch Volume Result

## Dataset and environment

- NYC TLC Yellow Taxi: January and February 2023
- Source trip files: 2, plus one zone lookup
- Source trip rows: 5,980,721
- Local source bytes including lookup: 95,433,713
- Spark: 4.2.0, standalone master
- Workers: 2
- Total worker cores: 4
- Total configured worker memory: 1,536 MiB
- Storage: SeaweedFS 4.45 through Hadoop S3A 3.5.0

## Bronze

```text
bronze_rows=5,980,721
trip objects including directory markers=4
trip object bytes=95,421,382
landing runtime=29 seconds
```

The original monthly Parquet files are copied byte-for-byte into year/month
Bronze paths; the object total above excludes the zone lookup path.

## Silver

```text
source_rows=5,980,721
silver_rows=5,930,325
rejected_rows=50,396
Silver objects=11
Silver bytes=129,457,390
Bronze-to-Silver runtime=3 minutes 38 seconds
```

The Silver output is partitioned by derived `year` and `month` and was read back
from S3A with the same row count and Foundation schema.

## Historical Gold

```text
historical period=2023-01-01 through 2023-01-31
historical rows=534,240
Historical Gold objects=49
Historical Gold bytes=3,651,690
baseline runtime=1 minute 40 seconds
```

Historical Gold includes eligible zero-count zone/date/slot combinations before
mean and population-standard-deviation aggregation.
