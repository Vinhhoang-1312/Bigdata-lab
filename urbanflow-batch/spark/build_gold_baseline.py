#!/usr/bin/env python3
"""Build Foundation-compliant historical Gold from canonical Silver."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F

from batch.baseline import build_historical_baseline
from batch.baseline_validation import assert_historical_baseline_contract
from batch.io import read_zone_lookup
from batch.schemas import HISTORICAL_BASELINE_FIELDS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-path", default="s3a://urbanflow/silver/trips/")
    parser.add_argument(
        "--zone-lookup-path", default="s3a://urbanflow/bronze/zone_lookup/"
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument(
        "--output-path",
        default="s3a://urbanflow/gold/historical_zone_baseline/",
    )
    parser.add_argument("--write-mode", default="overwrite")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("UrbanFlow Historical Gold").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    actual_tz = spark.conf.get("spark.sql.session.timeZone")
    if actual_tz != "America/New_York":
        raise AssertionError(f"Spark session timezone must be America/New_York, got {actual_tz}")
    spark.sparkContext.setLogLevel("WARN")
    silver = spark.read.parquet(args.silver_path)
    zones = read_zone_lookup(spark, args.zone_lookup_path).cache()
    expected_zone_count = (
        zones.select(F.expr("try_cast(LocationID as int)").alias("zone_id"))
        .where(F.col("zone_id").isNotNull())
        .distinct()
        .count()
    )
    require_full_grid = (
        args.start_date == "2023-01-01" and args.end_date == "2023-01-31"
    )
    baseline = build_historical_baseline(
        silver, zones, args.start_date, args.end_date
    ).cache()
    assert_historical_baseline_contract(
        baseline,
        expected_zone_count=expected_zone_count,
        require_full_grid=require_full_grid,
    )
    rows = baseline.count()
    baseline.write.mode(args.write_mode).parquet(args.output_path)

    persisted = spark.read.parquet(args.output_path).select(
        *HISTORICAL_BASELINE_FIELDS
    )
    assert_historical_baseline_contract(
        persisted,
        expected_zone_count=expected_zone_count,
        require_full_grid=require_full_grid,
    )
    persisted_rows = persisted.count()
    if persisted_rows != rows:
        raise AssertionError(
            f"Historical Gold read-back differs: wrote {rows}, read {persisted_rows}"
        )
    print(f"historical_start={args.start_date}")
    print(f"historical_end={args.end_date}")
    print(f"historical_rows={rows}")
    print(f"historical_readback_rows={persisted_rows}")
    print(f"historical_output={args.output_path}")
    print("HISTORICAL GOLD = PASS")
    baseline.unpersist()
    zones.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
