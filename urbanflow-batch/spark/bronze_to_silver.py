#!/usr/bin/env python3
"""Run the UrbanFlow Bronze-to-Silver Spark job."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F

from batch.config import load_settings
from batch.io import read_source_trips, read_zone_lookup
from batch.transforms import bronze_to_silver
from batch.validation import assert_silver_contract


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default="/workspace/urbanflow-batch/config/settings.fixture.json",
        help="Path to a Batch JSON settings file",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings(args.config)
    spark = SparkSession.builder.appName("UrbanFlow Bronze to Silver").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", settings.analytics_timezone)
    spark.sparkContext.setLogLevel("WARN")

    source = read_source_trips(
        spark, settings.source_trips_path, settings.source_trips_format
    )
    zones = read_zone_lookup(spark, settings.zone_lookup_path)
    result = bronze_to_silver(source, zones)
    result.silver.cache()
    result.rejected.cache()

    source_rows = source.count()
    silver_rows = result.silver.count()
    rejected_rows = result.rejected.count()
    if source_rows != silver_rows + rejected_rows:
        raise AssertionError("Accepted and rejected row counts do not match source count")

    assert_silver_contract(result.silver)
    (
        result.silver.write.mode(settings.write_mode)
        .partitionBy("year", "month")
        .parquet(settings.silver_output_path)
    )
    result.rejected.write.mode(settings.write_mode).json(
        settings.rejected_output_path
    )

    persisted_silver = spark.read.parquet(settings.silver_output_path).select(
        *result.silver.columns
    )
    assert_silver_contract(persisted_silver)
    persisted_rows = persisted_silver.count()
    if persisted_rows != silver_rows:
        raise AssertionError(
            f"Silver read-back count differs: wrote {silver_rows}, read {persisted_rows}"
        )

    reason_counts = (
        result.rejected.select(F.explode("rejection_reasons").alias("reason"))
        .groupBy("reason")
        .count()
        .orderBy("reason")
        .collect()
    )
    print(f"source_rows={source_rows}")
    print(f"valid_rows={silver_rows}")
    print(f"rejected_rows={rejected_rows}")
    print(f"silver_rows={silver_rows}")
    print(f"silver_readback_rows={persisted_rows}")
    for row in reason_counts:
        print(f"rejection_reason[{row.reason}]={row['count']}")
    print(f"silver_output={settings.silver_output_path}")
    print(f"rejected_output={settings.rejected_output_path}")
    print("BRONZE TO SILVER = PASS")

    result.silver.unpersist()
    result.rejected.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
