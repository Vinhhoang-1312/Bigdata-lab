#!/usr/bin/env python3
"""Land original NYC TLC files in Foundation Bronze S3A paths."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession

TRIP_NAME = "yellow_tripdata_{period}.parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--period", action="append", required=True, help="YYYY-MM")
    parser.add_argument("--source-dir", default="/workspace/local-data/nyc-tlc")
    parser.add_argument("--bronze-trips", default="s3a://urbanflow/bronze/trips/")
    parser.add_argument(
        "--bronze-zone-lookup", default="s3a://urbanflow/bronze/zone_lookup/"
    )
    return parser.parse_args()


def copy_local_file(spark: SparkSession, source: Path, destination: str) -> None:
    if not source.is_file():
        raise FileNotFoundError(source)
    jvm = spark.sparkContext._jvm
    hadoop = spark.sparkContext._jsc.hadoopConfiguration()
    local_fs = jvm.org.apache.hadoop.fs.FileSystem.getLocal(hadoop)
    destination_path = jvm.org.apache.hadoop.fs.Path(destination)
    destination_fs = destination_path.getFileSystem(hadoop)
    destination_fs.mkdirs(destination_path.getParent())
    destination_fs.copyFromLocalFile(
        False,
        True,
        jvm.org.apache.hadoop.fs.Path(source.resolve().as_uri()),
        destination_path,
    )
    if not destination_fs.exists(destination_path):
        raise AssertionError(f"Bronze copy missing after write: {destination}")
    destination_size = destination_fs.getFileStatus(destination_path).getLen()
    if destination_size != source.stat().st_size:
        raise AssertionError(
            f"Bronze copy size differs for {destination}: "
            f"local={source.stat().st_size}, bronze={destination_size}"
        )


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("UrbanFlow Bronze Landing").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    source_dir = Path(args.source_dir)

    total_rows = 0
    total_bytes = 0
    for period in sorted(set(args.period)):
        year_text, month_text = period.split("-", maxsplit=1)
        year, month = int(year_text), int(month_text)
        source = source_dir / TRIP_NAME.format(period=period)
        destination = (
            f"{args.bronze_trips.rstrip('/')}/year={year}/month={month}/"
            f"{source.name}"
        )
        rows = spark.read.parquet(str(source)).count()
        copy_local_file(spark, source, destination)
        total_rows += rows
        total_bytes += source.stat().st_size
        print(f"landed_period[{period}].rows={rows}")
        print(f"landed_period[{period}].bytes={source.stat().st_size}")
        print(f"landed_period[{period}].path={destination}")

    zone_source = source_dir / "taxi_zone_lookup.csv"
    zone_destination = (
        f"{args.bronze_zone_lookup.rstrip('/')}/taxi_zone_lookup.csv"
    )
    zone_rows = spark.read.option("header", True).csv(str(zone_source)).count()
    copy_local_file(spark, zone_source, zone_destination)

    bronze_rows = spark.read.parquet(args.bronze_trips).count()
    if bronze_rows != total_rows:
        raise AssertionError(
            f"Bronze row count differs: local={total_rows}, bronze={bronze_rows}"
        )
    print(f"source_files={len(set(args.period)) + 1}")
    print(f"source_rows={total_rows}")
    print(f"source_bytes={total_bytes + zone_source.stat().st_size}")
    print(f"zone_rows={zone_rows}")
    print(f"bronze_rows={bronze_rows}")
    print("BRONZE LANDING = PASS")
    spark.stop()


if __name__ == "__main__":
    main()
