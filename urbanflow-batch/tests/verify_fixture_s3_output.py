#!/usr/bin/env python3
"""Verify persisted fixture-mode Silver Parquet against Foundation JSONL."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from batch.schemas import SILVER_FIELDS
from batch.validation import assert_silver_contract

EXPECTED_PATH = (
    Path(__file__).resolve().parents[2]
    / "urbanflow-foundation/fixtures/silver/expected_silver_from_bronze.jsonl"
)
TIMESTAMP_OUTPUT_PATTERN = "yyyy-MM-dd'T'HH:mm:ssXXX"
FLOAT_FIELDS = {
    "trip_distance",
    "fare_amount",
    "total_amount",
    "trip_duration_minutes",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--silver-path",
        default="s3a://urbanflow/dev/batch/silver/trips/",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("Verify Batch Fixture S3 Output").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.parquet(args.silver_path).select(*SILVER_FIELDS)
    assert_silver_contract(silver)
    actual = [
        row.asDict(recursive=True)
        for row in silver.select(
            F.date_format("pickup_datetime", TIMESTAMP_OUTPUT_PATTERN).alias(
                "pickup_datetime"
            ),
            F.date_format("dropoff_datetime", TIMESTAMP_OUTPUT_PATTERN).alias(
                "dropoff_datetime"
            ),
            *[name for name in SILVER_FIELDS if not name.endswith("_datetime")],
        ).collect()
    ]
    with EXPECTED_PATH.open(encoding="utf-8") as handle:
        expected = [json.loads(line) for line in handle if line.strip()]

    key = lambda row: (
        row["pickup_datetime"],
        row["dropoff_datetime"],
        row["pickup_zone_id"],
    )
    actual.sort(key=key)
    expected.sort(key=key)
    assert len(actual) == len(expected) == 10
    for actual_row, expected_row in zip(actual, expected, strict=True):
        assert actual_row.keys() == expected_row.keys()
        for field, expected_value in expected_row.items():
            actual_value = actual_row[field]
            if field in FLOAT_FIELDS and expected_value is not None:
                assert math.isclose(actual_value, expected_value, abs_tol=1e-4), (
                    field,
                    actual_value,
                    expected_value,
                )
            else:
                assert actual_value == expected_value, (
                    field,
                    actual_value,
                    expected_value,
                )

    print(f"SILVER_PATH = {args.silver_path}")
    print("SILVER_ROWS = 10")
    print("PERSISTED FIXTURE SILVER = PASS")
    spark.stop()


if __name__ == "__main__":
    main()
