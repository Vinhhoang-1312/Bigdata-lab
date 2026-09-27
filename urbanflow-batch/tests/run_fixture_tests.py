#!/usr/bin/env python3
"""Fixture contract tests for UrbanFlow Batch Phase 1."""

from __future__ import annotations

import json
import math
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from batch.config import load_settings
from batch.io import read_source_trips, read_zone_lookup
from batch.schemas import (
    SILVER_FIELDS,
    SILVER_TYPES,
    SOURCE_COLUMNS,
    SOURCE_CSV_SCHEMA,
)
from batch.transforms import bronze_to_silver
from batch.validation import assert_silver_contract

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "urbanflow-foundation/fixtures/bronze/source_trips.csv"
ZONES = ROOT / "urbanflow-foundation/fixtures/bronze/zone_lookup.csv"
EXPECTED = ROOT / "urbanflow-foundation/fixtures/silver/expected_silver_from_bronze.jsonl"
TIMESTAMP_OUTPUT_PATTERN = "yyyy-MM-dd'T'HH:mm:ssXXX"
FLOAT_FIELDS = {
    "trip_distance",
    "fare_amount",
    "total_amount",
    "trip_duration_minutes",
}


def load_expected() -> list[dict]:
    with EXPECTED.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def actual_as_dicts(silver) -> list[dict]:
    projected = silver.select(
        F.date_format("pickup_datetime", TIMESTAMP_OUTPUT_PATTERN).alias(
            "pickup_datetime"
        ),
        F.date_format("dropoff_datetime", TIMESTAMP_OUTPUT_PATTERN).alias(
            "dropoff_datetime"
        ),
        *[name for name in SILVER_FIELDS if not name.endswith("_datetime")],
    )
    return [row.asDict(recursive=True) for row in projected.collect()]


def assert_records_equal(actual: list[dict], expected: list[dict]) -> None:
    def sort_key(row: dict) -> tuple:
        return (
            row["pickup_datetime"],
            row["dropoff_datetime"],
            row["pickup_zone_id"],
        )

    actual = sorted(actual, key=sort_key)
    expected = sorted(expected, key=sort_key)
    assert len(actual) == len(expected)
    for actual_row, expected_row in zip(actual, expected, strict=True):
        assert actual_row.keys() == expected_row.keys()
        for field in actual_row:
            if field in FLOAT_FIELDS and expected_row[field] is not None:
                assert math.isclose(
                    actual_row[field], expected_row[field], abs_tol=1e-4
                ), (field, actual_row[field], expected_row[field])
            else:
                assert actual_row[field] == expected_row[field], (
                    field,
                    actual_row[field],
                    expected_row[field],
                )


def test_optional_numeric_handling(spark: SparkSession, zones) -> None:
    rows = [
        (
            "2024-03-04 10:00:00",
            "2024-03-04 10:10:00",
            "161",
            "236",
            "1.0",
            None,
            "10.0",
            None,
        ),
        (
            "2024-03-04 10:15:00",
            "2024-03-04 10:25:00",
            "161",
            "236",
            "1.0",
            "not-a-number",
            "10.0",
            "12.0",
        ),
        (
            "2024-03-04 10:30:00",
            "2024-03-04 10:40:00",
            "161",
            "236",
            "1.0",
            "1",
            "10.0",
            "not-a-number",
        ),
    ]
    result = bronze_to_silver(spark.createDataFrame(rows, SOURCE_CSV_SCHEMA), zones)
    assert result.silver.count() == 1
    nullable_row = result.silver.first()
    assert nullable_row.passenger_count is None
    assert nullable_row.total_amount is None
    rejected_rows = result.rejected.select(
        "passenger_count", "total_amount", "rejection_reasons"
    ).collect()
    reasons = {
        reason
        for row in rejected_rows
        for reason in row.rejection_reasons
    }
    assert reasons == {"passenger_count_invalid", "total_amount_invalid"}, reasons
    assert {row.passenger_count for row in rejected_rows} >= {"not-a-number"}
    assert {row.total_amount for row in rejected_rows} >= {"not-a-number"}


def test_all_rejection_rules(spark: SparkSession, zones) -> None:
    valid = [
        "2024-03-04 11:00:00",
        "2024-03-04 11:10:00",
        "161",
        "236",
        "1.0",
        "1",
        "10.0",
        "12.0",
    ]
    cases = {
        "pickup_datetime_invalid": (0, None),
        "dropoff_datetime_invalid": (1, None),
        "dropoff_before_pickup": (1, "2024-03-04 10:59:00"),
        "pickup_zone_invalid": (2, "999"),
        "dropoff_zone_invalid": (3, "999"),
        "trip_distance_invalid": (4, None),
        "passenger_count_invalid": (5, "-1"),
        "fare_amount_invalid": (6, None),
        "total_amount_invalid": (7, "-1"),
    }
    rows = []
    for index, replacement in cases.values():
        row = valid.copy()
        row[index] = replacement
        rows.append(tuple(row))

    result = bronze_to_silver(spark.createDataFrame(rows, SOURCE_CSV_SCHEMA), zones)
    assert result.silver.count() == 0
    assert result.rejected.count() == len(cases)
    reason_counts = {
        row.reason: row["count"]
        for row in (
            result.rejected.select(F.explode("rejection_reasons").alias("reason"))
            .groupBy("reason")
            .count()
            .collect()
        )
    }
    assert reason_counts == {reason: 1 for reason in cases}, reason_counts


def test_dst_edge_cases(spark: SparkSession, zones) -> None:
    rows = [
        (
            "2024-03-10 02:30:00",
            "2024-03-10 03:40:00",
            "161",
            "236",
            "1.0",
            "1",
            "10.0",
            "12.0",
        ),
        (
            "2024-11-03 01:30:00",
            "2024-11-03 01:45:00",
            "161",
            "236",
            "1.0",
            "1",
            "10.0",
            "12.0",
        ),
    ]
    source = spark.createDataFrame(rows, SOURCE_CSV_SCHEMA)
    result = bronze_to_silver(source, zones)
    assert result.silver.count() == 1
    assert result.rejected.count() == 1
    assert result.rejected.first().rejection_reasons == ["pickup_datetime_invalid"]
    fall_back_offset = result.silver.select(
        F.date_format("pickup_datetime", TIMESTAMP_OUTPUT_PATTERN).alias("value")
    ).first().value
    assert fall_back_offset == "2024-11-03T01:30:00-04:00", fall_back_offset


def test_parquet_fractional_and_non_finite_rejection(spark: SparkSession, zones) -> None:
    """Parquet physical columns (double/float) must reject fractional integers and non-finite values."""
    rows = [
        # 1. Fractional PULocationID=161.5
        (datetime(2024, 3, 4, 12, 0), datetime(2024, 3, 4, 12, 10), 161.5, 236.0, 1.0, 1.0, 10.0, 12.0),
        # 2. Fractional DOLocationID=236.5
        (datetime(2024, 3, 4, 12, 0), datetime(2024, 3, 4, 12, 10), 161.0, 236.5, 1.0, 1.0, 10.0, 12.0),
        # 3. Fractional passenger_count=1.5
        (datetime(2024, 3, 4, 12, 0), datetime(2024, 3, 4, 12, 10), 161.0, 236.0, 1.0, 1.5, 10.0, 12.0),
        # 4. NaN trip_distance
        (datetime(2024, 3, 4, 12, 0), datetime(2024, 3, 4, 12, 10), 161.0, 236.0, float("nan"), 1.0, 10.0, 12.0),
        # 5. Infinity fare_amount
        (datetime(2024, 3, 4, 12, 0), datetime(2024, 3, 4, 12, 10), 161.0, 236.0, 1.0, 1.0, float("inf"), 12.0),
        # 6. Negative infinity total_amount
        (datetime(2024, 3, 4, 12, 0), datetime(2024, 3, 4, 12, 10), 161.0, 236.0, 1.0, 1.0, 10.0, float("-inf")),
    ]
    parquet_df = spark.createDataFrame(rows, SOURCE_COLUMNS)
    result = bronze_to_silver(parquet_df, zones)
    assert result.silver.count() == 0, f"Expected 0 silver rows, got {result.silver.count()}"
    assert result.rejected.count() == 6, f"Expected 6 rejected rows, got {result.rejected.count()}"

    reasons_list = [r.rejection_reasons for r in result.rejected.collect()]
    assert any("pickup_zone_invalid" in r for r in reasons_list)
    assert any("dropoff_zone_invalid" in r for r in reasons_list)
    assert any("passenger_count_invalid" in r for r in reasons_list)
    assert any("trip_distance_invalid" in r for r in reasons_list)
    assert any("fare_amount_invalid" in r for r in reasons_list)
    assert any("total_amount_invalid" in r for r in reasons_list)


def main() -> None:
    spark = SparkSession.builder.appName("UrbanFlow Batch Fixture Tests").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert spark.conf.get("spark.sql.session.timeZone") == "America/New_York"

    source = read_source_trips(spark, str(SOURCE), "csv")
    zones = read_zone_lookup(spark, str(ZONES))
    result = bronze_to_silver(source, zones)
    result.silver.cache()
    result.rejected.cache()

    assert source.count() == 15
    assert result.silver.count() == 10
    assert result.rejected.count() == 5
    assert_silver_contract(result.silver, zones)
    invalid_zone_silver = result.silver.withColumn(
        "pickup_zone_id", F.lit(999).cast("int")
    )
    try:
        assert_silver_contract(invalid_zone_silver, zones)
        raise AssertionError("Expected unknown-zone validation to fail")
    except AssertionError as error:
        assert "not present in zone lookup" in str(error)
    assert tuple(result.silver.columns) == SILVER_FIELDS
    assert {
        field.name: field.dataType.simpleString() for field in result.silver.schema.fields
    } == SILVER_TYPES

    reason_counts = {
        row.reason: row["count"]
        for row in (
            result.rejected.select(F.explode("rejection_reasons").alias("reason"))
            .groupBy("reason")
            .count()
            .collect()
        )
    }
    assert reason_counts == {
        "pickup_datetime_invalid": 1,
        "dropoff_before_pickup": 1,
        "trip_distance_invalid": 1,
        "pickup_zone_invalid": 1,
        "fare_amount_invalid": 1,
    }, reason_counts

    expected = load_expected()
    assert_records_equal(actual_as_dicts(result.silver), expected)
    test_optional_numeric_handling(spark, zones)
    test_all_rejection_rules(spark, zones)
    test_dst_edge_cases(spark, zones)
    test_parquet_fractional_and_non_finite_rejection(spark, zones)

    temporary_root = tempfile.mkdtemp(prefix="urbanflow-batch-")
    output = str(Path(temporary_root) / "silver")
    parquet_source_path = str(Path(temporary_root) / "bronze-parquet")
    mixed_schema_path = Path(temporary_root) / "mixed-schema"
    try:
        source.write.mode("overwrite").parquet(parquet_source_path)
        parquet_source = read_source_trips(spark, parquet_source_path, "parquet")
        parquet_result = bronze_to_silver(parquet_source, zones)
        assert parquet_result.silver.count() == 10
        assert parquet_result.rejected.count() == 5
        assert_records_equal(actual_as_dicts(parquet_result.silver), expected)

        common_values = (
            datetime(2024, 3, 4, 12, 0),
            datetime(2024, 3, 4, 12, 10),
            161,
            236,
            1.0,
        )
        spark.createDataFrame(
            [(*common_values, 1.0, 10.0, 12.0)], SOURCE_COLUMNS
        ).write.parquet(str(mixed_schema_path / "double"))
        spark.createDataFrame(
            [(*common_values, 1, 10.0, 12.0)], SOURCE_COLUMNS
        ).write.parquet(str(mixed_schema_path / "long"))
        mixed_source = read_source_trips(spark, str(mixed_schema_path), "parquet")
        assert dict(mixed_source.dtypes)["passenger_count"] == "double"
        mixed_result = bronze_to_silver(mixed_source, zones)
        assert mixed_result.silver.count() == 2
        assert mixed_result.rejected.count() == 0

        for _ in range(2):
            result.silver.write.mode("overwrite").partitionBy(
                "year", "month"
            ).parquet(output)
            round_trip = spark.read.parquet(output).select(*SILVER_FIELDS)
            assert round_trip.count() == 10
            assert_silver_contract(round_trip, zones)
            assert_records_equal(actual_as_dicts(round_trip), expected)

        partition_dirs = {
            path.parent.name + "/" + path.name
            for path in Path(output).glob("year=*/month=*")
        }
        assert partition_dirs == {
            "year=2024/month=3",
            "year=2024/month=6",
            "year=2024/month=9",
        }, partition_dirs
    finally:
        shutil.rmtree(temporary_root, ignore_errors=True)

    result.silver.unpersist()
    result.rejected.unpersist()
    spark.stop()

    settings = load_settings(ROOT / "urbanflow-batch/config/settings.fixture.yaml")
    assert settings.foundation_version == "1.0.0"
    assert settings.analytics_timezone == "America/New_York"
    assert settings.source_trips_format == "csv"
    assert settings.write_mode == "overwrite"
    print("BATCH CONFIG LOADING = PASS")
    print("BATCH FIXTURE TESTS = PASS")


if __name__ == "__main__":
    main()
