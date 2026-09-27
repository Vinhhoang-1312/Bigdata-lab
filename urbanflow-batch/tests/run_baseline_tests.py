#!/usr/bin/env python3
"""Contract tests for the zero-inclusive historical Gold baseline."""

from __future__ import annotations

import math
import shutil
import tempfile

from pyspark.sql import SparkSession, functions as F

from batch.baseline import build_historical_baseline
from batch.baseline_validation import assert_historical_baseline_contract
from batch.io import read_source_trips, read_zone_lookup
from batch.schemas import HISTORICAL_BASELINE_FIELDS
from batch.transforms import bronze_to_silver

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = str(ROOT / "urbanflow-foundation/fixtures/bronze/source_trips.csv")
ZONES = str(ROOT / "urbanflow-foundation/fixtures/bronze/zone_lookup.csv")


def main() -> None:
    spark = SparkSession.builder.appName("UrbanFlow Historical Baseline Tests").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    source = read_source_trips(spark, SOURCE, "csv")
    zones = read_zone_lookup(spark, ZONES)
    expected_zone_count = (
        zones.select(F.expr("try_cast(LocationID as int)").alias("zone_id"))
        .where(F.col("zone_id").isNotNull())
        .distinct()
        .count()
    )
    silver = bronze_to_silver(source, zones).silver

    baseline = build_historical_baseline(
        silver, zones, "2024-03-04", "2024-03-11"
    ).cache()
    assert_historical_baseline_contract(
        baseline, expected_zone_count=expected_zone_count
    )
    assert baseline.count() == 16032

    zone_161 = baseline.where(
        (F.col("pickup_zone_id") == 161)
        & (F.col("day_of_week") == 1)
        & (F.col("hour") == 8)
        & (F.col("five_minute_slot") == 0)
    ).first()
    assert math.isclose(zone_161.historical_mean_trip_count, 2.5)
    assert math.isclose(zone_161.historical_std_trip_count, 0.5)
    assert math.isclose(zone_161.historical_mean_distance, 2.74)

    zone_236 = baseline.where(
        (F.col("pickup_zone_id") == 236)
        & (F.col("day_of_week") == 1)
        & (F.col("hour") == 8)
        & (F.col("five_minute_slot") == 0)
    ).first()
    assert math.isclose(zone_236.historical_mean_trip_count, 1.0)
    assert math.isclose(zone_236.historical_std_trip_count, 0.0)

    zero_key = baseline.where(
        (F.col("pickup_zone_id") == 48)
        & (F.col("day_of_week") == 1)
        & (F.col("hour") == 8)
        & (F.col("five_minute_slot") == 0)
    ).first()
    assert math.isclose(zero_key.historical_mean_trip_count, 0.0)
    assert math.isclose(zero_key.historical_std_trip_count, 0.0)
    assert zero_key.historical_mean_distance is None
    assert zero_key.historical_mean_duration is None

    spring_gap = baseline.where(
        (F.col("day_of_week") == 7) & (F.col("hour") == 2)
    ).count()
    assert spring_gap == 0

    output = tempfile.mkdtemp(prefix="urbanflow-baseline-")
    try:
        for _ in range(2):
            baseline.write.mode("overwrite").parquet(output)
            persisted = spark.read.parquet(output).select(*HISTORICAL_BASELINE_FIELDS)
            assert_historical_baseline_contract(
                persisted, expected_zone_count=expected_zone_count
            )
            assert persisted.count() == 16032
    finally:
        shutil.rmtree(output, ignore_errors=True)

    # Negative tests: missing zones and uneven slot coverage are visible.
    missing_zone = baseline.where(F.col("pickup_zone_id") != 161)
    try:
        assert_historical_baseline_contract(
            missing_zone, expected_zone_count=expected_zone_count
        )
        raise AssertionError("Expected missing-zone validation to fail")
    except AssertionError as err:
        assert "zone count differs" in str(err)

    missing_slot = baseline.where(
        ~(
            (F.col("pickup_zone_id") == 161)
            & (F.col("day_of_week") == 1)
            & (F.col("hour") == 8)
            & (F.col("five_minute_slot") == 0)
        )
    )
    try:
        assert_historical_baseline_contract(missing_slot)
        raise AssertionError("Expected uneven-slot validation to fail")
    except AssertionError as err:
        assert "uneven slot counts" in str(err)

    # require_full_grid fails on a DST transition week (2,004 != 2,016).
    try:
        assert_historical_baseline_contract(baseline, require_full_grid=True)
        raise AssertionError("Expected require_full_grid check to fail on DST baseline")
    except AssertionError as err:
        assert "complete 2,016 slot grid coverage" in str(err)

    # Negative test: invalid non-finite value triggers assertion
    bad_baseline = baseline.withColumn(
        "historical_mean_trip_count",
        F.when(F.col("pickup_zone_id") == 161, F.lit(float("nan"))).otherwise(
            F.col("historical_mean_trip_count")
        ),
    )
    try:
        assert_historical_baseline_contract(bad_baseline)
        raise AssertionError("Expected non-finite check to fail")
    except AssertionError as err:
        assert "invalid contract values" in str(err)

    baseline.unpersist()
    spark.stop()
    print("ZERO COUNTS = PASS")
    print("STDDEV_POP = PASS")
    print("DST ELIGIBILITY = PASS")
    print("HISTORICAL BASELINE TESTS = PASS")


if __name__ == "__main__":
    main()
