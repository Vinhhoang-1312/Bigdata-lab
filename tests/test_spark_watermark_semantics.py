#!/usr/bin/env python3
"""Demonstrates and verifies Spark Structured Streaming watermark and tumbling window semantics.

Verifies:
1. Exact matching of watermark column and window timeColumn ("event_timestamp").
2. F.window positional signature with tumbling window (5 minutes).
3. Logical plan verification:
   - Tumbling window uses 300000000 us (5 min) modulo without sliding Expand operators.
   - Absence of 1-minute sliding interval (60000000 us word boundary match).
4. Direct API assertion:
   - aggregate_current() rejects slide_duration with ValueError to prevent contract drift.
"""

from __future__ import annotations

from pathlib import Path
import re
import sys

from pyspark.sql import SparkSession, functions as F

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "urbanflow-streaming"))

from spark.live_trip_metrics import aggregate_current  # noqa: E402


def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("WatermarkSemanticsTest")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def test_watermark_and_tumbling_window_semantics(spark: SparkSession) -> None:
    print("=== Testing Spark Watermark & Tumbling Window Consistency ===")

    # Use readStream so Spark generates a streaming logical plan with EventTimeWatermark
    stream = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    stream_events = stream.select(
        F.lit("e1").alias("event_id"),
        F.col("timestamp").alias("event_timestamp"),
        F.lit(161).alias("pickup_zone_id"),
    )

    # 1. Verify pipeline aggregate_current produces compliant tumbling window
    windowed = aggregate_current(stream_events, watermark="10 minutes", window_duration="5 minutes")

    # 2. Verify explicit slide_duration rejection prevents contract drift
    try:
        aggregate_current(stream_events, slide_duration="1 minute")
        raise AssertionError("Expected ValueError when passing slide_duration to aggregate_current")
    except ValueError as exc:
        assert "slide_duration" in str(exc)
        print("  -> aggregate_current(slide_duration=...) correctly rejected: CONFIRMED")

    # 3. Inspect logical plan to confirm watermark attachment and tumbling window expression
    plan = windowed._jdf.queryExecution().logical().toString()
    print("Logical Plan:\n", plan)

    assert (
        "EventTimeWatermark" in plan and "event_timestamp" in plan and "10 minutes" in plan
    ), "Watermark not on event_timestamp!"
    assert (
        "event_timestamp" in plan and "300000000" in plan
    ), "Tumbling window 5-minute duration (300000000 us) missing!"
    # Ensure no sliding window Expand operator is present
    assert "Expand" not in plan, "Sliding window Expand operator found; tumbling windows must not expand rows!"
    # Word-boundary check: ensure 60000000 us (1 minute) is not present as an independent token
    assert re.search(r"\b60000000\b", plan) is None, "Found unexpected 1-minute slide duration (60000000 us) in plan!"

    print("  -> withWatermark('event_timestamp', '10 minutes'): CONFIRMED")
    print("  -> F.window('event_timestamp', '5 minutes') [tumbling]: CONFIRMED")
    print("  -> Expand operator absent (no overlapping sliding windows): CONFIRMED")
    print("Spark Watermark & Tumbling Window semantics: PASS\n")


def main() -> None:
    spark = create_spark()
    try:
        test_watermark_and_tumbling_window_semantics(spark)
        print("ALL SPARK WATERMARK SEMANTICS TESTS = PASS")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
