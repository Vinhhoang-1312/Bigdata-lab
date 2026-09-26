#!/usr/bin/env python3
"""Verify canonical Batch outputs and the frozen real-data split."""

from __future__ import annotations

from pyspark.sql import SparkSession, functions as F

from batch.baseline_validation import assert_historical_baseline_contract
from batch.schemas import HISTORICAL_BASELINE_FIELDS, SILVER_FIELDS
from batch.validation import assert_silver_contract

SILVER_PATH = "s3a://urbanflow/silver/trips/"
HISTORICAL_PATH = "s3a://urbanflow/gold/historical_zone_baseline/"
HISTORICAL_START = "2023-01-01"
HISTORICAL_END = "2023-01-31"
REPLAY_START = "2023-02-01"
REPLAY_END = "2023-02-28"


def main() -> None:
    assert HISTORICAL_END < REPLAY_START
    spark = SparkSession.builder.appName("Verify Integrated Batch Outputs").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.parquet(SILVER_PATH).select(*SILVER_FIELDS).cache()
    assert_silver_contract(silver)
    silver_rows = silver.count()
    historical_silver = silver.where(
        F.to_date("pickup_datetime").between(HISTORICAL_START, HISTORICAL_END)
    )
    replay_silver = silver.where(
        F.to_date("pickup_datetime").between(REPLAY_START, REPLAY_END)
    ).cache()
    historical_rows = historical_silver.count()
    replay_rows = replay_silver.count()
    if historical_rows == 0 or replay_rows == 0:
        raise AssertionError("Frozen historical and replay periods must both contain data")

    replay_offsets = {
        row.offset
        for row in replay_silver.select(
            F.date_format("pickup_datetime", "XXX").alias("offset")
        )
        .distinct()
        .collect()
    }
    if replay_offsets != {"-05:00"}:
        raise AssertionError(f"Unexpected February event offsets: {replay_offsets}")

    baseline = spark.read.parquet(HISTORICAL_PATH).select(
        *HISTORICAL_BASELINE_FIELDS
    ).cache()
    assert_historical_baseline_contract(baseline)
    baseline_rows = baseline.count()
    if baseline_rows == 0:
        raise AssertionError("Historical Gold is empty")

    print(f"silver_rows={silver_rows}")
    print(f"historical_period_silver_rows={historical_rows}")
    print(f"replay_period_silver_rows={replay_rows}")
    print(f"replay_offsets={sorted(replay_offsets)}")
    print(f"historical_baseline_rows={baseline_rows}")
    print("PERIODS NON_OVERLAPPING = PASS")
    print("CANONICAL SILVER = PASS")
    print("CANONICAL HISTORICAL GOLD = PASS")
    print("INTEGRATED BATCH OUTPUTS = PASS")

    baseline.unpersist()
    replay_silver.unpersist()
    silver.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
