"""Logical Silver contract assertions."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from batch.schemas import (
    SILVER_FIELDS,
    SILVER_REQUIRED_FIELDS,
    SILVER_TYPES,
)


def assert_silver_contract(silver: DataFrame) -> None:
    if tuple(silver.columns) != SILVER_FIELDS:
        raise AssertionError(
            f"Silver fields differ: expected {SILVER_FIELDS}, got {tuple(silver.columns)}"
        )

    actual_types = {
        field.name: field.dataType.simpleString() for field in silver.schema.fields
    }
    if actual_types != SILVER_TYPES:
        raise AssertionError(
            f"Silver types differ: expected {SILVER_TYPES}, got {actual_types}"
        )

    null_counts = silver.agg(
        *[
            F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias(name)
            for name in SILVER_REQUIRED_FIELDS
        ]
    ).first()
    invalid_nulls = {
        name: null_counts[name]
        for name in SILVER_REQUIRED_FIELDS
        if null_counts[name] != 0
    }
    if invalid_nulls:
        raise AssertionError(f"Required Silver fields contain nulls: {invalid_nulls}")

    invalid_values = silver.where(
        (F.col("dropoff_datetime") < F.col("pickup_datetime"))
        | (F.col("pickup_zone_id") <= 0)
        | (F.col("dropoff_zone_id") <= 0)
        | (F.col("trip_distance") < 0)
        | (F.col("trip_duration_minutes") < 0)
        | (F.col("fare_amount") < 0)
        | (F.col("passenger_count") < 0)
        | (F.col("total_amount") < 0)
        | ~F.col("month").between(1, 12)
        | ~F.col("day_of_week").between(1, 7)
        | ~F.col("hour").between(0, 23)
        | ~F.col("five_minute_slot").between(0, 11)
    ).limit(1)
    if invalid_values.count():
        raise AssertionError("Silver contains a value outside Foundation constraints")
