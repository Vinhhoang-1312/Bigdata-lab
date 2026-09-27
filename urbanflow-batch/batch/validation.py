"""Logical Silver contract assertions."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from batch.schemas import (
    SILVER_FIELDS,
    SILVER_REQUIRED_FIELDS,
    SILVER_TYPES,
)


def assert_silver_contract(
    silver: DataFrame,
    valid_zone_ids: set[int] | DataFrame | None = None,
) -> None:
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

    # Physical validity, range checks, and non-finite number checks
    invalid_values = silver.where(
        (F.col("dropoff_datetime") < F.col("pickup_datetime"))
        | (F.col("pickup_zone_id") <= 0)
        | (F.col("dropoff_zone_id") <= 0)
        | (F.col("trip_distance") < 0)
        | F.isnan("trip_distance")
        | (F.col("trip_distance") == float("inf"))
        | (F.col("trip_distance") == float("-inf"))
        | (F.col("trip_duration_minutes") < 0)
        | F.isnan("trip_duration_minutes")
        | (F.col("trip_duration_minutes") == float("inf"))
        | (F.col("fare_amount") < 0)
        | F.isnan("fare_amount")
        | (F.col("fare_amount") == float("inf"))
        | (F.col("passenger_count") < 0)
        | (F.col("total_amount") < 0)
        | (F.col("total_amount").isNotNull() & (F.isnan("total_amount") | (F.col("total_amount") == float("inf"))))
        | ~F.col("month").between(1, 12)
        | ~F.col("day_of_week").between(1, 7)
        | ~F.col("hour").between(0, 23)
        | ~F.col("five_minute_slot").between(0, 11)
    ).limit(1)
    if invalid_values.count():
        raise AssertionError("Silver contains a value outside Foundation constraints")

    # Derived fields must strictly equal values derived from pickup_datetime
    derived_mismatch = silver.where(
        (F.col("year") != F.year("pickup_datetime"))
        | (F.col("month") != F.month("pickup_datetime"))
        | (F.col("day_of_week") != (F.weekday("pickup_datetime") + 1))
        | (F.col("hour") != F.hour("pickup_datetime"))
        | (F.col("five_minute_slot") != F.floor(F.minute("pickup_datetime") / F.lit(5)).cast("int"))
    ).limit(1)
    if derived_mismatch.count():
        raise AssertionError("Silver contains derived time fields inconsistent with pickup_datetime")

    # Duration consistency with pickup and dropoff timestamps
    expected_duration = F.round(
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / F.lit(60.0),
        4,
    )
    duration_mismatch = silver.where(
        F.abs(F.col("trip_duration_minutes") - expected_duration) > 0.0001
    ).limit(1)
    if duration_mismatch.count():
        raise AssertionError("Silver trip_duration_minutes is inconsistent with pickup/dropoff timestamps")

    # Zone ID lookup verification
    if valid_zone_ids is not None:
        if isinstance(valid_zone_ids, DataFrame):
            if "LocationID" not in valid_zone_ids.columns:
                raise AssertionError("Zone lookup is missing LocationID")
            valid_set = {
                int(row.zone_id)
                for row in (
                    valid_zone_ids.select(
                        F.expr("try_cast(LocationID as int)").alias("zone_id")
                    )
                    .where(F.col("zone_id").isNotNull())
                    .distinct()
                    .collect()
                )
            }
        else:
            valid_set = {int(zone_id) for zone_id in valid_zone_ids}
        if not valid_set:
            raise AssertionError("Zone lookup contains no valid LocationID values")
        invalid_zones = silver.where(
            ~F.col("pickup_zone_id").isin(valid_set)
            | ~F.col("dropoff_zone_id").isin(valid_set)
        ).limit(1)
        if invalid_zones.count():
            raise AssertionError("Silver contains zone IDs not present in zone lookup")
