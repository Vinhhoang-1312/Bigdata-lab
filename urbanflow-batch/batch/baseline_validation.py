"""Historical Gold contract assertions."""

from pyspark.sql import DataFrame, functions as F

from batch.schemas import (
    HISTORICAL_BASELINE_FIELDS,
    HISTORICAL_BASELINE_TYPES,
)


def assert_historical_baseline_contract(baseline: DataFrame) -> None:
    if tuple(baseline.columns) != HISTORICAL_BASELINE_FIELDS:
        raise AssertionError("Historical baseline fields differ from Foundation")
    actual_types = {
        field.name: field.dataType.simpleString() for field in baseline.schema.fields
    }
    if actual_types != HISTORICAL_BASELINE_TYPES:
        raise AssertionError(
            f"Historical baseline types differ: expected "
            f"{HISTORICAL_BASELINE_TYPES}, got {actual_types}"
        )

    invalid = baseline.where(
        F.col("pickup_zone_id").isNull()
        | (F.col("pickup_zone_id") <= 0)
        | ~F.col("day_of_week").between(1, 7)
        | ~F.col("hour").between(0, 23)
        | ~F.col("five_minute_slot").between(0, 11)
        | F.col("historical_mean_trip_count").isNull()
        | (F.col("historical_mean_trip_count") < 0)
        | F.col("historical_std_trip_count").isNull()
        | (F.col("historical_std_trip_count") < 0)
    ).limit(1)
    if invalid.count():
        raise AssertionError("Historical baseline contains invalid contract values")

    key = ["pickup_zone_id", "day_of_week", "hour", "five_minute_slot"]
    if baseline.groupBy(*key).count().where(F.col("count") > 1).limit(1).count():
        raise AssertionError("Historical baseline grain is not unique")
