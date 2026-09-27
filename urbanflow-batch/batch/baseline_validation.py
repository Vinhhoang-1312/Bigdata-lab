"""Historical Gold contract assertions."""

from pyspark.sql import DataFrame, functions as F

from batch.schemas import (
    HISTORICAL_BASELINE_FIELDS,
    HISTORICAL_BASELINE_TYPES,
)


def assert_historical_baseline_contract(
    baseline: DataFrame,
    expected_zone_count: int | None = None,
    require_full_grid: bool = False,
) -> None:
    """Validate Historical Gold contract conformity.

    Parameters
    ----------
    baseline : DataFrame
        Historical Gold baseline DataFrame to validate.
    expected_zone_count : int | None
        If provided, asserts exact number of distinct zones in the baseline.
    require_full_grid : bool
        If True, asserts every zone contains all 2,016 slots (7 days * 24 hours * 12 slots).
        Default is False to allow partial test periods or periods spanning Daylight
        Saving Time (DST) spring-forward transitions where 02:00-03:00 local time does
        not exist in America/New_York (resulting in 2,004 slots).
    """
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
        | F.isnan("historical_mean_trip_count")
        | (F.col("historical_mean_trip_count") == float("inf"))
        | F.col("historical_std_trip_count").isNull()
        | (F.col("historical_std_trip_count") < 0)
        | F.isnan("historical_std_trip_count")
        | (F.col("historical_std_trip_count") == float("inf"))
    ).limit(1)
    if invalid.count():
        raise AssertionError("Historical baseline contains invalid contract values")

    key = ["pickup_zone_id", "day_of_week", "hour", "five_minute_slot"]
    if baseline.groupBy(*key).count().where(F.col("count") > 1).limit(1).count():
        raise AssertionError("Historical baseline grain is not unique")

    # Grid completeness: slot coverage must be uniform across all zones
    zone_slot_counts = baseline.groupBy("pickup_zone_id").count()
    if zone_slot_counts.select("count").distinct().count() > 1:
        raise AssertionError(
            "Historical baseline contains uneven slot counts across zones"
        )

    if require_full_grid:
        incomplete_zones = zone_slot_counts.where(F.col("count") != 2016).limit(1)
        if incomplete_zones.count():
            raise AssertionError(
                "Historical baseline contains zones without complete 2,016 slot grid coverage"
            )

    if expected_zone_count is not None:
        actual_zones = zone_slot_counts.count()
        if actual_zones != expected_zone_count:
            raise AssertionError(
                f"Historical baseline zone count differs: expected {expected_zone_count}, got {actual_zones}"
            )
