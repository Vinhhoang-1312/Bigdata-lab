"""Historical Gold construction using Foundation 1.0.0 semantics."""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, functions as F

from batch.schemas import HISTORICAL_BASELINE_FIELDS

TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss"


def build_historical_baseline(
    silver: DataFrame,
    zone_lookup: DataFrame,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """Build a zero-inclusive zone/weekday/hour/five-minute baseline."""
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if start > end:
        raise ValueError("Historical start_date must not be after end_date")
    if "LocationID" not in zone_lookup.columns:
        raise ValueError("Zone lookup is missing LocationID")

    spark = silver.sparkSession
    historical = silver.where(
        F.to_date("pickup_datetime").between(F.lit(start_date), F.lit(end_date))
    ).cache()
    if historical.limit(1).count() == 0:
        historical.unpersist()
        raise ValueError("Historical period contains no Silver trips")

    dates = spark.sql(
        f"SELECT explode(sequence(to_date('{start_date}'), "
        f"to_date('{end_date}'), interval 1 day)) AS service_date"
    )
    zones = (
        zone_lookup.select(F.expr("try_cast(LocationID as int)").alias("pickup_zone_id"))
        .where(F.col("pickup_zone_id").isNotNull())
        .distinct()
    )
    slots = spark.range(24 * 12).select(
        F.floor(F.col("id") / F.lit(12)).cast("int").alias("hour"),
        (F.col("id") % F.lit(12)).cast("int").alias("five_minute_slot"),
    )
    date_slots = dates.crossJoin(F.broadcast(slots)).withColumn(
        "_local_window_text",
        F.format_string(
            "%s %02d:%02d:00",
            F.date_format("service_date", "yyyy-MM-dd"),
            F.col("hour"),
            F.col("five_minute_slot") * 5,
        ),
    )
    date_slots = (
        date_slots.withColumn(
            "_window_start",
            F.try_to_timestamp(
                F.col("_local_window_text"), F.lit(TIMESTAMP_PATTERN)
            ),
        )
        .where(
            F.date_format("_window_start", TIMESTAMP_PATTERN)
            == F.col("_local_window_text")
        )
        .withColumn("day_of_week", (F.weekday("service_date") + 1).cast("int"))
    )
    complete_grid = F.broadcast(zones).crossJoin(date_slots).select(
        "service_date",
        "pickup_zone_id",
        "day_of_week",
        "hour",
        "five_minute_slot",
    )

    daily_counts = (
        historical.withColumn("service_date", F.to_date("pickup_datetime"))
        .groupBy(
            "service_date",
            "pickup_zone_id",
            "day_of_week",
            "hour",
            "five_minute_slot",
        )
        .agg(F.count(F.lit(1)).alias("trip_count"))
    )
    zero_inclusive = complete_grid.join(
        daily_counts,
        on=[
            "service_date",
            "pickup_zone_id",
            "day_of_week",
            "hour",
            "five_minute_slot",
        ],
        how="left",
    ).fillna({"trip_count": 0})

    count_metrics = zero_inclusive.groupBy(
        "pickup_zone_id", "day_of_week", "hour", "five_minute_slot"
    ).agg(
        F.avg("trip_count").cast("double").alias("historical_mean_trip_count"),
        F.stddev_pop("trip_count")
        .cast("double")
        .alias("historical_std_trip_count"),
    )
    trip_metrics = historical.groupBy(
        "pickup_zone_id", "day_of_week", "hour", "five_minute_slot"
    ).agg(
        F.avg("trip_distance").cast("double").alias("historical_mean_distance"),
        F.avg("trip_duration_minutes")
        .cast("double")
        .alias("historical_mean_duration"),
    )
    result = count_metrics.join(
        trip_metrics,
        on=["pickup_zone_id", "day_of_week", "hour", "five_minute_slot"],
        how="left",
    ).select(*HISTORICAL_BASELINE_FIELDS)
    historical.unpersist()
    return result
