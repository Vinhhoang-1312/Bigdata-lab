"""Leakage-safe feature construction for the demand forecast experiment."""

from __future__ import annotations

import math


FEATURE_COLUMNS = [
    "pickup_zone_id",
    "day_of_week",
    "half_hour_slot",
    "month",
    "is_weekend",
    "time_of_day_sin",
    "time_of_day_cos",
    "lag_1_count",
    "lag_2_count",
    "trailing_2h_mean",
    "same_slot_prev_day",
    "same_slot_prev_week",
    "prev_day_missing",
    "prev_week_missing",
]


def _with_recent_lags(demand):
    from pyspark.sql import Window, functions as F

    zone_order = Window.partitionBy("pickup_zone_id").orderBy("bucket_start")
    trailing = zone_order.rowsBetween(-4, -1)
    return (
        demand.withColumn("label", F.col("trip_count").cast("double"))
        .withColumn("lag_1_count", F.coalesce(F.lag("trip_count", 1).over(zone_order).cast("double"), F.lit(0.0)))
        .withColumn("lag_2_count", F.coalesce(F.lag("trip_count", 2).over(zone_order).cast("double"), F.lit(0.0)))
        .withColumn("trailing_2h_mean", F.coalesce(F.avg(F.col("trip_count").cast("double")).over(trailing), F.lit(0.0)))
    )


def _join_seasonal_lag(frame, demand, *, days_back: int, value_col: str, missing_col: str):
    from pyspark.sql import functions as F

    lagged = demand.select(
        F.col("pickup_zone_id").alias("_lag_zone"),
        F.date_add("service_date", days_back).alias("_target_service_date"),
        F.col("half_hour_slot").alias("_lag_half_hour_slot"),
        F.col("trip_count").cast("double").alias(f"_{value_col}_raw"),
    )
    joined = frame.join(
        lagged,
        (frame.pickup_zone_id == lagged._lag_zone)
        & (frame.service_date == lagged._target_service_date)
        & (frame.half_hour_slot == lagged._lag_half_hour_slot),
        "left",
    )
    raw_col = f"_{value_col}_raw"
    return (
        joined.withColumn(missing_col, F.when(F.col(raw_col).isNull(), F.lit(1)).otherwise(F.lit(0)).cast("int"))
        .withColumn(value_col, F.coalesce(F.col(raw_col), F.lit(0.0)))
        .drop("_lag_zone", "_target_service_date", "_lag_half_hour_slot", raw_col)
    )


def build_features(demand):
    from pyspark.sql import functions as F

    frame = _with_recent_lags(demand)
    frame = _join_seasonal_lag(
        frame,
        demand,
        days_back=1,
        value_col="same_slot_prev_day",
        missing_col="prev_day_missing",
    )
    frame = _join_seasonal_lag(
        frame,
        demand,
        days_back=7,
        value_col="same_slot_prev_week",
        missing_col="prev_week_missing",
    )
    return (
        frame.withColumn(
            "time_of_day_sin",
            F.sin(F.lit(2.0 * math.pi) * F.col("half_hour_slot").cast("double") / F.lit(48.0)),
        )
        .withColumn(
            "time_of_day_cos",
            F.cos(F.lit(2.0 * math.pi) * F.col("half_hour_slot").cast("double") / F.lit(48.0)),
        )
    )
