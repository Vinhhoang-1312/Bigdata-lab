"""Leakage-safe baseline forecasts for the demand experiment."""

from __future__ import annotations


def add_seasonal_naive_prediction(frame):
    from pyspark.sql import functions as F

    return frame.withColumn(
        "seasonal_naive_prediction",
        F.col("same_slot_prev_week").cast("double"),
    )


def fit_historical_mean(train):
    from pyspark.sql import functions as F

    return (
        train.groupBy("pickup_zone_id", "day_of_week", "half_hour_slot")
        .agg(F.avg(F.col("label").cast("double")).alias("historical_mean_prediction"))
        .orderBy("pickup_zone_id", "day_of_week", "half_hour_slot")
    )
