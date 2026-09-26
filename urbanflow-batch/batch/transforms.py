"""Bronze-to-Silver transformations governed by Foundation 1.0.0."""

from __future__ import annotations

from typing import NamedTuple

from pyspark.sql import Column, DataFrame, functions as F

from batch.schemas import SILVER_FIELDS, SOURCE_COLUMNS

TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss"


class TransformationResult(NamedTuple):
    silver: DataFrame
    rejected: DataFrame


def _required_columns_present(source: DataFrame) -> None:
    missing = sorted(set(SOURCE_COLUMNS) - set(source.columns))
    if missing:
        raise ValueError(f"Source data is missing required columns: {missing}")


def _invalid_timestamp(raw: Column, parsed: Column) -> Column:
    round_trip_matches = F.date_format(parsed, TIMESTAMP_PATTERN) == raw
    return ~F.coalesce(parsed.isNotNull() & round_trip_matches, F.lit(False))


def bronze_to_silver(source: DataFrame, zone_lookup: DataFrame) -> TransformationResult:
    """Clean source trips and return valid Silver and visibly rejected rows."""
    _required_columns_present(source)
    if "LocationID" not in zone_lookup.columns:
        raise ValueError("Zone lookup is missing LocationID")

    valid_zone_ids = [
        row.location_id
        for row in (
            zone_lookup.select(
                F.expr("try_cast(LocationID as int)").alias("location_id")
            )
            .where(F.col("location_id").isNotNull())
            .distinct()
            .collect()
        )
    ]
    if not valid_zone_ids:
        raise ValueError("Zone lookup contains no valid LocationID values")

    prepared = source
    for name in SOURCE_COLUMNS:
        prepared = prepared.withColumn(f"_source_{name}", F.col(name))

    raw_pickup = F.trim(F.col("_source_tpep_pickup_datetime").cast("string"))
    raw_dropoff = F.trim(F.col("_source_tpep_dropoff_datetime").cast("string"))

    prepared = (
        prepared.withColumn("_raw_pickup", raw_pickup)
        .withColumn("_raw_dropoff", raw_dropoff)
        .withColumn(
            "pickup_datetime",
            F.try_to_timestamp(F.col("_raw_pickup"), F.lit(TIMESTAMP_PATTERN)),
        )
        .withColumn(
            "dropoff_datetime",
            F.try_to_timestamp(F.col("_raw_dropoff"), F.lit(TIMESTAMP_PATTERN)),
        )
        .withColumn("pickup_zone_id", F.expr("try_cast(PULocationID as int)"))
        .withColumn("dropoff_zone_id", F.expr("try_cast(DOLocationID as int)"))
        .withColumn("trip_distance", F.expr("try_cast(trip_distance as double)"))
        .withColumn("passenger_count", F.expr("try_cast(passenger_count as int)"))
        .withColumn("fare_amount", F.expr("try_cast(fare_amount as double)"))
        .withColumn("total_amount", F.expr("try_cast(total_amount as double)"))
    )

    pickup_invalid = _invalid_timestamp(
        F.col("_raw_pickup"), F.col("pickup_datetime")
    )
    dropoff_invalid = _invalid_timestamp(
        F.col("_raw_dropoff"), F.col("dropoff_datetime")
    )
    passenger_supplied = (
        F.length(F.trim(F.col("_source_passenger_count").cast("string"))) > 0
    )
    total_supplied = F.length(
        F.trim(F.col("_source_total_amount").cast("string"))
    ) > 0

    rejection_rules = (
        (pickup_invalid, "pickup_datetime_invalid"),
        (dropoff_invalid, "dropoff_datetime_invalid"),
        (
            F.col("pickup_datetime").isNotNull()
            & F.col("dropoff_datetime").isNotNull()
            & (F.col("dropoff_datetime") < F.col("pickup_datetime")),
            "dropoff_before_pickup",
        ),
        (
            F.col("pickup_zone_id").isNull()
            | ~F.col("pickup_zone_id").isin(valid_zone_ids),
            "pickup_zone_invalid",
        ),
        (
            F.col("dropoff_zone_id").isNull()
            | ~F.col("dropoff_zone_id").isin(valid_zone_ids),
            "dropoff_zone_invalid",
        ),
        (
            F.col("trip_distance").isNull() | (F.col("trip_distance") < 0),
            "trip_distance_invalid",
        ),
        (
            F.col("fare_amount").isNull() | (F.col("fare_amount") < 0),
            "fare_amount_invalid",
        ),
        (
            passenger_supplied
            & (
                F.col("passenger_count").isNull()
                | (F.col("passenger_count") < 0)
            ),
            "passenger_count_invalid",
        ),
        (
            total_supplied
            & (F.col("total_amount").isNull() | (F.col("total_amount") < 0)),
            "total_amount_invalid",
        ),
    )

    with_reasons = prepared.withColumn(
        "rejection_reasons",
        F.filter(
            F.array(
                *[F.when(condition, F.lit(reason)) for condition, reason in rejection_rules]
            ),
            lambda reason: reason.isNotNull(),
        ),
    )

    valid = (
        with_reasons.where(F.size("rejection_reasons") == 0)
        .withColumn(
            "trip_duration_minutes",
            F.round(
                (
                    F.unix_timestamp("dropoff_datetime")
                    - F.unix_timestamp("pickup_datetime")
                )
                / F.lit(60.0),
                4,
            ).cast("double"),
        )
        .withColumn("year", F.year("pickup_datetime").cast("int"))
        .withColumn("month", F.month("pickup_datetime").cast("int"))
        .withColumn("day_of_week", (F.weekday("pickup_datetime") + 1).cast("int"))
        .withColumn("hour", F.hour("pickup_datetime").cast("int"))
        .withColumn(
            "five_minute_slot",
            F.floor(F.minute("pickup_datetime") / F.lit(5)).cast("int"),
        )
        .select(*SILVER_FIELDS)
    )
    rejected = with_reasons.where(F.size("rejection_reasons") > 0).select(
        *[
            F.col(f"_source_{name}").alias(name)
            for name in SOURCE_COLUMNS
        ],
        "rejection_reasons",
    )
    return TransformationResult(silver=valid, rejected=rejected)
