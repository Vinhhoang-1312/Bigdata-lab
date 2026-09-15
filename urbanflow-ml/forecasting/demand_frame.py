"""Build the zero-inclusive demand frame for zone-level forecasting."""

from __future__ import annotations


TIMEZONE = "America/New_York"
OUTPUT_COLUMNS = [
    "service_date",
    "bucket_start",
    "bucket_end",
    "pickup_zone_id",
    "half_hour_slot",
    "day_of_week",
    "month",
    "is_weekend",
    "trip_count",
]


def _zone_ids(zone_lookup):
    from pyspark.sql import functions as F

    if "pickup_zone_id" in zone_lookup.columns:
        source_col = "pickup_zone_id"
    elif "LocationID" in zone_lookup.columns:
        source_col = "LocationID"
    else:
        raise ValueError("zone_lookup must contain LocationID or pickup_zone_id")

    return (
        zone_lookup.select(F.col(source_col).cast("int").alias("pickup_zone_id"))
        .where(F.col("pickup_zone_id").isNotNull() & (F.col("pickup_zone_id") >= 1))
        .dropDuplicates(["pickup_zone_id"])
    )


def _date_slots(spark, start_date: str, end_date: str):
    from pyspark.sql import functions as F

    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")

    dates = spark.sql(
        f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS service_date"
    )
    slots = spark.range(0, 48).select(F.col("id").cast("int").alias("half_hour_slot"))
    frame = (
        dates.crossJoin(slots)
        .withColumn("hour", F.floor(F.col("half_hour_slot") / F.lit(2)).cast("int"))
        .withColumn("minute", ((F.col("half_hour_slot") % F.lit(2)) * F.lit(30)).cast("int"))
        .withColumn(
            "local_start_text",
            F.format_string(
                "%s %02d:%02d:00",
                F.col("service_date").cast("string"),
                F.col("hour"),
                F.col("minute"),
            ),
        )
        .withColumn("bucket_start", F.to_timestamp("local_start_text"))
        .where(F.date_format("bucket_start", "yyyy-MM-dd HH:mm:ss") == F.col("local_start_text"))
        .withColumn("bucket_end", F.col("bucket_start") + F.expr("INTERVAL 30 MINUTES"))
        .withColumn("day_of_week", (((F.dayofweek("bucket_start") + F.lit(5)) % F.lit(7)) + F.lit(1)).cast("int"))
        .withColumn("month", F.month("service_date").cast("int"))
        .withColumn("is_weekend", F.col("day_of_week").isin(6, 7))
        .select("service_date", "bucket_start", "bucket_end", "half_hour_slot", "day_of_week", "month", "is_weekend")
    )
    return frame


def _trip_counts(silver, start_date: str, end_date: str):
    from pyspark.sql import functions as F

    return (
        silver.select(
            F.to_date("pickup_datetime").alias("service_date"),
            F.col("pickup_zone_id").cast("int").alias("pickup_zone_id"),
            (
                F.hour("pickup_datetime") * F.lit(2)
                + F.floor(F.minute("pickup_datetime") / F.lit(30))
            ).cast("int").alias("half_hour_slot"),
        )
        .where((F.col("service_date") >= F.to_date(F.lit(start_date))) & (F.col("service_date") <= F.to_date(F.lit(end_date))))
        .groupBy("service_date", "pickup_zone_id", "half_hour_slot")
        .agg(F.count(F.lit(1)).cast("long").alias("trip_count"))
    )


def validate_demand_frame_invariants(frame) -> None:
    from pyspark.sql import functions as F

    null_keys = frame.where(
        F.col("service_date").isNull()
        | F.col("bucket_start").isNull()
        | F.col("bucket_end").isNull()
        | F.col("pickup_zone_id").isNull()
        | F.col("half_hour_slot").isNull()
    ).limit(1).count()
    if null_keys:
        raise ValueError("Demand frame contains null key fields")

    invalid_rows = frame.where(
        (F.col("trip_count") < 0)
        | (F.col("half_hour_slot") < 0)
        | (F.col("half_hour_slot") > 47)
        | (F.col("bucket_end") <= F.col("bucket_start"))
    ).limit(1).count()
    if invalid_rows:
        raise ValueError("Demand frame contains invalid ranges")

    duplicates = (
        frame.groupBy("service_date", "bucket_start", "pickup_zone_id")
        .agg(F.count(F.lit(1)).alias("rows"))
        .where(F.col("rows") > 1)
        .limit(1)
        .count()
    )
    if duplicates:
        raise ValueError("Demand frame contains duplicate zone/date/bucket keys")


def build_demand_frame(silver, zone_lookup, start_date: str, end_date: str):
    spark = silver.sparkSession
    spark.conf.set("spark.sql.session.timeZone", TIMEZONE)

    zones = _zone_ids(zone_lookup)
    grid = _date_slots(spark, start_date, end_date).crossJoin(zones)
    counts = _trip_counts(silver, start_date, end_date)
    frame = (
        grid.join(counts, ["service_date", "pickup_zone_id", "half_hour_slot"], "left")
        .fillna({"trip_count": 0})
        .select(*OUTPUT_COLUMNS)
        .orderBy("service_date", "bucket_start", "pickup_zone_id")
    )
    validate_demand_frame_invariants(frame)
    return frame
