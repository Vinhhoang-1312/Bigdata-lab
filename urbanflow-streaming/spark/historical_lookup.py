"""Historical Gold loading and Foundation-contract validation."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

HISTORICAL_SCHEMA = T.StructType([
    T.StructField("pickup_zone_id", T.IntegerType(), False),
    T.StructField("day_of_week", T.IntegerType(), False),
    T.StructField("hour", T.IntegerType(), False),
    T.StructField("five_minute_slot", T.IntegerType(), False),
    T.StructField("historical_mean_trip_count", T.DoubleType(), False),
    T.StructField("historical_std_trip_count", T.DoubleType(), False),
    T.StructField("historical_mean_distance", T.DoubleType(), True),
    T.StructField("historical_mean_duration", T.DoubleType(), True),
])
KEY = ["pickup_zone_id", "day_of_week", "hour", "five_minute_slot"]


def validate_historical_baseline(df: DataFrame) -> DataFrame:
    missing = [field.name for field in HISTORICAL_SCHEMA if field.name not in df.columns]
    if missing:
        raise ValueError(f"Historical baseline missing columns: {missing}")
    invalid = df.where(
        F.col("pickup_zone_id").isNull() | (F.col("pickup_zone_id") < 1)
        | ~F.col("day_of_week").between(1, 7) | ~F.col("hour").between(0, 23)
        | ~F.col("five_minute_slot").between(0, 11)
        | F.col("historical_mean_trip_count").isNull()
        | F.col("historical_std_trip_count").isNull()
        | (F.col("historical_mean_trip_count") < 0)
        | (F.col("historical_std_trip_count") < 0)
    ).limit(1).count()
    if invalid:
        raise ValueError("Historical baseline violates Foundation value constraints")
    if df.groupBy(*KEY).count().where(F.col("count") > 1).limit(1).count():
        raise ValueError("Historical baseline contains duplicate lookup keys")
    return df.select(*[F.col(field.name).cast(field.dataType).alias(field.name) for field in HISTORICAL_SCHEMA])


def load_historical_baseline(spark: SparkSession, path: str, fixture: bool | None = None) -> DataFrame:
    """Read fixture JSONL or integrated Parquet, then enforce the same schema."""
    is_json = fixture if fixture is not None else path.lower().endswith((".json", ".jsonl"))
    df = spark.read.schema(HISTORICAL_SCHEMA).json(path) if is_json else spark.read.parquet(path)
    return validate_historical_baseline(df)
