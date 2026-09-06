"""Spark schemas and frozen Silver contract fields."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SOURCE_COLUMNS = (
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "passenger_count",
    "fare_amount",
    "total_amount",
)

SOURCE_CSV_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in SOURCE_COLUMNS]
)

ZONE_LOOKUP_CSV_SCHEMA = StructType(
    [
        StructField("LocationID", StringType(), True),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True),
    ]
)

SILVER_SCHEMA = StructType(
    [
        StructField("pickup_datetime", TimestampType(), False),
        StructField("dropoff_datetime", TimestampType(), False),
        StructField("pickup_zone_id", IntegerType(), False),
        StructField("dropoff_zone_id", IntegerType(), False),
        StructField("trip_distance", DoubleType(), False),
        StructField("passenger_count", IntegerType(), True),
        StructField("fare_amount", DoubleType(), False),
        StructField("total_amount", DoubleType(), True),
        StructField("trip_duration_minutes", DoubleType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day_of_week", IntegerType(), False),
        StructField("hour", IntegerType(), False),
        StructField("five_minute_slot", IntegerType(), False),
    ]
)

SILVER_FIELDS = tuple(field.name for field in SILVER_SCHEMA.fields)
SILVER_TYPES = {field.name: field.dataType.simpleString() for field in SILVER_SCHEMA.fields}
SILVER_REQUIRED_FIELDS = tuple(
    field.name for field in SILVER_SCHEMA.fields if not field.nullable
)

HISTORICAL_BASELINE_SCHEMA = StructType(
    [
        StructField("pickup_zone_id", IntegerType(), False),
        StructField("day_of_week", IntegerType(), False),
        StructField("hour", IntegerType(), False),
        StructField("five_minute_slot", IntegerType(), False),
        StructField("historical_mean_trip_count", DoubleType(), False),
        StructField("historical_std_trip_count", DoubleType(), False),
        StructField("historical_mean_distance", DoubleType(), True),
        StructField("historical_mean_duration", DoubleType(), True),
    ]
)
HISTORICAL_BASELINE_FIELDS = tuple(
    field.name for field in HISTORICAL_BASELINE_SCHEMA.fields
)
HISTORICAL_BASELINE_TYPES = {
    field.name: field.dataType.simpleString()
    for field in HISTORICAL_BASELINE_SCHEMA.fields
}
