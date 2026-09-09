"""Kafka trip-event Spark schema and strict, observable parsing."""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F, types as T

EVENT_FIELDS = (
    "event_id", "event_time", "pickup_zone_id", "dropoff_zone_id",
    "trip_distance", "trip_duration_minutes", "fare_amount",
)

TRIP_EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType(), False),
    T.StructField("event_time", T.StringType(), False),
    T.StructField("pickup_zone_id", T.IntegerType(), False),
    T.StructField("dropoff_zone_id", T.IntegerType(), False),
    T.StructField("trip_distance", T.DoubleType(), False),
    T.StructField("trip_duration_minutes", T.DoubleType(), False),
    T.StructField("fare_amount", T.DoubleType(), False),
])


def event_schema() -> T.StructType:
    """Return a fresh schema mirroring Foundation's seven required fields."""
    return T.StructType([T.StructField(f.name, f.dataType, f.nullable) for f in TRIP_EVENT_SCHEMA])


@dataclass(frozen=True)
class ParsedEvents:
    valid: DataFrame
    invalid: DataFrame


def parse_events(source: DataFrame, value_column: str = "value") -> ParsedEvents:
    """Parse Kafka JSON while preserving raw malformed rows and an error reason."""
    raw = F.col(value_column).cast("string")
    frame = source.withColumn("raw_value", raw).withColumn(
        "event", F.from_json(raw, event_schema(), {"mode": "PERMISSIVE"})
    )
    # Parsing to a declared struct ignores unknown properties, so compare JSON object keys too.
    key_schema = T.MapType(T.StringType(), T.StringType())
    keys = F.map_keys(F.from_json(raw, key_schema))
    extras = F.array_except(keys, F.array(*[F.lit(name) for name in EVENT_FIELDS]))
    timestamp = F.to_timestamp(F.col("event.event_time"))
    has_offset = F.col("event.event_time").rlike(r"(Z|[+-][0-9]{2}:[0-9]{2})$")
    required_null = F.exists(
        F.array(*[F.col(f"event.{name}").isNull() for name in EVENT_FIELDS]), lambda x: x
    )
    invalid = (
        F.col("event").isNull() | required_null | ~has_offset | timestamp.isNull()
        | (F.size(extras) > 0)
        | (F.length(F.trim(F.col("event.event_id"))) == 0)
        | (F.col("event.pickup_zone_id") < 1) | (F.col("event.dropoff_zone_id") < 1)
        | (F.col("event.trip_distance") < 0) | (F.col("event.trip_duration_minutes") < 0)
        | (F.col("event.fare_amount") < 0)
    )
    reason = (
        F.when(F.col("event").isNull(), "invalid_json_or_type")
        .when(required_null, "missing_required_field")
        .when((~has_offset) | timestamp.isNull(), "invalid_event_time")
        .when(F.size(extras) > 0, "additional_property")
        .otherwise("constraint_violation")
    )
    valid = frame.where(~invalid).withColumn("event_timestamp", timestamp).select(
        "raw_value", "event.*", "event_timestamp"
    )
    bad = frame.where(invalid).withColumn("validation_error", reason).select("raw_value", "validation_error")
    return ParsedEvents(valid=valid, invalid=bad)
