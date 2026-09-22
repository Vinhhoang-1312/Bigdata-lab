"""Local copies of Foundation 1.0.0 field expectations for ML audits."""

SILVER_REQUIRED_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_zone_id",
    "dropoff_zone_id",
    "trip_distance",
    "passenger_count",
    "fare_amount",
    "total_amount",
    "trip_duration_minutes",
    "year",
    "month",
    "day_of_week",
    "hour",
    "five_minute_slot",
]

SILVER_NON_NULL_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_zone_id",
    "dropoff_zone_id",
    "trip_distance",
    "fare_amount",
    "trip_duration_minutes",
    "year",
    "month",
    "day_of_week",
    "hour",
    "five_minute_slot",
]
