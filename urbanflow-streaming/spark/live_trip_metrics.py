"""UrbanFlow event-time aggregation, historical comparison, and stream runner."""
from __future__ import annotations

import argparse
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession, functions as F

try:  # Supports both package imports and spark-submit of this file.
    from .event_schema import ParsedEvents, parse_events
    from .historical_lookup import KEY, load_historical_baseline
except ImportError:
    from event_schema import ParsedEvents, parse_events
    from historical_lookup import KEY, load_historical_baseline

ANALYTICS_TIMEZONE = "America/New_York"
DEFAULT_WATERMARK = "10 minutes"
LIVE_COLUMNS = [
    "window_start", "window_end", "pickup_zone_id", "current_trip_count",
    "historical_mean_trip_count", "demand_ratio", "z_score",
]


def configure_session(spark: SparkSession) -> SparkSession:
    spark.conf.set("spark.sql.session.timeZone", ANALYTICS_TIMEZONE)
    return spark


def aggregate_current(events: DataFrame, watermark: str = DEFAULT_WATERMARK) -> DataFrame:
    """Count pickups in clock-aligned five-minute event-time tumbling windows."""
    return (
        events.withWatermark("event_timestamp", watermark)
        .groupBy(F.window("event_timestamp", "5 minutes"), "pickup_zone_id")
        .agg(F.count(F.lit(1)).cast("long").alias("current_trip_count"))
        .select(F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"), "pickup_zone_id", "current_trip_count")
    )


def add_lookup_keys(current: DataFrame) -> DataFrame:
    """Derive the Foundation lookup key in the configured New York session zone."""
    return (
        current.withColumn("day_of_week", F.weekday("window_start") + F.lit(1))
        .withColumn("hour", F.hour("window_start"))
        .withColumn("five_minute_slot", F.floor(F.minute("window_start") / F.lit(5)).cast("int"))
    )


def join_historical(current: DataFrame, historical: DataFrame) -> DataFrame:
    """Preserve all current windows and apply the two contractual null rules."""
    joined = add_lookup_keys(current).join(F.broadcast(historical), KEY, "left")
    mean = F.col("historical_mean_trip_count")
    std = F.col("historical_std_trip_count")
    return (
        joined.withColumn(
            "demand_ratio",
            F.when(mean.isNotNull() & (mean > 0), F.col("current_trip_count").cast("double") / mean)
            .otherwise(F.lit(None).cast("double")),
        ).withColumn(
            "z_score",
            F.when(std.isNotNull() & (std > 0), (F.col("current_trip_count") - mean) / std)
            .otherwise(F.lit(None).cast("double")),
        ).select(*LIVE_COLUMNS)
    )


def transform(source: DataFrame, historical: DataFrame, watermark: str = DEFAULT_WATERMARK) -> tuple[DataFrame, DataFrame]:
    """Transform a Kafka-like value DataFrame; return Live Gold and invalid rows."""
    parsed: ParsedEvents = parse_events(source)
    return join_historical(aggregate_current(parsed.valid, watermark), historical), parsed.invalid


def read_kafka(spark: SparkSession, bootstrap_servers: str, topic: str) -> DataFrame:
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic).option("startingOffsets", "latest").load())


def write_live_gold(metrics: DataFrame, output_path: str, checkpoint_path: str, trigger: str = "10 seconds"):
    """Start restart-safe Parquet output; Spark's checkpoint tracks committed batches."""
    if output_path.rstrip("/") == checkpoint_path.rstrip("/"):
        raise ValueError("Live Gold and checkpoint paths must be different")
    return (metrics.writeStream.format("parquet").outputMode("append")
            .option("path", output_path).option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=trigger).start())


def _settings(path: str) -> dict:
    with open(path, encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(Path(__file__).parents[1] / "config/settings.yaml"))
    parser.add_argument("--fixture-events", help="Run a deterministic available-now file stream")
    args = parser.parse_args()
    cfg = _settings(args.config)
    spark = configure_session(SparkSession.builder.appName("UrbanFlowLiveMetrics").getOrCreate())
    historical = load_historical_baseline(spark, cfg["storage"]["historical_baseline"])
    if args.fixture_events:
        # File streams require a directory; copy fixtures to an input directory in real runs.
        source = spark.readStream.format("text").load(args.fixture_events).select(F.col("value"))
    else:
        source = read_kafka(spark, cfg["kafka"]["bootstrap_servers"], cfg["kafka"]["topic"])
    gold, invalid = transform(source, historical, cfg["streaming"]["watermark"])
    invalid_query = invalid.writeStream.format("console").outputMode("append").queryName("invalid_events").start()
    query = write_live_gold(gold, cfg["storage"]["live_metrics"],
                            cfg["storage"]["streaming_checkpoint"], cfg["streaming"]["trigger"])
    try:
        query.awaitTermination()
    finally:
        invalid_query.stop()


if __name__ == "__main__":
    main()
