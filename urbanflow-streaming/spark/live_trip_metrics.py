"""UrbanFlow event-time aggregation, historical comparison, and stream runner."""
from __future__ import annotations

import argparse
from pathlib import Path
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession, functions as F

try:  # Supports both package imports and spark-submit of this file.
    from .event_schema import ParsedEvents, parse_events
    from .historical_lookup import KEY, load_historical_baseline
    from .streaming_metrics import UrbanFlowMetricsListener
    from .telemetry_writer import start_telemetry_writer
except ImportError:
    from event_schema import ParsedEvents, parse_events
    from historical_lookup import KEY, load_historical_baseline
    from streaming_metrics import UrbanFlowMetricsListener
    from telemetry_writer import start_telemetry_writer

ANALYTICS_TIMEZONE = "America/New_York"
DEFAULT_WATERMARK = "10 minutes"
LIVE_COLUMNS = [
    "window_start", "window_end", "pickup_zone_id", "current_trip_count",
    "historical_mean_trip_count", "demand_ratio", "z_score",
]
REPO_ROOT = Path(__file__).resolve().parents[2]
FOUNDATION_FIXTURES = (REPO_ROOT / "urbanflow-foundation" / "fixtures").resolve()


def configure_session(spark: SparkSession) -> SparkSession:
    spark.conf.set("spark.sql.session.timeZone", ANALYTICS_TIMEZONE)
    spark.conf.set("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    return spark


def aggregate_current(
    events: DataFrame,
    watermark: str = DEFAULT_WATERMARK,
    window_duration: str = "5 minutes",
    slide_duration: str | None = None,
) -> DataFrame:
    """Count pickups in clock-aligned event-time tumbling or sliding windows."""
    window_spec = (
        F.window("event_timestamp", window_duration, slide_duration)
        if slide_duration
        else F.window("event_timestamp", window_duration)
    )
    return (
        events.withWatermark("event_timestamp", watermark)
        .groupBy(window_spec, "pickup_zone_id")
        .agg(F.count(F.lit(1)).cast("long").alias("current_trip_count"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "pickup_zone_id",
            "current_trip_count",
        )
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


def transform(
    source: DataFrame,
    historical: DataFrame,
    watermark: str = DEFAULT_WATERMARK,
    window_duration: str = "5 minutes",
    slide_duration: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Transform a Kafka-like value DataFrame; return Live Gold and invalid rows."""
    parsed: ParsedEvents = parse_events(source)
    return (
        join_historical(
            aggregate_current(
                parsed.valid,
                watermark=watermark,
                window_duration=window_duration,
                slide_duration=slide_duration,
            ),
            historical,
        ),
        parsed.invalid,
    )


def read_kafka(spark: SparkSession, bootstrap_servers: str, topic: str) -> DataFrame:
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic).option("startingOffsets", "latest").load())


def _same_path(left: str, right: str) -> bool:
    return left.rstrip("/\\") == right.rstrip("/\\")


def _is_local_path(path: str) -> bool:
    return not urlparse(path).scheme


def _resolved(path: str) -> Path:
    return Path(path).expanduser().resolve(strict=False)


def _is_under(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def validate_runtime_paths(output_path: str, checkpoint_path: str) -> None:
    """Reject equal paths and generated output below Foundation fixtures."""
    if _same_path(output_path, checkpoint_path):
        raise ValueError("Live Gold and checkpoint paths must be different")

    for label, value in (("Live Gold", output_path), ("checkpoint", checkpoint_path)):
        if _is_local_path(value) and _is_under(_resolved(value), FOUNDATION_FIXTURES):
            raise ValueError(f"{label} generated output must not be below Foundation fixtures")


def write_live_gold(
    metrics: DataFrame,
    output_path: str,
    checkpoint_path: str,
    trigger: str = "10 seconds",
    available_now: bool = False,
):
    """Start restart-safe Parquet output; Spark's checkpoint tracks committed batches."""
    validate_runtime_paths(output_path, checkpoint_path)
    writer = (metrics.writeStream.format("parquet").outputMode("append")
              .option("path", output_path).option("checkpointLocation", checkpoint_path))
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=trigger)
    return writer.start()


def write_invalid_rows(
    invalid: DataFrame,
    trigger: str = "10 seconds",
    available_now: bool = False,
):
    writer = invalid.writeStream.format("console").outputMode("append").queryName("invalid_events")
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=trigger)
    return writer.start()


def _settings(path: str) -> dict:
    import yaml

    with open(path, encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(Path(__file__).parents[1] / "config/settings.yaml"))
    parser.add_argument("--fixture-events", help="Run a deterministic available-now file stream")
    parser.add_argument(
        "--available-now", action="store_true", help="Run a finite available-now stream"
    )
    args = parser.parse_args()
    cfg = _settings(args.config)
    spark = configure_session(SparkSession.builder.appName("UrbanFlowLiveMetrics").getOrCreate())
    if not (args.fixture_events or args.available_now):
        listener = UrbanFlowMetricsListener()
        spark.streams.addListener(listener)
    historical = load_historical_baseline(spark, cfg["storage"]["historical_baseline"])
    if args.fixture_events:
        # File streams require a directory; copy fixtures to an input directory in real runs.
        source = spark.readStream.format("text").load(args.fixture_events).select(F.col("value"))
    else:
        source = read_kafka(spark, cfg["kafka"]["bootstrap_servers"], cfg["kafka"]["topic"])
    streaming_cfg = cfg.get("streaming", {})
    watermark = streaming_cfg.get("watermark", DEFAULT_WATERMARK)
    window_duration = streaming_cfg.get("window_duration", "5 minutes")
    slide_duration = streaming_cfg.get("slide_duration")
    gold, invalid = transform(
        source,
        historical,
        watermark=watermark,
        window_duration=window_duration,
        slide_duration=slide_duration,
    )
    available_now = bool(args.fixture_events or args.available_now)
    validate_runtime_paths(cfg["storage"]["live_metrics"], cfg["storage"]["streaming_checkpoint"])
    invalid_query = write_invalid_rows(invalid, cfg["streaming"]["trigger"], available_now)
    query = write_live_gold(gold, cfg["storage"]["live_metrics"],
                            cfg["storage"]["streaming_checkpoint"],
                            cfg["streaming"]["trigger"], available_now)
    
    if not available_now:
        telemetry_path = cfg.get("telemetry", {}).get("heartbeat_path", "/workspace/.telemetry/streaming_health.json")
        _telemetry_thread = start_telemetry_writer(listener, telemetry_path)

    try:
        query.awaitTermination()
        if available_now:
            invalid_query.awaitTermination()
    finally:
        invalid_query.stop()


if __name__ == "__main__":
    main()
