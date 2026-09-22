import json
from datetime import datetime

def test_fixture_matches_contract(spark, foundation):
    from spark.historical_lookup import load_historical_baseline
    from spark.live_trip_metrics import aggregate_current, join_historical
    from spark.event_schema import parse_events

    events = spark.read.text(str(foundation / "fixtures/kafka/trip_events.jsonl"))
    parsed = parse_events(events)
    assert parsed.invalid.count() == 0
    # Static DataFrames cannot carry watermarks but use exactly the production transforms.
    current = aggregate_current(parsed.valid)
    baseline = load_historical_baseline(spark, str(foundation / "fixtures/gold/historical_zone_baseline.jsonl"))
    actual = [r.asDict(recursive=True) for r in join_historical(current, baseline).orderBy("pickup_zone_id").collect()]
    expected = [json.loads(line) for line in (foundation / "fixtures/gold/expected_live_zone_metrics.jsonl").read_text().splitlines()]
    assert [(r["pickup_zone_id"], r["current_trip_count"], r["demand_ratio"], r["z_score"]) for r in actual] == [
        (r["pickup_zone_id"], r["current_trip_count"], r["demand_ratio"], r["z_score"]) for r in expected]
    # PySpark materializes timestamp instants to Python in UTC; analytical key derivation
    # and Spark display/serialization remain in the configured New York session zone.
    assert actual[0]["window_start"] == datetime(2025, 1, 6, 13, 0)
    assert actual[0]["window_end"] == datetime(2025, 1, 6, 13, 5)


def test_malformed_events_remain_visible(spark):
    from spark.event_schema import parse_events
    rows = [('not json',), ('{"event_id":"","event_time":"bad"}',),
            ('{"event_id":"x","event_time":"2025-01-06T08:00:00-05:00","pickup_zone_id":1,"dropoff_zone_id":2,"trip_distance":1,"trip_duration_minutes":2,"fare_amount":3,"extra":4}',)]
    result = parse_events(spark.createDataFrame(rows, ["value"]))
    assert result.valid.count() == 0
    assert result.invalid.count() == 3
    assert all(r.raw_value for r in result.invalid.collect())


def test_boundary_missing_baseline_and_null_rules(spark):
    from spark.event_schema import parse_events
    from spark.historical_lookup import HISTORICAL_SCHEMA
    from spark.live_trip_metrics import aggregate_current, join_historical

    base = [(1, 1, 8, 0, 0.0, 0.0, None, None), (2, 1, 8, 1, 2.0, 0.0, None, None)]
    baseline = spark.createDataFrame(base, HISTORICAL_SCHEMA)
    def event(eid, at, zone):
        return (json.dumps(dict(event_id=eid, event_time=at, pickup_zone_id=zone, dropoff_zone_id=1,
                                trip_distance=0, trip_duration_minutes=0, fare_amount=0)),)
    source = spark.createDataFrame([event("a", "2025-01-06T08:04:59-05:00", 1),
                                    event("b", "2025-01-06T08:05:00-05:00", 2),
                                    event("c", "2025-01-06T08:05:01-05:00", 3)], ["value"])
    out = {r.pickup_zone_id: r for r in join_historical(aggregate_current(parse_events(source).valid), baseline).collect()}
    assert out[1].window_start.minute == 0 and out[2].window_start.minute == 5
    assert out[1].demand_ratio is None and out[1].z_score is None
    assert out[2].demand_ratio == 0.5 and out[2].z_score is None
    assert out[3].historical_mean_trip_count is None and out[3].demand_ratio is None


def test_watermark_is_attached(spark):
    from spark.event_schema import parse_events
    from spark.live_trip_metrics import aggregate_current
    stream = spark.readStream.format("rate").load().selectExpr("cast(null as string) value")
    plan = aggregate_current(parse_events(stream).valid, "10 minutes")._jdf.queryExecution().logical().toString()
    assert "EventTimeWatermark" in plan and "10 minutes" in plan


def test_checkpoint_restart_does_not_duplicate_files(spark, tmp_path):
    """A restarted source/checkpoint does not process its first input again."""
    from pyspark.sql import functions as F

    source, output, checkpoint = (tmp_path / name for name in ("in", "out", "checkpoint"))
    source.mkdir()
    payload = json.dumps(dict(event_id="once", event_time="2025-01-06T08:00:00-05:00",
                              pickup_zone_id=1, dropoff_zone_id=1, trip_distance=1,
                              trip_duration_minutes=1, fare_amount=1))
    (source / "batch-1.jsonl").write_text(payload + "\n")

    def run_once():
        raw = spark.readStream.format("text").load(str(source))
        # Exercise Spark's exact Parquet/checkpoint commit protocol independently from
        # watermark finalization (which intentionally needs a later event-time batch).
        metrics = raw.select(
            F.to_timestamp(F.lit("2025-01-06T08:00:00-05:00")).alias("window_start"),
            F.to_timestamp(F.lit("2025-01-06T08:05:00-05:00")).alias("window_end"),
            F.lit(1).cast("int").alias("pickup_zone_id"),
            F.lit(1).cast("long").alias("current_trip_count"),
            F.lit(1.0).alias("historical_mean_trip_count"), F.lit(1.0).alias("demand_ratio"),
            F.lit(0.0).alias("z_score"),
        )
        query = (metrics.writeStream.format("parquet").outputMode("append")
                 .option("path", str(output)).option("checkpointLocation", str(checkpoint))
                 .trigger(availableNow=True).start())
        query.awaitTermination()

    run_once()
    assert spark.read.parquet(str(output)).count() == 1
    run_once()
    assert spark.read.parquet(str(output)).count() == 1
