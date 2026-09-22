#!/usr/bin/env python3
"""Publish Foundation Kafka fixtures, then verify Spark can consume them."""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import uuid
from pathlib import Path
from typing import NamedTuple

STREAMING_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = STREAMING_ROOT.parent
if str(STREAMING_ROOT) not in sys.path:
    sys.path.insert(0, str(STREAMING_ROOT))


class KafkaMessage(NamedTuple):
    key: str
    value: str
    event: dict


def render_event(event: dict) -> str:
    return json.dumps(event, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def load_fixture_messages(fixture_path: str | Path, run_id: str) -> list[KafkaMessage]:
    messages: list[KafkaMessage] = []
    with Path(fixture_path).open(encoding="utf-8") as handle:
        for line in handle:
            event = json.loads(line)
            event["event_id"] = f"{event['event_id']}::{run_id}"
            messages.append(
                KafkaMessage(
                    key=str(event["pickup_zone_id"]),
                    value=render_event(event),
                    event=event,
                )
            )
    return messages


def malformed_message(run_id: str) -> KafkaMessage:
    value = render_event({"event_id": "", "event_time": f"bad-{run_id}"})
    return KafkaMessage(key="malformed", value=value, event={})


def table_name(prefix: str, run_id: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_]", "_", run_id)
    return f"{prefix}_{safe}"


def publish_messages(
    spark,
    bootstrap_servers: str,
    topic: str,
    messages: list[KafkaMessage],
) -> None:
    rows = [(message.key, message.value) for message in messages]
    frame = spark.createDataFrame(rows, ["key", "value"])
    (
        frame.write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", topic)
        .save()
    )


def kafka_values(spark, bootstrap_servers: str, topic: str):
    from pyspark.sql import functions as F

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(F.col("value"))
    )


def write_available_now(frame, table: str, timeout_seconds: int) -> None:
    query = (
        frame.writeStream.format("memory")
        .queryName(table)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    finished = query.awaitTermination(timeout_seconds)
    if not finished:
        query.stop()
        raise TimeoutError(f"Timed out waiting for streaming query {table}")


def verify_valid_events(
    spark,
    bootstrap_servers: str,
    topic: str,
    messages: list[KafkaMessage],
    run_id: str,
) -> dict:
    from pyspark.sql import functions as F
    from spark.event_schema import parse_events

    parsed = parse_events(kafka_values(spark, bootstrap_servers, topic))
    valid_table = table_name("kafka_valid", run_id)
    invalid_table = table_name("kafka_invalid", run_id)
    marker = f"::{run_id}"

    write_available_now(parsed.valid.where(F.col("event_id").endswith(marker)), valid_table, 60)
    write_available_now(
        parsed.invalid.where(F.col("raw_value").contains(run_id)),
        invalid_table,
        60,
    )

    valid_rows = [row.asDict() for row in spark.table(valid_table).collect()]
    invalid_rows = [row.asDict() for row in spark.table(invalid_table).collect()]
    expected_events = [message.event for message in messages]

    if len(valid_rows) != 7:
        raise AssertionError(f"valid_count expected 7, got {len(valid_rows)}")
    if invalid_rows:
        raise AssertionError(f"invalid_count expected 0, got {len(invalid_rows)}")

    actual_ids = {row["event_id"] for row in valid_rows}
    expected_ids = {event["event_id"] for event in expected_events}
    if actual_ids != expected_ids:
        raise AssertionError("Kafka valid event ids do not match fixture ids")

    actual_offsets = {row["event_time"] for row in valid_rows}
    expected_offsets = {event["event_time"] for event in expected_events}
    if actual_offsets != expected_offsets:
        raise AssertionError("Event-time strings lost their explicit offsets")

    return {
        "valid_count": len(valid_rows),
        "invalid_count": len(invalid_rows),
        "pickup_zone_ids": sorted({row["pickup_zone_id"] for row in valid_rows}),
        "event_times": sorted(actual_offsets),
    }


def verify_malformed_event(spark, bootstrap_servers: str, topic: str, run_id: str) -> dict:
    from pyspark.sql import functions as F
    from spark.event_schema import parse_events

    parsed = parse_events(kafka_values(spark, bootstrap_servers, topic))
    table = table_name("kafka_bad", run_id)
    write_available_now(parsed.invalid.where(F.col("raw_value").contains(run_id)), table, 60)
    rows = [row.asDict() for row in spark.table(table).collect()]
    if len(rows) != 1:
        raise AssertionError(f"malformed invalid_count expected 1, got {len(rows)}")
    row = rows[0]
    if not row["raw_value"] or not row["validation_error"]:
        raise AssertionError("Malformed row must keep raw_value and validation_error")
    return {
        "invalid_count": 1,
        "validation_error": row["validation_error"],
        "raw_value_non_empty": bool(row["raw_value"]),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="urbanflow.trip-events")
    parser.add_argument("--invalid-topic", default="urbanflow.trip-events.smoke-invalid")
    parser.add_argument(
        "--fixture",
        default=str(REPO_ROOT / "urbanflow-foundation/fixtures/kafka/trip_events.jsonl"),
    )
    parser.add_argument("--run-id", default=f"smoke-{uuid.uuid4().hex[:10]}")
    return parser.parse_args()


def main() -> int:
    from pyspark.sql import SparkSession

    args = parse_args()
    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    spark = (
        SparkSession.builder.appName("UrbanFlow Kafka Spark Smoke")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        messages = load_fixture_messages(args.fixture, args.run_id)
        publish_messages(spark, args.bootstrap_servers, args.topic, messages)
        publish_messages(
            spark,
            args.bootstrap_servers,
            args.invalid_topic,
            [malformed_message(args.run_id)],
        )
        valid = verify_valid_events(
            spark,
            args.bootstrap_servers,
            args.topic,
            messages,
            args.run_id,
        )
        malformed = verify_malformed_event(
            spark,
            args.bootstrap_servers,
            args.invalid_topic,
            args.run_id,
        )
        summary = {
            "result": "Kafka -> Spark = PASS",
            "run_id": args.run_id,
            "started_at": started_at,
            "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "bootstrap_servers": args.bootstrap_servers,
            "topic": args.topic,
            "invalid_topic": args.invalid_topic,
            "valid": valid,
            "malformed": malformed,
        }
        print(json.dumps(summary, sort_keys=True))
        print("Kafka -> Spark = PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
