#!/usr/bin/env python3
"""Verify a bounded Replay -> Kafka -> Streaming -> Live Gold run."""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path
from typing import Any

STREAMING_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = STREAMING_ROOT.parent
if str(STREAMING_ROOT) not in sys.path:
    sys.path.insert(0, str(STREAMING_ROOT))

DEFAULT_TOPIC = "urbanflow.trip-events"
DEFAULT_BOOTSTRAP = "kafka:9092"
DEFAULT_HISTORICAL = "s3a://urbanflow/gold/historical_zone_baseline/"
DEFAULT_RESULT_MD = (
    STREAMING_ROOT / "docs" / "results" / "end-to-end-streaming.md"
)


def run_marker(run_id: str) -> str:
    return f"-{run_id}"


def safe_run_id(run_id: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in run_id)


def default_output_path(run_id: str) -> str:
    return f"s3a://urbanflow/gold/live_zone_metrics/task6/{safe_run_id(run_id)}/"


def default_checkpoint_path(run_id: str) -> str:
    return (
        "s3a://urbanflow/checkpoints/live_trip_metrics/task6/"
        f"{safe_run_id(run_id)}/"
    )


def kafka_values_batch(spark, bootstrap_servers: str, topic: str):
    from pyspark.sql import functions as F

    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(F.col("value"))
    )


def kafka_values_stream(spark, bootstrap_servers: str, topic: str):
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


def static_reference(valid, historical, watermark_minutes: int):
    from pyspark.sql import functions as F
    from spark.live_trip_metrics import join_historical

    max_event_timestamp = valid.agg(F.max("event_timestamp")).first()[0]
    if max_event_timestamp is None:
        raise ValueError("No valid events matched the run id")

    cutoff = max_event_timestamp - __import__("datetime").timedelta(
        minutes=watermark_minutes
    )
    current = (
        valid.groupBy(F.window("event_timestamp", "5 minutes"), "pickup_zone_id")
        .agg(F.count(F.lit(1)).cast("long").alias("current_trip_count"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "pickup_zone_id",
            "current_trip_count",
        )
        .where(F.col("window_end") <= F.lit(cutoff))
    )
    return join_historical(current, historical)


def write_streaming_live_gold(
    spark,
    bootstrap_servers: str,
    topic: str,
    run_id: str,
    historical,
    output_path: str,
    checkpoint_path: str,
    timeout_seconds: int,
):
    from pyspark.sql import functions as F
    from spark.event_schema import parse_events
    from spark.live_trip_metrics import aggregate_current, join_historical

    parsed = parse_events(kafka_values_stream(spark, bootstrap_servers, topic))
    valid = parsed.valid.where(F.col("event_id").endswith(run_marker(run_id)))
    gold = join_historical(aggregate_current(valid), historical)
    query = (
        gold.writeStream.format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )
    finished = query.awaitTermination(timeout_seconds)
    if not finished:
        query.stop()
        raise TimeoutError("Timed out waiting for integrated streaming query")
    return query.lastProgress


def comparable_rows(frame) -> dict[tuple[Any, Any, int], dict[str, Any]]:
    rows = {}
    for row in frame.collect():
        item = row.asDict()
        key = (item["window_start"], item["window_end"], int(item["pickup_zone_id"]))
        rows[key] = item
    return rows


def compare_frames(reference, actual) -> list[str]:
    expected = comparable_rows(reference)
    observed = comparable_rows(actual)
    mismatches: list[str] = []
    for key in sorted(set(expected) | set(observed)):
        if key not in expected:
            mismatches.append(f"unexpected key {key}")
            continue
        if key not in observed:
            mismatches.append(f"missing key {key}")
            continue
        left = expected[key]
        right = observed[key]
        if left["current_trip_count"] != right["current_trip_count"]:
            mismatches.append(f"count mismatch {key}")
        for name in ("historical_mean_trip_count", "demand_ratio", "z_score"):
            l_value = left[name]
            r_value = right[name]
            if l_value is None or r_value is None:
                if l_value is not None or r_value is not None:
                    mismatches.append(f"{name} null mismatch {key}")
            elif not math.isclose(
                float(l_value),
                float(r_value),
                rel_tol=1e-9,
                abs_tol=1e-9,
            ):
                mismatches.append(f"{name} mismatch {key}")
    return mismatches


def verify(args: argparse.Namespace) -> dict[str, Any]:
    from pyspark.sql import SparkSession, functions as F
    from spark.event_schema import parse_events
    from spark.historical_lookup import load_historical_baseline

    spark = (
        SparkSession.builder.appName("UrbanFlow Integrated Run Verifier")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        marker = run_marker(args.run_id)
        historical = load_historical_baseline(spark, args.historical_path)
        parsed_batch = parse_events(
            kafka_values_batch(spark, args.bootstrap_servers, args.topic)
        )
        valid = parsed_batch.valid.where(F.col("event_id").endswith(marker)).cache()
        invalid = parsed_batch.invalid.where(F.col("raw_value").contains(args.run_id))
        replay_events = valid.count()
        invalid_rows = invalid.count()
        if replay_events != args.expected_events:
            raise AssertionError(
                f"Expected {args.expected_events} replay events, got {replay_events}"
            )
        if invalid_rows:
            raise AssertionError(f"Expected 0 invalid rows, got {invalid_rows}")

        progress = write_streaming_live_gold(
            spark,
            args.bootstrap_servers,
            args.topic,
            args.run_id,
            historical,
            args.output_path,
            args.checkpoint_path,
            args.timeout_seconds,
        )
        reference = static_reference(valid, historical, args.watermark_minutes)
        actual = spark.read.parquet(args.output_path).select(*reference.columns)
        mismatches = compare_frames(reference, actual)
        live_rows = actual.count()
        reference_rows = reference.count()
        status = "PASS" if not mismatches and live_rows == reference_rows else "FAIL"
        return {
            "status": status,
            "run_id": args.run_id,
            "topic": args.topic,
            "bootstrap_servers": args.bootstrap_servers,
            "replay_events": replay_events,
            "invalid_rows": invalid_rows,
            "reference_rows": reference_rows,
            "live_rows": live_rows,
            "mismatched_rows": len(mismatches),
            "mismatch_examples": mismatches[:10],
            "output_path": args.output_path,
            "checkpoint_path": args.checkpoint_path,
            "streaming_progress": progress,
            "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "limitations": [
                "Run-scoped Live Gold path used for safe verification.",
                "Replay was run before availableNow consumption in this bounded verifier.",
                "Streamlit visual smoke is recorded separately when the app is launched.",
            ],
        }
    finally:
        spark.stop()


def render_markdown(summary: dict[str, Any]) -> str:
    limitations = "\n".join(
        f"- {item}" for item in summary.get("limitations", [])
    )
    return "\n".join(
        [
            "# End-to-End Streaming Result",
            "",
            f"Date: {summary.get('checked_at', 'not-run')}",
            f"Status: `{summary['status']}`",
            f"Run ID: `{summary['run_id']}`",
            "",
            "## Summary",
            "",
            f"- Replay events: `{summary.get('replay_events')}`",
            f"- Invalid rows: `{summary.get('invalid_rows')}`",
            f"- Reference rows: `{summary.get('reference_rows')}`",
            f"- Live rows: `{summary.get('live_rows')}`",
            f"- Mismatched rows: `{summary.get('mismatched_rows')}`",
            f"- Output path: `{summary.get('output_path')}`",
            f"- Checkpoint path: `{summary.get('checkpoint_path')}`",
            "",
            "## Mismatch Examples",
            "",
            "```json",
            json.dumps(summary.get("mismatch_examples", []), indent=2),
            "```",
            "",
            "## Streaming Progress",
            "",
            "```json",
            json.dumps(
                json_safe(summary.get("streaming_progress")),
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
            "## Limitations",
            "",
            limitations,
        ]
    )


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def write_result(path: str, summary: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_markdown(summary) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected-events", type=int, default=10000)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--historical-path", default=DEFAULT_HISTORICAL)
    parser.add_argument("--output-path")
    parser.add_argument("--checkpoint-path")
    parser.add_argument("--watermark-minutes", type=int, default=10)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--result-md", default=str(DEFAULT_RESULT_MD))
    args = parser.parse_args()
    args.output_path = args.output_path or default_output_path(args.run_id)
    args.checkpoint_path = args.checkpoint_path or default_checkpoint_path(
        args.run_id
    )
    return args


def main() -> int:
    try:
        summary = verify(parse_args())
    except Exception as exc:  # noqa: BLE001 - CLI records failure evidence.
        args = parse_args()
        summary = {
            "status": "FAIL",
            "run_id": args.run_id,
            "reason": str(exc).splitlines()[0],
            "replay_events": None,
            "invalid_rows": None,
            "reference_rows": None,
            "live_rows": None,
            "mismatched_rows": None,
            "mismatch_examples": [],
            "output_path": args.output_path,
            "checkpoint_path": args.checkpoint_path,
            "streaming_progress": None,
            "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "limitations": ["Verifier failed before completing comparison."],
        }
    write_result(parse_args().result_md, summary)
    print(json.dumps(json_safe(summary), sort_keys=True))
    print(f"END TO END STREAMING = {summary['status']}")
    return 0 if summary["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
