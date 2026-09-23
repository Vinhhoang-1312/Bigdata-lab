"""Kafka source backlog metrics for Spark Structured Streaming.

Semantics:
  Calculated as: backlog_p = latestKafkaOffset_p - sparkProcessedEndOffset_p
  Total backlog = sum(backlog_p)

Spark Structured Streaming manages its own offsets in its checkpoint metadata;
it does NOT commit consumer offsets to Kafka consumer groups during normal
streaming execution. Therefore, this metric measures source backlog, not
standard consumer-group lag.
"""
from __future__ import annotations

import os
from collections import deque
from typing import Any

# Rolling history of total backlog for trend analysis (last 5 observations)
_backlog_history: deque[int] = deque(maxlen=5)


def record_backlog_observation(total: int | None) -> None:
    """Record a backlog observation into rolling history."""
    if total is not None:
        _backlog_history.append(total)


def compute_backlog_trend(history: list[int] | None = None) -> str | None:
    """Compute trend from history: 'increasing', 'decreasing', 'stable', or None."""
    vals = history if history is not None else list(_backlog_history)
    if len(vals) < 2:
        return None
    # All increasing
    if all(vals[i] > vals[i - 1] for i in range(1, len(vals))):
        return "increasing"
    # All decreasing
    if all(vals[i] < vals[i - 1] for i in range(1, len(vals))):
        return "decreasing"
    # Mixed or equal
    diff = vals[-1] - vals[0]
    if diff > 5:
        return "increasing"
    if diff < -5:
        return "decreasing"
    return "stable"


def fetch_live_kafka_latest_offsets(
    bootstrap_servers: str = "localhost:9094",
    topic: str = "urbanflow.trip-events",
    num_partitions: int = 4,
) -> dict[str, int] | None:
    """Query Kafka broker for the current end offsets of each partition."""
    try:
        from kafka import KafkaConsumer, TopicPartition

        consumer = KafkaConsumer(
            bootstrap_servers=[bootstrap_servers],
            request_timeout_ms=1500,
        )
        tps = [TopicPartition(topic, p) for p in range(num_partitions)]
        end_offsets = consumer.end_offsets(tps)
        consumer.close()
        return {str(tp.partition): int(offset) for tp, offset in end_offsets.items()}
    except Exception:
        return None


def build_kafka_backlog_report(
    telemetry: dict[str, Any] | None,
    topic: str = "urbanflow.trip-events",
) -> dict[str, Any]:
    """Build the Kafka Source Backlog section of the Glass Cockpit contract."""
    semantics_note = (
        "Calculated as (latestKafkaOffset - sparkProcessedEndOffset). "
        "Spark SSS manages its own offsets in checkpoint metadata, not committed consumer-group offsets."
    )

    if not telemetry:
        return {
            "topic": topic,
            "metric_name": "Kafka Source Backlog",
            "total_backlog_rows": None,
            "per_partition": None,
            "trend": None,
            "semantics_note": semantics_note,
        }

    # 1. Check if telemetry has already calculated per-partition backlog
    per_partition = telemetry.get("kafka_per_partition_backlog")
    total_backlog = telemetry.get("kafka_total_backlog_rows")

    # 2. If not, try calculating from spark_end and latest offsets in telemetry
    if per_partition is None:
        part_end = telemetry.get("kafka_partition_end_offsets") or {}
        part_latest = telemetry.get("kafka_partition_latest_offsets") or {}

        # Optionally enrich with live Kafka probe if host has access
        if not part_latest:
            kafka_host = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
            live_latest = fetch_live_kafka_latest_offsets(bootstrap_servers=kafka_host, topic=topic)
            if live_latest:
                part_latest = live_latest

        if part_end:
            per_partition = {}
            total_backlog = 0
            for p_str, end_off in part_end.items():
                lat_off = part_latest.get(p_str, end_off)
                diff = max(0, lat_off - end_off)
                total_backlog += diff
                per_partition[p_str] = {
                    "latest_offset": lat_off,
                    "spark_end_offset": end_off,
                    "backlog": diff,
                }

    record_backlog_observation(total_backlog)
    trend = compute_backlog_trend()

    return {
        "topic": topic,
        "metric_name": "Kafka Source Backlog",
        "total_backlog_rows": total_backlog,
        "per_partition": per_partition,
        "trend": trend,
        "semantics_note": semantics_note,
    }
