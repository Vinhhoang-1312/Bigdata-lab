"""Read-only status models built from Streaming Gold outputs and Spark telemetry."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from analytics_app.data_access import GoldReader, gold_freshness_age_seconds, prepare_live_view
from observability.governance import build_data_governance_report
from observability.health_state import classify_health
from observability.kafka_backlog import build_kafka_backlog_report
from observability.telemetry_reader import telemetry_age_seconds
from observability.watermark import build_watermark_status


def _iso(value: pd.Timestamp) -> str:
    return value.to_pydatetime().astimezone(timezone.utc).isoformat()


def build_streaming_overview(reader: GoldReader, lookup: pd.DataFrame) -> dict[str, Any]:
    """Summarize the latest completed Live Gold window without reproducing ETL."""
    view = prepare_live_view(reader.live(), lookup)
    if view.empty:
        return {"status": "empty", "latest_window": None, "latest_rows": [], "current_trip_count": 0}
    latest = view[view["window_start"] == view["window_start"].max()].copy()
    columns = [
        "pickup_zone_id",
        "zone_name",
        "current_trip_count",
        "historical_mean_trip_count",
        "demand_ratio",
        "z_score",
        "is_high_demand",
    ]
    serializable = latest.loc[:, columns].sort_values("demand_ratio", ascending=False)
    serializable = serializable.astype(object).where(pd.notna(serializable), None)
    return {
        "status": "available",
        "latest_window": _iso(latest["window_start"].iloc[0]),
        "current_trip_count": int(latest["current_trip_count"].sum()),
        "high_demand_zone_count": int(latest["is_high_demand"].sum()),
        "latest_rows": serializable.to_dict("records"),
    }


def build_streaming_health(
    reader: GoldReader,
    telemetry: dict[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    """Construct full Glass Cockpit health contract while preserving backwards compatibility."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must include a timezone")

    # Read live gold
    live = reader.live()
    if live.empty:
        latest_end_iso = None
        age_seconds = None
        freshness_basis = "unavailable"
        row_count = 0
    else:
        latest_end = live["window_end"].max()
        latest_end_iso = _iso(latest_end)
        age_seconds, freshness_basis = gold_freshness_age_seconds(live, pd.Timestamp(now))
        row_count = int(len(live))

    # Health classification
    health = classify_health(telemetry, age_seconds, now)
    state = health["state"]
    reason = health["reason"]

    # Pipeline configuration metadata
    pipeline_meta = {
        "engine": "Apache Spark Structured Streaming",
        "version": "4.2.0",
        "master": "local[2]",
        "execution_mode": "local driver execution (local[2])",
        "cluster_topology": (
            "Standalone cluster (master:8080, workers:8081/8082) is idle infrastructure; "
            "streaming query executes locally on driver (port 4040)"
        ),
        "session_timezone": "America/New_York",
        "window_type": "tumbling",
        "window_duration": os.getenv("URBANFLOW_WINDOW_DURATION", "5 minutes"),
        "slide_duration": None,
        "watermark_delay": os.getenv("URBANFLOW_WATERMARK_DELAY", "10 minutes"),
    }

    # Spark telemetry
    spark_telemetry = None
    if telemetry:
        tel_age = telemetry_age_seconds(telemetry, now)
        spark_telemetry = {
            "query_active": telemetry.get("query_active", False),
            "batch_id": telemetry.get("batch_id", -1),
            "input_rows_per_second": telemetry.get("input_rows_per_second", 0.0),
            "processed_rows_per_second": telemetry.get("processed_rows_per_second", 0.0),
            "num_input_rows": telemetry.get("num_input_rows", 0),
            "batch_duration_ms": telemetry.get("batch_duration_ms", 0),
            "trigger_interval_ms": telemetry.get("trigger_interval_ms", 5000),
            "trigger_load": health["trigger_load"],
            "throughput_ratio": health["throughput_ratio"],
            "telemetry_age_seconds": tel_age,
        }

    # Watermark status
    watermark_status = build_watermark_status(
        telemetry, configured_delay=pipeline_meta["watermark_delay"]
    )

    # Kafka backlog
    kafka_backlog = build_kafka_backlog_report(
        telemetry, topic=os.getenv("URBANFLOW_KAFKA_TOPIC", "urbanflow.trip-events")
    )

    # Data governance
    data_governance = build_data_governance_report(telemetry)

    # Live Gold freshness section
    live_gold_freshness = {
        "latest_window_end": latest_end_iso,
        "age_seconds": age_seconds,
        "basis": freshness_basis,
        "row_count": row_count,
    }

    # Return full contract including backwards-compatible fields
    return {
        "status": state,
        "reason": reason,
        "pipeline": pipeline_meta,
        "spark_telemetry": spark_telemetry,
        "watermark_status": watermark_status,
        "kafka_backlog": kafka_backlog,
        "data_governance": data_governance,
        "live_gold_freshness": live_gold_freshness,
        # Backwards-compatible legacy fields
        "telemetry_status": "unavailable" if telemetry is None else "available",
        "invalid_record_count": None if telemetry is None else telemetry.get("invalid_events_total"),
        "live_gold_age_seconds": age_seconds,
        "latest_window_end": latest_end_iso,
        "live_gold_row_count": row_count,
    }
