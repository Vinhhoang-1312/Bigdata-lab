"""Unit and contract tests for UrbanFlow Glass Cockpit Observability."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import unittest

from observability.governance import build_data_governance_report
from observability.health_state import HealthState, classify_health
from observability.kafka_backlog import build_kafka_backlog_report, compute_backlog_trend
from observability.watermark import build_watermark_status
from serving.streaming_service import build_streaming_health
from analytics_app.data_access import GoldReader

ROOT = Path(__file__).resolve().parents[2]
FOUNDATION = ROOT / "urbanflow-foundation" / "fixtures"
NOW = datetime(2025, 1, 6, 13, 10, tzinfo=timezone.utc)


class GlassCockpitObservabilityTest(unittest.TestCase):
    def test_progress_json_parsing(self):
        """StreamingQueryProgress JSON must extract rates, watermark, durations, and partition offsets."""
        from spark.streaming_metrics import snapshot_from_progress

        sample_progress = {
            "name": "live_zone_metrics",
            "batchId": 42,
            "numInputRows": 622,
            "inputRowsPerSecond": 124.5,
            "processedRowsPerSecond": 312.0,
            "durationMs": {
                "triggerExecution": 1980,
                "addBatch": 1500,
            },
            "eventTime": {
                "watermark": "2023-02-01T08:05:00.000Z",
                "max": "2023-02-01T08:15:00.000Z",
            },
            "sources": [
                {
                    "description": "KafkaV2[Subscribe[urbanflow.trip-events]]",
                    "endOffset": json.dumps({"urbanflow.trip-events": {"0": 1000, "1": 1010, "2": 1000, "3": 1010}}),
                    "latestOffset": json.dumps({"urbanflow.trip-events": {"0": 1050, "1": 1040, "2": 1035, "3": 1045}}),
                }
            ],
            "sink": {"numOutputRows": 50},
        }

        snap = snapshot_from_progress(sample_progress, invalid_events=2, trigger_interval_ms=5000)
        self.assertEqual(42, snap.batch_id)
        self.assertEqual(622, snap.input_rows)
        self.assertEqual(124.5, snap.input_rows_per_second)
        self.assertEqual(312.0, snap.processed_rows_per_second)
        self.assertEqual(1980, snap.batch_duration_ms)
        self.assertEqual(5000, snap.trigger_interval_ms)
        # trigger_load = 1980 / 5000 = 0.396
        self.assertEqual(0.396, snap.trigger_load)
        # throughput_ratio = 124.5 / 312.0 = 0.399
        self.assertEqual(0.399, snap.throughput_ratio)
        self.assertEqual("2023-02-01T08:05:00.000Z", snap.watermark)
        self.assertEqual("2023-02-01T08:15:00.000Z", snap.max_event_time)
        self.assertEqual(2, snap.invalid_events)
        self.assertEqual(50, snap.output_rows)
        self.assertEqual({"0": 1000, "1": 1010, "2": 1000, "3": 1010}, snap.kafka_partition_end_offsets)
        self.assertEqual({"0": 1050, "1": 1040, "2": 1035, "3": 1045}, snap.kafka_partition_latest_offsets)

    def test_trigger_load_and_throughput_ratio_semantic_separation(self):
        """trigger_load (duty cycle) and throughput_ratio (rate ratio) must be distinct metrics."""
        from spark.streaming_metrics import snapshot_from_progress

        progress = {
            "batchId": 1,
            "durationMs": {"triggerExecution": 2500},
            "inputRowsPerSecond": 100.0,
            "processedRowsPerSecond": 400.0,
        }
        # 2500 ms execution / 5000 ms interval = 0.500 trigger_load
        # 100 eps / 400 eps = 0.250 throughput_ratio
        snap = snapshot_from_progress(progress, trigger_interval_ms=5000)
        self.assertEqual(0.500, snap.trigger_load)
        self.assertEqual(0.250, snap.throughput_ratio)
        self.assertNotEqual(snap.trigger_load, snap.throughput_ratio)

    def test_backlog_calculation(self):
        """Kafka Source Backlog must compute (latest - spark_end) per partition and total."""
        telemetry = {
            "kafka_partition_end_offsets": {"0": 1000, "1": 1010, "2": 1000, "3": 1010},
            "kafka_partition_latest_offsets": {"0": 1050, "1": 1040, "2": 1035, "3": 1045},
        }
        report = build_kafka_backlog_report(telemetry)
        self.assertEqual("Kafka Source Backlog", report["metric_name"])
        self.assertEqual(150, report["total_backlog_rows"])
        per_part = report["per_partition"]
        self.assertEqual(50, per_part["0"]["backlog"])
        self.assertEqual(30, per_part["1"]["backlog"])
        self.assertEqual(35, per_part["2"]["backlog"])
        self.assertEqual(35, per_part["3"]["backlog"])
        self.assertIn("checkpoint metadata", report["semantics_note"])

    def test_backlog_trend(self):
        """Trend classification over history must detect increasing, decreasing, or stable."""
        self.assertEqual("increasing", compute_backlog_trend([10, 20, 30, 40]))
        self.assertEqual("decreasing", compute_backlog_trend([40, 30, 20, 10]))
        self.assertEqual("stable", compute_backlog_trend([20, 22, 19, 21]))
        self.assertIsNone(compute_backlog_trend([10]))

    def test_stale_heartbeat_is_unavailable(self):
        """Telemetry older than 60s must result in UNAVAILABLE."""
        now = datetime(2025, 1, 6, 13, 0, 0, tzinfo=timezone.utc)
        stale_heartbeat = {"timestamp": "2025-01-06T12:58:00+00:00", "batch_id": 1}
        res = classify_health(stale_heartbeat, live_gold_age_seconds=10, now_utc=now)
        self.assertEqual(HealthState.UNAVAILABLE.value, res["state"])
        self.assertIn("stale", res["reason"].lower())

    def test_active_producer_stuck_batch_is_stale(self):
        """When input is expected but batch_id is unchanged for 3 observations, report STALE."""
        from observability.health_state import _batch_id_history
        _batch_id_history.clear()

        now = datetime(2025, 1, 6, 13, 0, 0, tzinfo=timezone.utc)
        tel = {
            "timestamp": "2025-01-06T12:59:58+00:00",
            "batch_id": 5,
            "input_rows_per_second": 50.0,
            "processed_rows_per_second": 0.0,
            "kafka_total_backlog_rows": 200,
        }
        # 3 consecutive observations of batch 5 while input is active
        classify_health(tel, live_gold_age_seconds=10, now_utc=now)
        classify_health(tel, live_gold_age_seconds=10, now_utc=now)
        res = classify_health(tel, live_gold_age_seconds=10, now_utc=now)

        self.assertEqual(HealthState.STALE.value, res["state"])
        self.assertIn("not advanced", res["reason"])
        self.assertFalse(res["source_idle"])

    def test_inactive_idle_source_unchanged_batch_is_healthy(self):
        """Legitimate source idle (0 backlog, 0 eps) must be HEALTHY, not false-alarm STALE."""
        from observability.health_state import _batch_id_history
        _batch_id_history.clear()

        now = datetime(2025, 1, 6, 13, 0, 0, tzinfo=timezone.utc)
        tel_idle = {
            "timestamp": "2025-01-06T12:59:58+00:00",
            "query_active": True,
            "batch_id": 10,
            "input_rows_per_second": 0.0,
            "processed_rows_per_second": 0.0,
            "num_input_rows": 0,
            "kafka_total_backlog_rows": 0,
            "batch_duration_ms": 10,
            "trigger_interval_ms": 5000,
            "watermark": "2025-01-06T12:50:00Z",
        }
        # Repeated observations with live Gold age > 300s
        for _ in range(4):
            res = classify_health(tel_idle, live_gold_age_seconds=450, now_utc=now)

        self.assertEqual(HealthState.HEALTHY.value, res["state"])
        self.assertTrue(res["source_idle"])
        self.assertIn("legitimately idle", res["reason"].lower())

    def test_increasing_backlog_stalled_processing_is_degraded(self):
        """Increasing backlog while micro-batch processing is stalled must be DEGRADED."""
        from observability.health_state import _backlog_history, _batch_id_history
        _backlog_history.clear()
        _batch_id_history.clear()

        now = datetime(2025, 1, 6, 13, 0, 0, tzinfo=timezone.utc)
        # Establish increasing backlog history with advancing batches: 10 -> 25 -> 40
        for bid, bl in enumerate([10, 25, 40], start=1):
            classify_health({
                "timestamp": "2025-01-06T12:59:58+00:00",
                "batch_id": bid,
                "input_rows_per_second": 10.0,
                "processed_rows_per_second": 10.0,
                "kafka_total_backlog_rows": bl,
            }, live_gold_age_seconds=10, now_utc=now)

        tel_stalled = {
            "timestamp": "2025-01-06T12:59:59+00:00",
            "batch_id": 4,
            "input_rows_per_second": 20.0,
            "processed_rows_per_second": 0.0,  # Stalled!
            "kafka_total_backlog_rows": 60,
        }
        res = classify_health(tel_stalled, live_gold_age_seconds=10, now_utc=now)
        self.assertEqual(HealthState.DEGRADED.value, res["state"])
        self.assertIn("increasing", res["reason"].lower())

    def test_high_trigger_load_is_degraded(self):
        """Trigger load > 0.80 (execution occupies >80% of trigger interval) must be DEGRADED."""
        now = datetime(2025, 1, 6, 13, 0, 0, tzinfo=timezone.utc)
        high_load_tel = {
            "timestamp": "2025-01-06T12:59:58+00:00",
            "batch_id": 11,
            "batch_duration_ms": 4500,
            "trigger_interval_ms": 5000,
            # trigger_load = 4500 / 5000 = 0.90 > 0.80
            "input_rows_per_second": 100.0,
            "processed_rows_per_second": 200.0,
            "watermark": "2025-01-06T12:50:00Z",
        }
        res = classify_health(high_load_tel, live_gold_age_seconds=10, now_utc=now)
        self.assertEqual(HealthState.DEGRADED.value, res["state"])
        self.assertAlmostEqual(0.90, res["trigger_load"])
        self.assertIn("high trigger load", res["reason"].lower())

    def test_watermark_advancement(self):
        """Watermark status must correctly flag advancing state and preserve timestamps."""
        wm_info = build_watermark_status({
            "watermark": "2023-02-01T08:05:00-05:00",
            "max_event_time": "2023-02-01T08:15:00-05:00",
        }, configured_delay="10 minutes")

        self.assertTrue(wm_info["advancing"])
        self.assertEqual("2023-02-01T08:05:00-05:00", wm_info["current_watermark"])
        self.assertEqual("2023-02-01T08:15:00-05:00", wm_info["max_event_time"])
        self.assertEqual("10 minutes", wm_info["configured_delay"])

        no_wm = build_watermark_status(None)
        self.assertIsNone(no_wm["advancing"])
        self.assertIsNone(no_wm["current_watermark"])

    def test_data_governance_separates_batch_and_streaming(self):
        """Batch quarantine and streaming invalid events must never be confused."""
        gov = build_data_governance_report({
            "invalid_events_total": 4,
            "streaming_validation_errors": {"invalid_event_time": 4},
        })
        self.assertEqual(4, gov["streaming_invalid_events_total"])
        self.assertEqual({"invalid_event_time": 4}, gov["streaming_validation_errors"])
        self.assertTrue(gov["batch_quarantine_available"])
        self.assertGreater(gov["batch_quarantine_total_rejected"], 0)
        reasons = gov["batch_rejection_reasons"]
        self.assertIn("fare_amount_invalid", reasons)
        self.assertIn("total_amount_invalid", reasons)
        self.assertIn("dropoff_before_pickup", reasons)
        # Note clarifies distinct sources and non-mutually-exclusive counting
        self.assertIn("Bronze->Silver", gov["note"])
        self.assertIn("Kafka ingest", gov["note"])
        expected_semantics = (
            "Rejection reason counts are non-mutually-exclusive; "
            "a single rejected record may contribute to multiple validation reason counters."
        )
        self.assertEqual(expected_semantics, gov["rejection_counting_semantics"])

    def test_fastapi_full_health_contract(self):
        """FastAPI health response must provide all contract sections and accurate topology."""
        reader = GoldReader(
            str(FOUNDATION / "gold" / "historical_zone_baseline.jsonl"),
            str(FOUNDATION / "gold" / "expected_live_zone_metrics.jsonl"),
        )
        telemetry = {
            "timestamp": "2025-01-06T13:09:55+00:00",
            "query_active": True,
            "batch_id": 42,
            "input_rows_per_second": 124.5,
            "processed_rows_per_second": 312.0,
            "num_input_rows": 622,
            "batch_duration_ms": 1980,
            "trigger_interval_ms": 5000,
            "watermark": "2023-02-01T08:05:00-05:00",
            "max_event_time": "2023-02-01T08:15:00-05:00",
            "kafka_partition_end_offsets": {"0": 1000, "1": 1010, "2": 1000, "3": 1010},
            "kafka_partition_latest_offsets": {"0": 1050, "1": 1040, "2": 1035, "3": 1045},
            "invalid_events_total": 0,
            "output_rows": 850,
        }

        health = build_streaming_health(reader, telemetry=telemetry, now=NOW)

        # 1. Top-level status & reason
        self.assertEqual("HEALTHY", health["status"])

        # 2. Pipeline metadata and actual topology
        self.assertEqual("Apache Spark Structured Streaming", health["pipeline"]["engine"])
        self.assertEqual("4.2.0", health["pipeline"]["version"])
        self.assertEqual("local[2]", health["pipeline"]["master"])
        self.assertIn("local[2]", health["pipeline"]["execution_mode"])
        self.assertIn("4040", health["pipeline"]["cluster_topology"])
        self.assertEqual("America/New_York", health["pipeline"]["session_timezone"])

        # 3. Spark telemetry: real trigger_load and throughput_ratio
        self.assertTrue(health["spark_telemetry"]["query_active"])
        self.assertEqual(42, health["spark_telemetry"]["batch_id"])
        self.assertEqual(124.5, health["spark_telemetry"]["input_rows_per_second"])
        self.assertEqual(312.0, health["spark_telemetry"]["processed_rows_per_second"])
        self.assertEqual(622, health["spark_telemetry"]["num_input_rows"])
        self.assertEqual(1980, health["spark_telemetry"]["batch_duration_ms"])
        self.assertEqual(5000, health["spark_telemetry"]["trigger_interval_ms"])
        # trigger_load = 1980 / 5000 = 0.396
        self.assertEqual(0.396, health["spark_telemetry"]["trigger_load"])
        # throughput_ratio = 124.5 / 312.0 = 0.399
        self.assertEqual(0.399, health["spark_telemetry"]["throughput_ratio"])

        # 4. Watermark status
        self.assertTrue(health["watermark_status"]["advancing"])
        self.assertEqual("2023-02-01T08:05:00-05:00", health["watermark_status"]["current_watermark"])

        # 5. Kafka backlog
        self.assertEqual("Kafka Source Backlog", health["kafka_backlog"]["metric_name"])
        self.assertEqual(150, health["kafka_backlog"]["total_backlog_rows"])

        # 6. Data governance
        self.assertEqual(0, health["data_governance"]["streaming_invalid_events_total"])
        self.assertTrue(health["data_governance"]["batch_quarantine_available"])
        self.assertIn("non-mutually-exclusive", health["data_governance"]["rejection_counting_semantics"])

        # 7. Live gold freshness
        self.assertEqual(300, health["live_gold_freshness"]["age_seconds"])

        # 8. Backwards-compatible legacy fields
        self.assertEqual("available", health["telemetry_status"])
        self.assertEqual(0, health["invalid_record_count"])
        self.assertEqual(300, health["live_gold_age_seconds"])
