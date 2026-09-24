"""Read-model tests for the Streaming Reliability viewpoint."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
FOUNDATION = ROOT / "urbanflow-foundation" / "fixtures"
NOW = datetime(2025, 1, 6, 13, 10, tzinfo=timezone.utc)


class StreamingServiceTest(unittest.TestCase):
    def test_gold_read_failure_is_exposed_as_service_unavailable(self):
        """Storage outages must not leak as unhandled HTTP 500 responses."""
        from fastapi import HTTPException
        from serving.app import read_gold_or_503

        def unavailable() -> dict:
            raise PermissionError("Forbidden")

        with self.assertRaises(HTTPException) as caught:
            read_gold_or_503(unavailable)

        self.assertEqual(503, caught.exception.status_code)
        self.assertEqual("Gold data is temporarily unavailable", caught.exception.detail)

    def test_missing_telemetry_is_unavailable_not_zero(self):
        """Removing telemetry must not make the Reliability view look healthy."""
        from analytics_app.data_access import GoldReader
        from serving.streaming_service import build_streaming_health

        reader = GoldReader(
            str(FOUNDATION / "gold" / "historical_zone_baseline.jsonl"),
            str(FOUNDATION / "gold" / "expected_live_zone_metrics.jsonl"),
        )

        health = build_streaming_health(reader, telemetry=None, now=NOW)

        self.assertEqual("unavailable", health["telemetry_status"])
        self.assertEqual("unavailable", health["spark_stream_status"])
        self.assertEqual(str(FOUNDATION / "gold" / "expected_live_zone_metrics.jsonl"), health["gold_source_uri"])
        self.assertIsNone(health["invalid_record_count"])
        self.assertEqual(300, health["live_gold_age_seconds"])

    def test_spark_master_apps_identify_only_the_streaming_job(self):
        from serving.spark_status import classify_stream_job

        self.assertEqual("inactive", classify_stream_job({"activeapps": []}))
        self.assertEqual("inactive", classify_stream_job(
            {"activeapps": [{"name": "UrbanFlow ML training"}]}))
        self.assertEqual("active", classify_stream_job(
            {"activeapps": [{"name": "UrbanFlowLiveMetrics"}]}))
        self.assertEqual("unavailable", classify_stream_job({"workers": []}))

    def test_health_includes_confirmed_stream_job_status(self):
        from analytics_app.data_access import GoldReader
        from serving.streaming_service import build_streaming_health

        reader = GoldReader(
            str(FOUNDATION / "gold" / "historical_zone_baseline.jsonl"),
            str(FOUNDATION / "gold" / "expected_live_zone_metrics.jsonl"),
        )
        health = build_streaming_health(reader, telemetry=None, now=NOW, spark_stream_status="inactive")
        self.assertEqual("inactive", health["spark_stream_status"])

    def test_health_route_returns_a_typed_status_document(self):
        """Removing the route would stop the Reliability UI from loading health."""
        from serving.app import create_app

        app = create_app()

        self.assertEqual("UrbanFlow Streaming API", app.title)

    def test_overview_identifies_replay_and_serializes_missing_baseline(self):
        from analytics_app.data_access import GoldReader
        from analytics_app.zones import load_zone_lookup
        from serving.streaming_service import build_streaming_overview

        reader = GoldReader(
            str(FOUNDATION / "gold" / "historical_zone_baseline.jsonl"),
            str(FOUNDATION / "gold" / "expected_live_zone_metrics.jsonl"),
        )
        lookup = load_zone_lookup(str(FOUNDATION / "bronze" / "zone_lookup.csv"))
        overview = build_streaming_overview(reader, lookup)

        self.assertEqual("historical_replay", overview["source_mode"])
        self.assertEqual(5, overview["window_minutes"])
        self.assertEqual("America/New_York", overview["timezone"])
        self.assertTrue(overview["latest_window"].endswith("+00:00"))
        import json
        json.dumps(overview, allow_nan=False)

    def test_zone_geometry_route_is_exposed(self):
        from serving.app import create_app

        routes = {route.path for route in create_app().routes}
        self.assertIn("/v1/streaming/zones", routes)

    def test_live_gold_reads_are_coalesced_until_refresh_or_expiry(self):
        from serving.app import read_live_snapshot

        class FakeReader:
            live_path = "fixture://serving-cache-test"
            calls = 0

            def live(self):
                self.calls += 1
                return {"version": self.calls}

        reader = FakeReader()
        self.assertEqual(1, read_live_snapshot(reader, at=100)["version"])
        self.assertEqual(1, read_live_snapshot(reader, at=101)["version"])
        self.assertEqual(2, read_live_snapshot(reader, refresh=True, at=102)["version"])
        self.assertEqual(3, read_live_snapshot(reader, at=163)["version"])
