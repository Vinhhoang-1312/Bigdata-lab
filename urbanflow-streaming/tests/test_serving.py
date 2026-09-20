"""Read-model tests for the Streaming Reliability viewpoint."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
FOUNDATION = ROOT / "urbanflow-foundation" / "fixtures"
NOW = datetime(2025, 1, 6, 13, 10, tzinfo=timezone.utc)


class StreamingServiceTest(unittest.TestCase):
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
        self.assertIsNone(health["invalid_record_count"])
        self.assertEqual(300, health["live_gold_age_seconds"])

    def test_health_route_returns_a_typed_status_document(self):
        """Removing the route would stop the Reliability UI from loading health."""
        from serving.app import create_app

        app = create_app()

        self.assertEqual("UrbanFlow Streaming API", app.title)
