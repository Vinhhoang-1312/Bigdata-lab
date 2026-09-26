"""Anomaly serving never turns a missing or incomplete run into a result."""

import io
import json
import unittest


class TripAnomalyServingTest(unittest.TestCase):
    def test_reads_completed_snapshot_and_preserves_detection_scope(self):
        from ml_serving.trip_anomalies import read_anomaly_snapshot

        payload = {
            "status": "PASS", "run_id": "trip-anomaly-test",
            "method": "spark_mllib_kmeans", "threshold": 3.5,
            "screened_sample_count": 200, "flagged_sample_count": 1,
            "trips": [{"pickup_zone_id": 43, "dropoff_zone_id": 161, "score": 4.2}],
        }

        class FakeClient:
            def get_object(self, **kwargs):
                self.request = kwargs
                return {"Body": io.BytesIO(json.dumps(payload).encode("utf-8"))}

        client = FakeClient()
        result = read_anomaly_snapshot(client, "urbanflow", "dev/ml/anomaly/test/result.json")
        self.assertEqual(payload, result)
        self.assertEqual("urbanflow", client.request["Bucket"])

    def test_rejects_incomplete_model_output(self):
        from ml_serving.trip_anomalies import validate_anomaly_snapshot

        with self.assertRaisesRegex(ValueError, "complete"):
            validate_anomaly_snapshot({"status": "RUNNING", "trips": []})
        with self.assertRaisesRegex(ValueError, "sample"):
            validate_anomaly_snapshot({"status": "PASS", "trips": []})


if __name__ == "__main__":
    unittest.main()
