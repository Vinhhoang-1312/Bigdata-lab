"""Anomaly serving never turns a missing or incomplete run into a result."""

import io
import json
import unittest
from unittest.mock import patch


def inference_artifact(*, threshold=1.0):
    return {
        "status": "PASS",
        "run_id": "trip-anomaly-test",
        "method": "spark_mllib_kmeans",
        "threshold": threshold,
        "threshold_percentile": 99.5,
        "screened_sample_count": 200,
        "flagged_sample_count": 1,
        "features": ["trip_distance", "trip_duration_minutes", "fare_amount", "average_speed_mph"],
        "scaler_mean": [0.0, 0.0, 0.0, 0.0],
        "scaler_std": [1.0, 1.0, 1.0, 1.0],
        "cluster_centers": [[0.0, 0.0, 0.0, 0.0], [2.0, 3.0, 2.0, 2.0]],
        "trips": [],
    }


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

    def test_accepts_complete_inference_artifact(self):
        from ml_serving.trip_anomalies import validate_anomaly_inference_artifact

        result = validate_anomaly_inference_artifact(inference_artifact())

        self.assertEqual("trip-anomaly-test", result["run_id"])

    def test_rejects_incomplete_inference_metadata_before_scoring(self):
        from ml_serving.trip_anomalies import validate_anomaly_inference_artifact

        for key in ("features", "scaler_mean", "scaler_std", "cluster_centers", "threshold"):
            payload = inference_artifact()
            payload.pop(key)
            with self.subTest(key=key):
                with self.assertRaises(ValueError):
                    validate_anomaly_inference_artifact(payload)

    def test_score_trip_returns_cluster_reason_and_threshold_decision(self):
        from ml_serving.trip_anomalies import score_trip

        result = score_trip(
            inference_artifact(),
            distance_miles=6.0,
            duration_minutes=30.0,
            fare_usd=12.0,
        )

        self.assertEqual(1, result["nearest_cluster"])
        self.assertLess(result["score"], result["threshold"])
        self.assertFalse(result["review_required"])
        self.assertAlmostEqual(12.0, result["derived_speed_mph"])
        self.assertEqual("Fare above learned pattern", result["reason"])
        self.assertEqual("trip-anomaly-test", result["run_id"])
        self.assertEqual("spark_mllib_kmeans", result["method"])
        self.assertEqual("research_only", result["use_status"])

    def test_score_trip_requires_review_at_exact_threshold(self):
        from ml_serving.trip_anomalies import score_trip
        from forecasting.trip_anomaly import raw_trip_features

        values = raw_trip_features(6.0, 30.0, 12.0)
        threshold = sum(value * value for value in values) ** 0.5
        artifact = inference_artifact(threshold=threshold)
        artifact["cluster_centers"] = [[0.0, 0.0, 0.0, 0.0]]

        result = score_trip(artifact, distance_miles=6.0, duration_minutes=30.0, fare_usd=12.0)

        self.assertTrue(result["review_required"])

    def test_fastapi_score_endpoint_returns_valid_result(self):
        from ml_serving.app import TripScoreRequest, create_app

        with patch("ml_serving.app.get_anomaly_snapshot", return_value=inference_artifact()):
            endpoint = next(route.endpoint for route in create_app().routes
                            if route.path == "/v1/anomaly/score")
            response = endpoint(TripScoreRequest(
                pickup_zone_id=43,
                dropoff_zone_id=161,
                trip_distance_miles=6.0,
                trip_duration_minutes=30.0,
                fare_amount_usd=12.0,
            ))

        self.assertIn("score", response)

    def test_fastapi_score_request_rejects_invalid_numeric_input(self):
        from pydantic import ValidationError
        from ml_serving.app import TripScoreRequest

        with self.assertRaises(ValidationError):
            TripScoreRequest(
                pickup_zone_id=43,
                dropoff_zone_id=161,
                trip_distance_miles=0.0,
                trip_duration_minutes=30.0,
                fare_amount_usd=12.0,
            )

    def test_fastapi_score_endpoint_hides_unavailable_artifact_details(self):
        from fastapi import HTTPException
        from ml_serving.app import TripScoreRequest, create_app

        with patch("ml_serving.app.get_anomaly_snapshot", side_effect=RuntimeError("secret S3 details")):
            endpoint = next(route.endpoint for route in create_app().routes
                            if route.path == "/v1/anomaly/score")
            with self.assertRaises(HTTPException) as raised:
                endpoint(TripScoreRequest(
                    pickup_zone_id=43,
                    dropoff_zone_id=161,
                    trip_distance_miles=6.0,
                    trip_duration_minutes=30.0,
                    fare_amount_usd=12.0,
                ))

        self.assertEqual(503, raised.exception.status_code)
        self.assertNotIn("secret S3 details", str(raised.exception.detail))


if __name__ == "__main__":
    unittest.main()
