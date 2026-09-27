"""Forecast serving must use a compatible checkpoint and past-only features."""

import unittest
from unittest.mock import patch

import numpy as np
import torch


class ForecastServingTest(unittest.TestCase):
    def test_information_endpoint_degrades_anomaly_without_hiding_demand(self):
        from ml_serving.app import create_app

        route = next(item.endpoint for item in create_app().routes if item.path == "/v1/ml/information")
        calls = []

        class Store:
            def information(self, anomaly, anomaly_error=None):
                calls.append((anomaly, anomaly_error))
                return {"demand_forecast": {"availability": "available"},
                        "kmeans_anomaly": {"availability": "unavailable"}}

        with patch("ml_serving.app._store", return_value=Store()), \
             patch("ml_serving.app.get_anomaly_snapshot", side_effect=RuntimeError("private storage detail")):
            result = route()

        self.assertEqual({"availability": "available"}, result["demand_forecast"])
        self.assertEqual((None, "artifact_unavailable"), calls[0])

    def test_anomaly_snapshot_cache_reads_once(self):
        from ml_serving.trip_anomalies import get_anomaly_snapshot

        payload = {"status": "PASS", "run_id": "cached"}
        get_anomaly_snapshot.cache_clear()
        with patch("ml_serving.trip_anomalies.load_anomaly_snapshot", return_value=payload) as load:
            self.assertEqual(payload, get_anomaly_snapshot("cached"))
            self.assertEqual(payload, get_anomaly_snapshot("cached"))
        load.assert_called_once_with("cached")
        get_anomaly_snapshot.cache_clear()

    def test_forecast_time_requires_offset_and_half_hour_bucket(self):
        from ml_serving.model_store import resolve_target_index

        timestamps = np.array(["2023-02-19T18:00:00-05:00"])
        with self.assertRaisesRegex(ValueError, "offset"):
            resolve_target_index(timestamps, "2023-02-19T18:00:00")
        with self.assertRaisesRegex(ValueError, "30-minute"):
            resolve_target_index(timestamps, "2023-02-19T18:15:00-05:00")
        self.assertEqual(0, resolve_target_index(timestamps, "2023-02-19T23:00:00+00:00"))


    def test_predict_uses_history_not_target_and_orders_quantiles(self):
        from ml_serving.model_store import predict_at

        class RecordingModel:
            def __call__(self, history, calendar, adjacency):
                assert history.shape == (1, 2, 2, 2)
                assert torch.all(history[0, :, :, 1] == 1)
                return torch.tensor([[[0.0, 0.4, 0.8], [0.1, 0.5, 0.9]]])

        arrays = {
        "counts": np.array([[1, 2], [3, 4], [999, 999]], dtype=np.float32),
        "observed": np.ones((3, 2), dtype=bool),
        "calendar": np.zeros((3, 7), dtype=np.float32),
        "zone_ids": np.array([43, 50]),
        "timestamps": np.array([
            "2023-02-19T17:00:00-05:00", "2023-02-19T17:30:00-05:00",
            "2023-02-19T18:00:00-05:00",
        ]),
        "scaler_center": np.zeros(2, dtype=np.float32),
        "scaler_scale": np.ones(2, dtype=np.float32),
        "fixed_adjacency": np.eye(2, dtype=np.float32),
        }
        result = predict_at(RecordingModel(), arrays, {"split_indices": {"test": [2, 3]}}, 43,
                            "2023-02-19T18:00:00-05:00", lookback=2)
        self.assertLessEqual(result["p10"], result["p50"])
        self.assertLessEqual(result["p50"], result["p90"])
        self.assertEqual(43, result["zone_id"])
        self.assertTrue(result["forecast_at"].startswith("2023-02-19T18:00:00"))


    def test_evidence_marks_raw_test_report_preliminary(self):
        from ml_serving.model_store import evidence_document

        result = evidence_document({"status": "PASS", "wape": 0.691, "mae": 5.6},
                                   {"distribution": {"status": "PASS", "world_size": 2}})
        self.assertEqual("preliminary", result["benchmark_status"])
        self.assertEqual(0.691, result["metrics"]["wape"])
        self.assertEqual(2, result["distributed_training"]["world_size"])
        self.assertTrue(result["missing_evidence"])

    def test_evidence_recommends_against_operational_gtqn_when_weekly_baseline_wins(self):
        from ml_serving.model_store import evidence_document

        metrics = {"status": "PASS", "wape": 0.69, "mae": 5.6,
                   "observed_target_count": 356160, "test_bucket_count": 1344}
        baseline = {"method": "same_zone_same_half_hour_previous_week", "wape": 0.24,
                    "mae": 1.97, "target_count": 356160, "matched_count": 356160,
                    "test_bucket_count": 1344}
        result = evidence_document(metrics, {"distribution": {"status": "PASS"}}, baseline)

        self.assertEqual("research_only", result["use_status"])
        self.assertEqual("weekly_naive", result["preferred_method"])
        self.assertTrue(result["baseline_comparison"]["same_test_targets"])
        self.assertAlmostEqual(0.45, result["baseline_comparison"]["wape_gap"])

    def test_evidence_keeps_model_metrics_when_baseline_targets_do_not_match(self):
        from ml_serving.model_store import evidence_document

        metrics = {"status": "PASS", "wape": 0.69, "observed_target_count": 10,
                   "test_bucket_count": 1}
        baseline = {"wape": 0.24, "target_count": 10, "matched_count": 9,
                    "test_bucket_count": 1}
        result = evidence_document(metrics, {}, baseline)
        self.assertIsNone(result["baseline_comparison"])
        self.assertIn("same observed test targets", " ".join(result["missing_evidence"]))
        self.assertEqual(metrics, result["metrics"])

    def test_legacy_test_report_without_fingerprint_is_visible_but_not_verified(self):
        from ml_serving.model_store import validate_report_provenance

        self.assertFalse(validate_report_provenance({"status": "PASS"}, "current-fingerprint"))
        self.assertTrue(validate_report_provenance(
            {"artifact": {"dataset_fingerprint": "current-fingerprint"}}, "current-fingerprint"))
        with self.assertRaisesRegex(ValueError, "fingerprint mismatch"):
            validate_report_provenance(
                {"artifact": {"dataset_fingerprint": "different"}}, "current-fingerprint")

    def test_retrospective_reference_never_invents_missing_weekly_history(self):
        from ml_serving.model_store import retrospective_reference

        counts = np.zeros((337, 1), dtype=np.float32)
        counts[0, 0] = 8
        counts[336, 0] = 10
        observed = np.ones_like(counts, dtype=bool)
        arrays = {"counts": counts, "observed": observed}
        self.assertEqual({"observed_actual": 10.0, "weekly_naive": 8.0},
                         retrospective_reference(arrays, 336, 0))
        observed[0, 0] = False
        self.assertEqual({"observed_actual": 10.0, "weekly_naive": None},
                         retrospective_reference(arrays, 336, 0))

    def test_best_step_must_match_restored_model_weights(self):
        from ml_serving.model_store import verified_best_step

        state = {"weight": torch.tensor([1.0])}
        payload = {"epoch": 10, "model": state,
                   "stopper": {"best_step": 2, "best_state": {"weight": torch.tensor([1.0])}}}
        self.assertEqual(2, verified_best_step(payload))
        payload["model"] = {"weight": torch.tensor([2.0])}
        with self.assertRaisesRegex(ValueError, "best weights"):
            verified_best_step(payload)
