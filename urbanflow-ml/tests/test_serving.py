"""Forecast serving must use a compatible checkpoint and past-only features."""

import unittest

import numpy as np
import torch


class ForecastServingTest(unittest.TestCase):
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

    def test_best_step_must_match_restored_model_weights(self):
        from ml_serving.model_store import verified_best_step

        state = {"weight": torch.tensor([1.0])}
        payload = {"epoch": 10, "model": state,
                   "stopper": {"best_step": 2, "best_state": {"weight": torch.tensor([1.0])}}}
        self.assertEqual(2, verified_best_step(payload))
        payload["model"] = {"weight": torch.tensor([2.0])}
        with self.assertRaisesRegex(ValueError, "best weights"):
            verified_best_step(payload)
