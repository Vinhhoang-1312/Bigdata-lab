from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from forecasting.probabilistic_evaluation import anomaly_candidates, probabilistic_metrics, regression_metrics


class ProbabilisticEvaluationTest(unittest.TestCase):
    def test_regression_metrics_use_median_quantile_and_report_wape(self):
        actual = np.array([10.0, 0.0, 5.0], dtype=np.float32)
        predictions = np.array(
            [[8.0, 8.0, 9.0], [-1.0, -1.0, 0.0], [9.0, 9.0, 9.0]],
            dtype=np.float32,
        )

        metrics = regression_metrics(actual, predictions)

        self.assertAlmostEqual(metrics["mae"], (2.0 + 1.0 + 4.0) / 3.0)
        self.assertAlmostEqual(metrics["rmse"], ((4.0 + 1.0 + 16.0) / 3.0) ** 0.5)
        self.assertAlmostEqual(metrics["wape"], 7.0 / 15.0)
        self.assertAlmostEqual(metrics["signed_bias"], (-2.0 - 1.0 + 4.0) / 15.0)
        self.assertAlmostEqual(metrics["clamp_rate"], 1.0 / 3.0)

    def test_regression_metrics_make_zero_demand_wape_unavailable(self):
        metrics = regression_metrics(
            np.array([0.0, 0.0], dtype=np.float32),
            np.zeros((2, 3), dtype=np.float32),
        )

        self.assertIsNone(metrics["wape"])
        self.assertIsNone(metrics["signed_bias"])

    def test_metrics_match_hand_calculated_coverage_and_width(self):
        actual = np.array([5.0, 10.0], dtype=np.float32)
        predictions = np.array([[4.0, 5.0, 7.0], [8.0, 9.0, 11.0]], dtype=np.float32)

        metrics = probabilistic_metrics(actual, predictions)

        self.assertEqual(metrics["coverage_10_90"], 1.0)
        self.assertEqual(metrics["mean_interval_width"], 3.0)
        self.assertAlmostEqual(metrics["pinball_p50"], 0.25)

    def test_anomaly_candidate_reports_direction_and_normalized_severity(self):
        actual = np.array([8.0, 3.0, 5.0], dtype=np.float32)
        predictions = np.array(
            [
                [4.0, 5.0, 7.0],
                [4.0, 5.0, 7.0],
                [4.0, 5.0, 7.0],
            ],
            dtype=np.float32,
        )

        candidates = anomaly_candidates(actual, predictions)

        self.assertEqual(len(candidates), 2)
        self.assertEqual(candidates[0]["direction"], "high")
        self.assertAlmostEqual(candidates[0]["severity"], 1.0 / 3.0)
        self.assertEqual(candidates[1]["direction"], "low")
        self.assertAlmostEqual(candidates[1]["severity"], 1.0 / 3.0)


if __name__ == "__main__":
    unittest.main()
