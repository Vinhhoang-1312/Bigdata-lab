from __future__ import annotations

import math
import sys
import unittest
from pathlib import Path

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

try:
    from pyspark.sql import SparkSession
except ModuleNotFoundError as exc:  # pragma: no cover - exercised in non-Spark local envs.
    raise unittest.SkipTest("PySpark is required for evaluation tests") from exc

from forecasting.baselines import add_seasonal_naive_prediction, fit_historical_mean
from forecasting.evaluation import (
    score_by_borough,
    score_demand_deciles,
    score_peak_windows,
    score_per_zone,
    score_predictions,
    top_k_recall,
)


class EvaluationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("urbanflow-ml-evaluation-tests")
            .config("spark.sql.session.timeZone", "America/New_York")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_scores_regression_metrics_from_hand_calculated_values(self):
        frame = self.spark.createDataFrame(
            [
                ("a", 10.0, 8.0),
                ("b", 0.0, -1.0),
                ("c", 5.0, 9.0),
            ],
            "row_id string, label double, prediction double",
        )

        report = score_predictions(frame, "prediction")

        self.assertEqual(report.row_count, 3)
        self.assertAlmostEqual(report.mae, (2 + 1 + 4) / 3)
        self.assertAlmostEqual(report.rmse, math.sqrt((4 + 1 + 16) / 3))
        self.assertAlmostEqual(report.wape, 7 / 15)
        self.assertAlmostEqual(report.signed_bias, (8 - 10 - 1 - 0 + 9 - 5) / 15)
        self.assertAlmostEqual(report.clamp_rate, 1 / 3)
        self.assertEqual(report.warnings, [])

    def test_wape_is_null_when_total_actual_is_zero(self):
        frame = self.spark.createDataFrame(
            [
                ("a", 0.0, 1.0),
                ("b", 0.0, 2.0),
            ],
            "row_id string, label double, prediction double",
        )

        report = score_predictions(frame, "prediction")

        self.assertIsNone(report.wape)
        self.assertIn("WAPE undefined when total actual demand is zero", report.warnings)

    def test_top_k_recall_uses_overlap_between_actual_and_predicted_peaks(self):
        frame = self.spark.createDataFrame(
            [
                ("a", 100.0, 90.0),
                ("b", 80.0, 10.0),
                ("c", 70.0, 95.0),
                ("d", 1.0, 1.0),
            ],
            "row_id string, label double, prediction double",
        )

        self.assertEqual(top_k_recall(frame, "prediction", k=2), 0.5)

    def test_seasonal_naive_uses_only_available_feature_column(self):
        frame = self.spark.createDataFrame(
            [(1, 4.0, 99.0)],
            "pickup_zone_id int, same_slot_prev_week double, label double",
        )

        row = add_seasonal_naive_prediction(frame).first()

        self.assertEqual(row.seasonal_naive_prediction, 4.0)

    def test_historical_mean_fits_train_only_keys(self):
        train = self.spark.createDataFrame(
            [
                (1, 1, 10, 2.0),
                (1, 1, 10, 4.0),
                (1, 2, 10, 100.0),
            ],
            "pickup_zone_id int, day_of_week int, half_hour_slot int, label double",
        )
        validation_a = self.spark.createDataFrame(
            [(1, 1, 10, 999.0)],
            "pickup_zone_id int, day_of_week int, half_hour_slot int, label double",
        )
        validation_b = self.spark.createDataFrame(
            [(1, 1, 10, 0.0)],
            "pickup_zone_id int, day_of_week int, half_hour_slot int, label double",
        )

        fit_a = fit_historical_mean(train)
        fit_b = fit_historical_mean(train)

        self.assertEqual(validation_a.count(), 1)
        self.assertEqual(validation_b.count(), 1)
        self.assertEqual(fit_a.collect(), fit_b.collect())
        row = fit_a.where("pickup_zone_id = 1 AND day_of_week = 1 AND half_hour_slot = 10").first()
        self.assertEqual(row.historical_mean_prediction, 3.0)

    def test_summary_frames_cover_peak_borough_decile_and_zone_slices(self):
        frame = self.spark.createDataFrame(
            [
                ("2023-02-01T00:00:00", 1, 100.0, 90.0),
                ("2023-02-01T00:00:00", 2, 80.0, 10.0),
                ("2023-02-01T00:00:00", 3, 10.0, 95.0),
                ("2023-02-01T00:30:00", 1, 5.0, 4.0),
                ("2023-02-01T00:30:00", 2, 7.0, 8.0),
                ("2023-02-01T00:30:00", 3, 1.0, 2.0),
            ],
            "forecast_window_start string, pickup_zone_id int, label double, prediction double",
        )
        zone_lookup = self.spark.createDataFrame(
            [(1, "A"), (2, "A"), (3, "B")],
            "pickup_zone_id int, borough string",
        )

        peak_rows = score_peak_windows(frame, "prediction", k=2).orderBy("forecast_window_start").collect()
        borough_rows = {row.borough: row for row in score_by_borough(frame, "prediction", zone_lookup=zone_lookup).collect()}
        decile_count = score_demand_deciles(frame, "prediction").count()
        zone_rows = {row.pickup_zone_id: row for row in score_per_zone(frame, "prediction").collect()}

        self.assertAlmostEqual(peak_rows[0].top_k_recall, 0.5)
        self.assertAlmostEqual(peak_rows[1].top_k_recall, 1.0)
        self.assertEqual(borough_rows["A"].row_count, 4)
        self.assertAlmostEqual(borough_rows["A"].mae, 20.5)
        self.assertEqual(decile_count, 6)
        self.assertEqual(zone_rows[1].row_count, 2)
        self.assertAlmostEqual(zone_rows[1].mae, 5.5)


if __name__ == "__main__":
    unittest.main()
