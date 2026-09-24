from __future__ import annotations

import math
import sys
import tempfile
import unittest
from pathlib import Path

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

try:
    from pyspark.ml import PipelineModel
    from pyspark.sql import SparkSession
except ModuleNotFoundError as exc:  # pragma: no cover - exercised in non-Spark local envs.
    raise unittest.SkipTest("PySpark is required for model tests") from exc

from forecasting.model import GBTParams, apply_prediction_postprocessing, build_pipeline, select_params


class ModelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("urbanflow-ml-model-tests")
            .config("spark.sql.session.timeZone", "America/New_York")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def _training_frame(self):
        rows = []
        for zone_id, base in [(1, 2.0), (2, 12.0)]:
            for slot in range(16):
                rows.append(
                    (
                        zone_id,
                        2,
                        slot % 48,
                        1,
                        0,
                        float(math.sin(slot / 48.0)),
                        float(math.cos(slot / 48.0)),
                        base + slot,
                        base + slot - 1,
                        base + slot - 0.5,
                        base + slot - 2,
                        base + slot - 3,
                        0,
                        0,
                        base + slot + 1,
                        f"2023-01-01T{slot // 2:02d}:{(slot % 2) * 30:02d}:00",
                    )
                )
        return self.spark.createDataFrame(
            rows,
            """
            pickup_zone_id int,
            day_of_week int,
            half_hour_slot int,
            month int,
            is_weekend int,
            time_of_day_sin double,
            time_of_day_cos double,
            lag_1_count double,
            lag_2_count double,
            trailing_2h_mean double,
            same_slot_prev_day double,
            same_slot_prev_week double,
            prev_day_missing int,
            prev_week_missing int,
            label double,
            forecast_window_start string
            """,
        )

    def test_pipeline_fits_predicts_saves_loads_and_keeps_keys(self):
        frame = self._training_frame()
        params = GBTParams(max_depth=2, max_iter=2, step_size=0.1, subsampling_rate=1.0)
        pipeline = build_pipeline(seed=20260913, params=params)

        model = pipeline.fit(frame)
        _ = apply_prediction_postprocessing(model.transform(frame))
        with tempfile.TemporaryDirectory() as tmp:
            model.write().overwrite().save(tmp)
            loaded = PipelineModel.load(tmp)
            loaded_predictions = apply_prediction_postprocessing(loaded.transform(frame))

        row = loaded_predictions.select(
            "pickup_zone_id",
            "forecast_window_start",
            "raw_prediction",
            "predicted_trip_count",
        ).first()

        self.assertIn("pickup_zone_id", loaded_predictions.columns)
        self.assertIn("forecast_window_start", loaded_predictions.columns)
        self.assertIsInstance(row.raw_prediction, float)
        self.assertTrue(math.isfinite(row.raw_prediction))
        self.assertGreaterEqual(row.predicted_trip_count, 0.0)

    def test_postprocessing_clamps_raw_prediction_at_zero(self):
        frame = self.spark.createDataFrame(
            [(-3.5,), (2.0,)],
            "raw_prediction double",
        )

        values = [row.predicted_trip_count for row in apply_prediction_postprocessing(frame).orderBy("raw_prediction").collect()]

        self.assertEqual(values, [0.0, 2.0])

    def test_select_params_uses_validation_mae_and_deterministic_tie_breakers(self):
        frame = self._training_frame()
        candidates = [
            GBTParams(max_depth=3, max_iter=2, step_size=0.1, subsampling_rate=1.0),
            GBTParams(max_depth=2, max_iter=2, step_size=0.1, subsampling_rate=1.0),
        ]

        result = select_params(frame, frame, candidates, seed=20260913)

        self.assertIn(result.selected_params, candidates)
        self.assertEqual(len(result.candidate_results), 2)
        self.assertIsNotNone(result.validation_mae)


if __name__ == "__main__":
    unittest.main()
