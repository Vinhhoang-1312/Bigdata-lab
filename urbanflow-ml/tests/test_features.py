from __future__ import annotations

import math
import sys
import unittest
from pathlib import Path

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

try:
    from pyspark.sql import SparkSession, functions as F
except ModuleNotFoundError as exc:  # pragma: no cover - exercised in non-Spark local envs.
    raise unittest.SkipTest("PySpark is required for feature tests") from exc

from forecasting.features import build_features


class FeatureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("urbanflow-ml-feature-tests")
            .config("spark.sql.session.timeZone", "America/New_York")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def _demand(self, rows):
        return self.spark.createDataFrame(
            rows,
            """
            service_date_text string,
            bucket_start_text string,
            pickup_zone_id int,
            half_hour_slot int,
            day_of_week int,
            month int,
            is_weekend boolean,
            trip_count long
            """,
        ).select(
            F.to_date("service_date_text").alias("service_date"),
            F.to_timestamp("bucket_start_text").alias("bucket_start"),
            (F.to_timestamp("bucket_start_text") + F.expr("INTERVAL 30 MINUTES")).alias("bucket_end"),
            "pickup_zone_id",
            "half_hour_slot",
            "day_of_week",
            "month",
            "is_weekend",
            "trip_count",
        )

    def _feature_row(self, demand, service_date, zone, slot):
        return (
            build_features(demand)
            .where(
                (F.col("service_date") == F.to_date(F.lit(service_date)))
                & (F.col("pickup_zone_id") == zone)
                & (F.col("half_hour_slot") == slot)
            )
            .first()
        )

    def test_recent_features_exclude_target_label(self):
        base_rows = [
            ("2023-01-02", "2023-01-02 00:00:00", 1, 0, 1, 1, False, 1),
            ("2023-01-02", "2023-01-02 00:30:00", 1, 1, 1, 1, False, 2),
            ("2023-01-02", "2023-01-02 01:00:00", 1, 2, 1, 1, False, 3),
            ("2023-01-02", "2023-01-02 01:30:00", 1, 3, 1, 1, False, 4),
            ("2023-01-02", "2023-01-02 02:00:00", 1, 4, 1, 1, False, 50),
        ]
        changed_rows = [*base_rows[:-1], ("2023-01-02", "2023-01-02 02:00:00", 1, 4, 1, 1, False, 999)]

        original = self._feature_row(self._demand(base_rows), "2023-01-02", 1, 4)
        changed = self._feature_row(self._demand(changed_rows), "2023-01-02", 1, 4)

        self.assertEqual(original.lag_1_count, 4.0)
        self.assertEqual(original.lag_2_count, 3.0)
        self.assertEqual(original.trailing_2h_mean, 2.5)
        self.assertEqual(changed.lag_1_count, original.lag_1_count)
        self.assertEqual(changed.lag_2_count, original.lag_2_count)
        self.assertEqual(changed.trailing_2h_mean, original.trailing_2h_mean)
        self.assertEqual(original.label, 50.0)
        self.assertEqual(changed.label, 999.0)

    def test_daily_lag_uses_only_previous_service_date(self):
        rows = [
            ("2023-01-01", "2023-01-01 01:00:00", 1, 2, 7, 1, True, 10),
            ("2023-01-02", "2023-01-02 01:00:00", 1, 2, 1, 1, False, 50),
        ]

        row = self._feature_row(self._demand(rows), "2023-01-02", 1, 2)

        self.assertEqual(row.same_slot_prev_day, 10.0)
        self.assertEqual(row.prev_day_missing, 0)
        self.assertEqual(row.prev_week_missing, 1)
        self.assertEqual(row.same_slot_prev_week, 0.0)

    def test_zone_windows_do_not_mix_rows(self):
        rows = [
            ("2023-01-02", "2023-01-02 00:00:00", 1, 0, 1, 1, False, 1),
            ("2023-01-02", "2023-01-02 00:00:00", 2, 0, 1, 1, False, 100),
            ("2023-01-02", "2023-01-02 00:30:00", 1, 1, 1, 1, False, 2),
            ("2023-01-02", "2023-01-02 00:30:00", 2, 1, 1, 1, False, 200),
        ]

        zone_one = self._feature_row(self._demand(rows), "2023-01-02", 1, 1)
        zone_two = self._feature_row(self._demand(rows), "2023-01-02", 2, 1)

        self.assertEqual(zone_one.lag_1_count, 1.0)
        self.assertEqual(zone_two.lag_1_count, 100.0)

    def test_spring_forward_gap_uses_previous_valid_window(self):
        rows = [
            ("2023-03-12", "2023-03-12 01:30:00", 1, 3, 7, 3, True, 7),
            ("2023-03-12", "2023-03-12 03:00:00", 1, 6, 7, 3, True, 11),
        ]

        row = self._feature_row(self._demand(rows), "2023-03-12", 1, 6)

        self.assertEqual(row.lag_1_count, 7.0)
        self.assertEqual(row.lag_2_count, 0.0)

    def test_cyclical_features_are_deterministic(self):
        row = self._feature_row(
            self._demand([("2023-01-02", "2023-01-02 06:00:00", 1, 12, 1, 1, False, 1)]),
            "2023-01-02",
            1,
            12,
        )

        self.assertAlmostEqual(row.time_of_day_sin, math.sin(2 * math.pi * 12 / 48))
        self.assertAlmostEqual(row.time_of_day_cos, math.cos(2 * math.pi * 12 / 48))


if __name__ == "__main__":
    unittest.main()
