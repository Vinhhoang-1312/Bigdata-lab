from __future__ import annotations

import sys
import unittest
from pathlib import Path

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

try:
    from pyspark.sql import SparkSession, functions as F
except ModuleNotFoundError as exc:  # pragma: no cover - exercised in non-Spark local envs.
    raise unittest.SkipTest("PySpark is required for demand-frame tests") from exc

from forecasting.demand_frame import build_demand_frame


class DemandFrameTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("urbanflow-ml-demand-frame-tests")
            .config("spark.sql.session.timeZone", "America/New_York")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def _silver(self, rows):
        return self.spark.createDataFrame(
            rows,
            "pickup_datetime_text string, pickup_zone_id int",
        ).select(
            F.to_timestamp("pickup_datetime_text").alias("pickup_datetime"),
            "pickup_zone_id",
        )

    def _zones(self, zone_ids):
        return self.spark.createDataFrame([(zone,) for zone in zone_ids], "LocationID int")

    def test_builds_zero_inclusive_half_hour_frame(self):
        silver = self._silver(
            [
                ("2023-01-02 00:00:00", 1),
                ("2023-01-02 00:29:59", 1),
                ("2023-01-02 00:30:00", 1),
            ]
        )
        frame = build_demand_frame(silver, self._zones([1, 2]), "2023-01-02", "2023-01-02")

        self.assertEqual(frame.count(), 96)
        distinct_keys = frame.select("service_date", "bucket_start", "pickup_zone_id").distinct().count()
        self.assertEqual(distinct_keys, 96)

        counts = {
            (row.pickup_zone_id, row.half_hour_slot): row.trip_count
            for row in frame.where(F.col("half_hour_slot").isin(0, 1)).collect()
        }
        self.assertEqual(counts[(1, 0)], 2)
        self.assertEqual(counts[(1, 1)], 1)
        self.assertEqual(counts[(2, 0)], 0)
        self.assertEqual(counts[(2, 1)], 0)

        first = frame.where((F.col("pickup_zone_id") == 1) & (F.col("half_hour_slot") == 0)).first()
        self.assertEqual(first.day_of_week, 1)
        self.assertEqual(first.month, 1)
        self.assertFalse(first.is_weekend)

    def test_spring_forward_invalid_local_slots_are_excluded(self):
        frame = build_demand_frame(
            self._silver([]),
            self._zones([1]),
            "2023-03-12",
            "2023-03-12",
        )

        slots = [row.half_hour_slot for row in frame.orderBy("half_hour_slot").collect()]
        self.assertEqual(len(slots), 46)
        self.assertNotIn(4, slots)
        self.assertNotIn(5, slots)

    def test_fall_back_repeated_hour_uses_one_wall_clock_bucket(self):
        silver = self._silver(
            [
                ("2022-11-06 01:10:00", 1),
                ("2022-11-06 01:20:00", 1),
            ]
        )
        frame = build_demand_frame(silver, self._zones([1]), "2022-11-06", "2022-11-06")

        self.assertEqual(frame.count(), 48)
        repeated_hour_bucket = frame.where(F.col("half_hour_slot") == 2).first()
        self.assertEqual(repeated_hour_bucket.trip_count, 2)


if __name__ == "__main__":
    unittest.main()
