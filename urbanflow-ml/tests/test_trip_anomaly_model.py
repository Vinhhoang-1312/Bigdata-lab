"""The anomaly score is a distance from a learned trip pattern."""

import unittest
import math


class TripAnomalyModelTest(unittest.TestCase):
    def test_raw_trip_features_match_spark_training_formula(self):
        from forecasting.trip_anomaly import raw_trip_features

        result = raw_trip_features(6.0, 30.0, 12.0)

        self.assertEqual(
            [math.log1p(6.0), math.log1p(30.0), math.log1p(12.0), math.log1p(12.0)],
            result,
        )

    def test_raw_trip_features_reject_invalid_measurements(self):
        from forecasting.trip_anomaly import raw_trip_features

        invalid = (
            (0.0, 30.0, 12.0),
            (6.0, 0.0, 12.0),
            (6.0, 30.0, -1.0),
            (math.nan, 30.0, 12.0),
            (6.0, math.inf, 12.0),
            (6.0, 30.0, math.inf),
        )
        for values in invalid:
            with self.subTest(values=values):
                with self.assertRaises(ValueError):
                    raw_trip_features(*values)

    def test_standardize_features_rejects_invalid_artifact_metadata(self):
        from forecasting.trip_anomaly import standardize_features

        invalid = (
            ([1.0], [0.0, 0.0, 0.0, 0.0], [1.0, 1.0, 1.0, 1.0]),
            ([1.0, 1.0, 1.0, 1.0], [0.0], [1.0, 1.0, 1.0, 1.0]),
            ([1.0, 1.0, 1.0, 1.0], [0.0, 0.0, 0.0, 0.0], [1.0, 1.0, 1.0]),
            ([1.0, 1.0, 1.0, 1.0], [0.0, 0.0, 0.0, 0.0], [1.0, 0.0, 1.0, 1.0]),
        )
        for values in invalid:
            with self.subTest(values=values):
                with self.assertRaises(ValueError):
                    standardize_features(*values)

    def test_score_nearest_cluster_returns_deterministic_cluster_and_reason(self):
        from forecasting.trip_anomaly import score_nearest_cluster

        cluster, score, reason = score_nearest_cluster(
            [0.0, 2.0, 0.0, 0.0],
            [[0.0, 0.0, 0.0, 0.0], [4.0, 2.0, 0.0, 0.0]],
        )

        self.assertEqual(0, cluster)
        self.assertAlmostEqual(2.0, score)
        self.assertEqual("Duration above learned pattern", reason)

    def test_score_nearest_cluster_rejects_empty_or_invalid_centers(self):
        from forecasting.trip_anomaly import score_nearest_cluster

        for centers in ([], [[0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0]]):
            with self.subTest(centers=centers):
                with self.assertRaises(ValueError):
                    score_nearest_cluster([0.0, 0.0, 0.0, 0.0], centers)

    def test_score_and_explanation_follow_largest_feature_deviation(self):
        from forecasting.trip_anomaly import explain_deviation, score_distance

        center = [0.0, 0.0, 0.0, 0.0]
        regular = [0.1, 0.1, 0.1, 0.1]
        unusual = [0.1, 3.0, 0.1, 0.1]
        self.assertLess(score_distance(regular, center), score_distance(unusual, center))
        self.assertEqual("Duration above learned pattern", explain_deviation(unusual, center))

    def test_invalid_feature_dimension_is_rejected(self):
        from forecasting.trip_anomaly import score_distance

        with self.assertRaisesRegex(ValueError, "four"):
            score_distance([1.0], [0.0])


if __name__ == "__main__":
    unittest.main()
