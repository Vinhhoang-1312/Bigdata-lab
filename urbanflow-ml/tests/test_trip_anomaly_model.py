"""The anomaly score is a distance from a learned trip pattern."""

import unittest


class TripAnomalyModelTest(unittest.TestCase):
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
