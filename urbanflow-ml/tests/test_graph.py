from __future__ import annotations

import unittest

import numpy as np

from forecasting.graph import build_od_adjacency


class ZoneGraphTest(unittest.TestCase):
    def test_builds_directed_row_normalized_training_graph(self):
        trips = [
            {"pickup_datetime": "2023-01-01T08:00:00", "pickup_zone_id": 1, "dropoff_zone_id": 2},
            {"pickup_datetime": "2023-01-01T08:30:00", "pickup_zone_id": 1, "dropoff_zone_id": 2},
            {"pickup_datetime": "2023-01-01T09:00:00", "pickup_zone_id": 1, "dropoff_zone_id": 3},
            {"pickup_datetime": "2023-01-01T09:30:00", "pickup_zone_id": 2, "dropoff_zone_id": 1},
        ]

        adjacency = build_od_adjacency(trips, [1, 2, 3], "2023-01-01")

        np.testing.assert_allclose(
            adjacency,
            np.array(
                [
                    [0.25, 0.50, 0.25],
                    [0.50, 0.50, 0.00],
                    [0.00, 0.00, 1.00],
                ],
                dtype=np.float32,
            ),
        )

    def test_post_train_trip_does_not_change_graph(self):
        train_trip = {"pickup_datetime": "2023-01-01T08:00:00", "pickup_zone_id": 1, "dropoff_zone_id": 2}
        future_trip = {"pickup_datetime": "2023-02-01T08:00:00", "pickup_zone_id": 1, "dropoff_zone_id": 3}

        expected = build_od_adjacency([train_trip], [1, 2, 3], "2023-01-31")
        actual = build_od_adjacency([train_trip, future_trip], [1, 2, 3], "2023-01-31")

        np.testing.assert_array_equal(actual, expected)

    def test_accepts_preaggregated_edge_counts(self):
        edges = [
            {
                "pickup_datetime": "2023-01-01T00:00:00",
                "pickup_zone_id": 1,
                "dropoff_zone_id": 2,
                "edge_count": 3,
            }
        ]

        adjacency = build_od_adjacency(edges, [1, 2], "2023-01-31")

        np.testing.assert_allclose(adjacency[0], np.array([0.25, 0.75], dtype=np.float32))


if __name__ == "__main__":
    unittest.main()
