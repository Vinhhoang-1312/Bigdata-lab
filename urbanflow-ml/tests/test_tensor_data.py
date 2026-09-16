from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np

from forecasting.tensor_data import (
    build_training_arrays,
    dataset_fingerprint,
    load_tensor_artifact,
    save_tensor_artifact,
)


SPLITS = {
    "train_start": "2023-01-01",
    "train_end": "2023-01-01",
    "validation_start": "2023-01-02",
    "validation_end": "2023-01-02",
    "test_start": "2023-01-03",
    "test_end": "2023-01-03",
}


class TensorDataTest(unittest.TestCase):
    def test_builds_deterministic_zone_order_and_counts(self):
        rows = [
            {"service_date": "2023-01-01", "half_hour_slot": 0, "pickup_zone_id": 9, "trip_count": 2},
            {"service_date": "2023-01-01", "half_hour_slot": 0, "pickup_zone_id": 4, "trip_count": 5},
            {"service_date": "2023-01-01", "half_hour_slot": 1, "pickup_zone_id": 4, "trip_count": 3},
            {"service_date": "2023-01-01", "half_hour_slot": 1, "pickup_zone_id": 9, "trip_count": 7},
            {"service_date": "2023-01-03", "half_hour_slot": 47, "pickup_zone_id": 4, "trip_count": 1},
        ]

        arrays, metadata = build_training_arrays(rows, [9, 4], SPLITS)

        self.assertEqual(arrays["zone_ids"].tolist(), [4, 9])
        self.assertEqual(arrays["counts"].shape, (3 * 48, 2))
        np.testing.assert_array_equal(arrays["counts"][0], np.array([5.0, 2.0], dtype=np.float32))
        np.testing.assert_array_equal(arrays["counts"][1], np.array([3.0, 7.0], dtype=np.float32))
        self.assertEqual(metadata["split_indices"]["train"], [0, 48])
        self.assertEqual(metadata["split_indices"]["validation"], [48, 96])
        self.assertEqual(metadata["split_indices"]["test"], [96, 144])

    def test_missing_bucket_is_zero_and_marked_unobserved(self):
        rows = [
            {"service_date": "2023-01-01", "half_hour_slot": 0, "pickup_zone_id": 4, "trip_count": 2},
            {"service_date": "2023-01-01", "half_hour_slot": 0, "pickup_zone_id": 9, "trip_count": 0},
            {"service_date": "2023-01-03", "half_hour_slot": 47, "pickup_zone_id": 4, "trip_count": 1},
        ]

        arrays, _ = build_training_arrays(rows, [4, 9], SPLITS)

        np.testing.assert_array_equal(arrays["counts"][1], np.array([0.0, 0.0], dtype=np.float32))
        np.testing.assert_array_equal(arrays["observed"][0], np.array([True, True]))
        np.testing.assert_array_equal(arrays["observed"][1], np.array([False, False]))

    def test_fingerprint_changes_with_counts_or_zone_order(self):
        arrays = {
            "counts": np.array([[1.0, 2.0]], dtype=np.float32),
            "zone_ids": np.array([4, 9], dtype=np.int32),
            "timestamps": np.array(["2023-01-01T00:00:00-05:00"]),
        }
        first = dataset_fingerprint(arrays, {"train": [0, 1]})

        changed_counts = {**arrays, "counts": np.array([[1.0, 3.0]], dtype=np.float32)}
        changed_order = {**arrays, "zone_ids": np.array([9, 4], dtype=np.int32)}

        self.assertNotEqual(first, dataset_fingerprint(changed_counts, {"train": [0, 1]}))
        self.assertNotEqual(first, dataset_fingerprint(changed_order, {"train": [0, 1]}))

    def test_compact_bucket_rows_and_artifact_round_trip(self):
        rows = [
            {
                "service_date": "2023-01-01",
                "half_hour_slot": 0,
                "source_observed": True,
                "zone_counts": [
                    {"pickup_zone_id": 4, "trip_count": 5},
                    {"pickup_zone_id": 9, "trip_count": 2},
                ],
            },
            {
                "service_date": "2023-01-03",
                "half_hour_slot": 47,
                "source_observed": False,
                "zone_counts": [
                    {"pickup_zone_id": 4, "trip_count": 0},
                    {"pickup_zone_id": 9, "trip_count": 0},
                ],
            },
        ]
        arrays, metadata = build_training_arrays(rows, [4, 9], SPLITS)

        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "tensor.npz"
            save_tensor_artifact(artifact, arrays, metadata)
            loaded_arrays, loaded_metadata = load_tensor_artifact(artifact)

        np.testing.assert_array_equal(loaded_arrays["counts"], arrays["counts"])
        self.assertEqual(loaded_metadata["dataset_fingerprint"], metadata["dataset_fingerprint"])
        self.assertFalse(loaded_arrays["observed"][-1].any())


if __name__ == "__main__":
    unittest.main()
