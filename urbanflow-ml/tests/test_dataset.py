from __future__ import annotations

import unittest
import sys
from pathlib import Path

import numpy as np
import torch

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from forecasting.dataset import RankPartitionSampler, ZoneDemandDataset


def fixture_arrays():
    counts = np.array(
        [
            [0.0, 10.0],
            [1.0, 11.0],
            [2.0, 12.0],
            [3.0, 13.0],
            [4.0, 14.0],
            [5.0, 15.0],
            [6.0, 16.0],
            [7.0, 17.0],
        ],
        dtype=np.float32,
    )
    return {
        "counts": counts,
        "observed": np.ones_like(counts, dtype=np.bool_),
        "calendar": np.arange(8 * 2, dtype=np.float32).reshape(8, 2),
        "scaler_center": np.zeros(2, dtype=np.float32),
        "scaler_scale": np.ones(2, dtype=np.float32),
    }


METADATA = {
    "split_indices": {
        "train": [2, 5],
        "validation": [5, 7],
        "test": [7, 8],
    }
}


class ZoneDemandDatasetTest(unittest.TestCase):
    def test_history_ends_before_the_exact_target(self):
        dataset = ZoneDemandDataset(fixture_arrays(), METADATA, "train", lookback=2)

        history, calendar, target, target_index = dataset[0]

        self.assertEqual(tuple(history.shape), (2, 2, 2))
        torch.testing.assert_close(history[:, :, 0], torch.log1p(torch.tensor([[0.0, 1.0], [10.0, 11.0]])))
        torch.testing.assert_close(calendar, torch.tensor([[0.0, 1.0], [2.0, 3.0]]))
        torch.testing.assert_close(target, torch.log1p(torch.tensor([2.0, 12.0])))
        self.assertEqual(int(target_index), 2)

    def test_targets_stay_inside_each_chronological_split(self):
        arrays = fixture_arrays()

        train = ZoneDemandDataset(arrays, METADATA, "train", lookback=2)
        validation = ZoneDemandDataset(arrays, METADATA, "validation", lookback=2)
        test = ZoneDemandDataset(arrays, METADATA, "test", lookback=2)

        self.assertEqual(train.target_indices, [2, 3, 4])
        self.assertEqual(validation.target_indices, [5, 6])
        self.assertEqual(test.target_indices, [7])

    def test_target_stride_subsamples_only_within_the_same_split(self):
        metadata = {"split_indices": {"train": [2, 8], "validation": [5, 7], "test": [7, 8]}}
        dataset = ZoneDemandDataset(fixture_arrays(), metadata, "train", lookback=2, target_stride=2)

        self.assertEqual(dataset.target_indices, [2, 4, 6])

    def test_two_rank_sampler_is_disjoint_deterministic_and_balanced(self):
        dataset = ZoneDemandDataset(fixture_arrays(), METADATA, "train", lookback=2)
        rank_zero = RankPartitionSampler(dataset, rank=0, world_size=2, seed=17, shuffle=True)
        rank_one = RankPartitionSampler(dataset, rank=1, world_size=2, seed=17, shuffle=True)
        rank_zero.set_epoch(3)
        rank_one.set_epoch(3)

        zero_indices = list(rank_zero)
        one_indices = list(rank_one)

        self.assertEqual(len(zero_indices), len(one_indices))
        self.assertTrue(set(zero_indices).isdisjoint(one_indices))
        self.assertEqual(zero_indices, list(rank_zero))
        self.assertEqual(len(set(zero_indices + one_indices)), 2)
        self.assertTrue(set(zero_indices + one_indices).issubset({0, 1, 2}))


if __name__ == "__main__":
    unittest.main()
