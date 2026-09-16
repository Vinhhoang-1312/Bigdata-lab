"""Lazy chronological windows and balanced rank partitioning."""

from __future__ import annotations

from collections.abc import Iterator, Mapping

import numpy as np
import torch
from torch.utils.data import Dataset, Sampler


class ZoneDemandDataset(Dataset):
    def __init__(
        self,
        arrays: Mapping[str, np.ndarray],
        metadata: Mapping[str, object],
        split: str,
        *,
        lookback: int = 336,
    ) -> None:
        if lookback <= 0:
            raise ValueError("lookback must be positive")
        split_indices = metadata["split_indices"]
        if split not in split_indices:
            raise ValueError(f"unknown split: {split}")
        left, right = (int(value) for value in split_indices[split])
        left = max(left, lookback)
        if left >= right:
            raise ValueError(f"split {split} has no targets after lookback={lookback}")

        self.counts = np.asarray(arrays["counts"], dtype=np.float32)
        self.observed = np.asarray(arrays["observed"], dtype=np.bool_)
        self.calendar = np.asarray(arrays["calendar"], dtype=np.float32)
        self.center = np.asarray(arrays["scaler_center"], dtype=np.float32)
        self.scale = np.asarray(arrays["scaler_scale"], dtype=np.float32)
        self.lookback = lookback
        self.target_indices = list(range(left, right))

        if self.counts.shape != self.observed.shape:
            raise ValueError("counts and observed must have the same shape")
        if self.calendar.shape[0] != self.counts.shape[0]:
            raise ValueError("calendar and counts must have the same time dimension")
        if self.center.shape != (self.counts.shape[1],) or self.scale.shape != self.center.shape:
            raise ValueError("scaler arrays must have one value per zone")

    def __len__(self) -> int:
        return len(self.target_indices)

    def _scale(self, values: np.ndarray) -> np.ndarray:
        return ((np.log1p(values) - self.center) / self.scale).astype(np.float32)

    def __getitem__(self, index: int):
        target_index = self.target_indices[index]
        history_slice = slice(target_index - self.lookback, target_index)
        scaled_history = self._scale(self.counts[history_slice]).T
        observed_history = self.observed[history_slice].T.astype(np.float32)
        history = np.stack([scaled_history, observed_history], axis=-1)
        calendar = self.calendar[history_slice]
        target = self._scale(self.counts[target_index : target_index + 1])[0]
        return (
            torch.from_numpy(history),
            torch.from_numpy(calendar.copy()),
            torch.from_numpy(target.copy()),
            torch.tensor(target_index, dtype=torch.int64),
        )


class RankPartitionSampler(Sampler[int]):
    """Give every DDP rank disjoint, equal-length indices without padding."""

    def __init__(
        self,
        dataset: Dataset,
        *,
        rank: int,
        world_size: int,
        seed: int,
        shuffle: bool,
    ) -> None:
        if world_size <= 0 or not 0 <= rank < world_size:
            raise ValueError("rank must be in [0, world_size)")
        self.dataset = dataset
        self.rank = rank
        self.world_size = world_size
        self.seed = seed
        self.shuffle = shuffle
        self.epoch = 0
        self.usable_size = len(dataset) - (len(dataset) % world_size)

    def set_epoch(self, epoch: int) -> None:
        self.epoch = int(epoch)

    def __iter__(self) -> Iterator[int]:
        if self.shuffle:
            generator = torch.Generator()
            generator.manual_seed(self.seed + self.epoch)
            indices = torch.randperm(len(self.dataset), generator=generator).tolist()
        else:
            indices = list(range(len(self.dataset)))
        return iter(indices[self.rank : self.usable_size : self.world_size])

    def __len__(self) -> int:
        return self.usable_size // self.world_size
