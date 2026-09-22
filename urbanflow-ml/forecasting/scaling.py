"""Train-only robust scaling for non-negative zone demand."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class RobustZoneScaler:
    center: np.ndarray
    scale: np.ndarray

    @classmethod
    def fit(cls, train_counts: np.ndarray) -> "RobustZoneScaler":
        values = np.asarray(train_counts, dtype=np.float32)
        if values.ndim != 2 or values.shape[0] == 0:
            raise ValueError("train_counts must have shape [time, zone] and be non-empty")
        if not np.isfinite(values).all() or (values < 0).any():
            raise ValueError("train_counts must be finite and non-negative")
        logged = np.log1p(values)
        center = np.median(logged, axis=0).astype(np.float32)
        q25 = np.quantile(logged, 0.25, axis=0).astype(np.float32)
        q75 = np.quantile(logged, 0.75, axis=0).astype(np.float32)
        scale = q75 - q25
        scale = np.where(scale > 1e-6, scale, np.float32(1.0)).astype(np.float32)
        return cls(center=center, scale=scale)

    def transform(self, counts: np.ndarray) -> np.ndarray:
        values = np.asarray(counts, dtype=np.float32)
        if not np.isfinite(values).all() or (values < 0).any():
            raise ValueError("counts must be finite and non-negative")
        return ((np.log1p(values) - self.center) / self.scale).astype(np.float32)

    def inverse_transform(self, scaled: np.ndarray) -> np.ndarray:
        values = np.asarray(scaled, dtype=np.float32)
        restored = np.expm1(values * self.scale + self.center)
        return np.maximum(restored, 0.0).astype(np.float32)
