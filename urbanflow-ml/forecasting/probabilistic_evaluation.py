"""Metrics and anomaly candidates for quantile demand forecasts."""

from __future__ import annotations

import numpy as np


def _validate(actual: np.ndarray, predictions: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    actual_values = np.asarray(actual, dtype=np.float64)
    predicted_values = np.asarray(predictions, dtype=np.float64)
    if predicted_values.shape != actual_values.shape + (3,):
        raise ValueError("predictions must have shape actual.shape + [3]")
    if not np.isfinite(actual_values).all() or not np.isfinite(predicted_values).all():
        raise ValueError("actual and predictions must be finite")
    if (predicted_values[..., 0] > predicted_values[..., 1]).any() or (
        predicted_values[..., 1] > predicted_values[..., 2]
    ).any():
        raise ValueError("predicted quantiles must be ordered")
    return actual_values, predicted_values


def probabilistic_metrics(actual: np.ndarray, predictions: np.ndarray) -> dict[str, float]:
    actual_values, predicted_values = _validate(actual, predictions)
    metrics: dict[str, float] = {}
    for position, quantile in enumerate((0.1, 0.5, 0.9)):
        error = actual_values - predicted_values[..., position]
        loss = np.maximum(quantile * error, (quantile - 1.0) * error)
        metrics[f"pinball_p{int(quantile * 100):02d}"] = float(loss.mean())
    covered = (actual_values >= predicted_values[..., 0]) & (actual_values <= predicted_values[..., 2])
    metrics["coverage_10_90"] = float(covered.mean())
    metrics["mean_interval_width"] = float(
        (predicted_values[..., 2] - predicted_values[..., 0]).mean()
    )
    return metrics


def anomaly_candidates(actual: np.ndarray, predictions: np.ndarray) -> list[dict]:
    actual_values, predicted_values = _validate(actual, predictions)
    flat_actual = actual_values.reshape(-1)
    flat_predictions = predicted_values.reshape(-1, 3)
    candidates: list[dict] = []
    for index, (value, quantiles) in enumerate(zip(flat_actual, flat_predictions, strict=True)):
        lower, median, upper = (float(item) for item in quantiles)
        direction = None
        breach = 0.0
        if value < lower:
            direction = "low"
            breach = lower - float(value)
        elif value > upper:
            direction = "high"
            breach = float(value) - upper
        if direction is None:
            continue
        width = max(upper - lower, 1e-6)
        candidates.append(
            {
                "flat_index": index,
                "actual": float(value),
                "p10": lower,
                "p50": median,
                "p90": upper,
                "residual": float(value) - median,
                "direction": direction,
                "severity": breach / width,
            }
        )
    return candidates
