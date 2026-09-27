"""Auditable scoring for the Spark MLlib trip-pattern model."""

from __future__ import annotations

from math import isfinite, log1p, sqrt
from typing import Sequence


FEATURE_LABELS = ("Distance", "Duration", "Fare", "Average speed")


def raw_trip_features(
    distance_miles: float,
    duration_minutes: float,
    fare_usd: float,
) -> list[float]:
    values = (float(distance_miles), float(duration_minutes), float(fare_usd))
    if not all(isfinite(value) for value in values):
        raise ValueError("trip anomaly measurements must be finite")
    distance, duration, fare = values
    if distance <= 0 or duration <= 0 or fare < 0:
        raise ValueError("trip anomaly measurements are outside the eligible range")
    speed_mph = distance * 60.0 / duration
    return [log1p(distance), log1p(duration), log1p(fare), log1p(speed_mph)]


def _validated_vector(values: Sequence[float], *, label: str) -> list[float]:
    if len(values) != 4:
        raise ValueError(f"{label} must have four dimensions")
    result = [float(value) for value in values]
    if not all(isfinite(value) for value in result):
        raise ValueError(f"{label} must contain finite values")
    return result


def standardize_features(
    raw: Sequence[float],
    mean: Sequence[float],
    std: Sequence[float],
) -> list[float]:
    raw_values = _validated_vector(raw, label="raw trip features")
    mean_values = _validated_vector(mean, label="scaler mean")
    std_values = _validated_vector(std, label="scaler standard deviation")
    if any(value <= 0 for value in std_values):
        raise ValueError("scaler standard deviation must be positive")
    return [(value - center) / scale
            for value, center, scale in zip(raw_values, mean_values, std_values)]


def score_distance(values, center) -> float:
    if len(values) != 4 or len(center) != 4:
        raise ValueError("trip anomaly features must have four dimensions")
    return sqrt(sum((float(value) - float(reference)) ** 2
                    for value, reference in zip(values, center)))


def explain_deviation(values, center) -> str:
    if len(values) != 4 or len(center) != 4:
        raise ValueError("trip anomaly features must have four dimensions")
    deviations = [float(value) - float(reference)
                  for value, reference in zip(values, center)]
    index = max(range(4), key=lambda item: abs(deviations[item]))
    direction = "above" if deviations[index] >= 0 else "below"
    return f"{FEATURE_LABELS[index]} {direction} learned pattern"


def score_nearest_cluster(
    values: Sequence[float],
    centers: Sequence[Sequence[float]],
) -> tuple[int, float, str]:
    values_checked = _validated_vector(values, label="trip anomaly features")
    if not centers:
        raise ValueError("cluster centers must not be empty")
    checked_centers = [
        _validated_vector(center, label="cluster center") for center in centers
    ]
    distances = [score_distance(values_checked, center) for center in checked_centers]
    cluster = min(range(len(distances)), key=lambda index: distances[index])
    return cluster, distances[cluster], explain_deviation(values_checked, checked_centers[cluster])
