"""Auditable scoring for the Spark MLlib trip-pattern model."""

from __future__ import annotations

from math import sqrt


FEATURE_LABELS = ("Distance", "Duration", "Fare", "Average speed")


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
