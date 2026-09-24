"""Read-only, diagnostic weekly-naive score on tensor test buckets."""

from __future__ import annotations

import numpy as np


WEEK_BUCKETS = 7 * 48


def weekly_naive_report(arrays: dict, metadata: dict) -> dict:
    counts = np.asarray(arrays["counts"], dtype=np.float64)
    observed = np.asarray(arrays["observed"], dtype=bool)
    if counts.ndim != 2 or observed.shape != counts.shape:
        raise ValueError("counts and observed must have the same [time, zone] shape")
    left, right = (int(value) for value in metadata["split_indices"]["test"])
    if left < WEEK_BUCKETS or right > len(counts) or left >= right:
        raise ValueError("test period needs a full week of prior history")

    actual = counts[left:right]
    prediction = counts[left - WEEK_BUCKETS:right - WEEK_BUCKETS]
    target_mask = observed[left:right]
    matched = target_mask & observed[left - WEEK_BUCKETS:right - WEEK_BUCKETS]
    target_count = int(target_mask.sum())
    matched_count = int(matched.sum())
    error = prediction[matched] - actual[matched]
    actual_sum = float(actual[matched].sum())
    return {
        "method": "same_zone_same_half_hour_previous_week",
        "lag_buckets": WEEK_BUCKETS,
        "test_bucket_count": right - left,
        "target_count": target_count,
        "matched_count": matched_count,
        "missing_history_count": target_count - matched_count,
        "mae": float(np.abs(error).mean()) if matched_count else None,
        "rmse": float(np.sqrt(np.square(error).mean())) if matched_count else None,
        "wape": float(np.abs(error).sum() / actual_sum) if actual_sum > 0 else None,
        "signed_bias": float(error.sum() / actual_sum) if actual_sum > 0 else None,
    }
