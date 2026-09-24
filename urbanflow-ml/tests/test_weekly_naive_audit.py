"""Contract tests for a diagnostic weekly-naive comparison."""

import numpy as np
import pytest

from forecasting.weekly_naive_audit import weekly_naive_report


def test_uses_exactly_one_week_earlier_and_only_observed_pairs():
    counts = np.zeros((340, 2), dtype=np.float32)
    counts[0] = [3, 20]
    counts[1] = [5, 30]
    counts[336] = [4, 18]
    counts[337] = [7, 32]
    observed = np.ones_like(counts, dtype=bool)
    observed[1, 1] = False
    observed[337, 0] = False
    arrays = {"counts": counts, "observed": observed}
    metadata = {"split_indices": {"test": [336, 338]}}

    report = weekly_naive_report(arrays, metadata)

    assert report["target_count"] == 3
    assert report["matched_count"] == 2
    assert report["missing_history_count"] == 1
    assert report["mae"] == 1.5
    assert report["wape"] == pytest.approx(3 / 22)


def test_rejects_test_period_without_full_week_history():
    arrays = {"counts": np.zeros((340, 1)), "observed": np.ones((340, 1), dtype=bool)}
    with pytest.raises(ValueError, match="full week"):
        weekly_naive_report(arrays, {"split_indices": {"test": [335, 340]}})


def test_zero_demand_has_undefined_wape():
    arrays = {"counts": np.zeros((337, 1)), "observed": np.ones((337, 1), dtype=bool)}
    report = weekly_naive_report(arrays, {"split_indices": {"test": [336, 337]}})
    assert report["wape"] is None
    assert report["mae"] == 0


def test_missing_weekly_history_returns_unavailable_metrics():
    arrays = {"counts": np.zeros((337, 1)), "observed": np.ones((337, 1), dtype=bool)}
    arrays["observed"][0, 0] = False
    report = weekly_naive_report(arrays, {"split_indices": {"test": [336, 337]}})
    assert report["target_count"] == 1
    assert report["matched_count"] == 0
    assert report["mae"] is None
    assert report["wape"] is None
