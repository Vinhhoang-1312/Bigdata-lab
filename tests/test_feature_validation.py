from __future__ import annotations

from scripts.feature_validation import describe_rows_to_dict


def test_describe_rows_to_dict_uses_summary_rows():
    rows = [
        {"summary": "count", "ndvi": "3.0", "brightness": "0.2"},
        {"summary": "mean", "ndvi": "0.5", "brightness": "0.4"},
    ]
    assert describe_rows_to_dict(rows) == {
        "summary": {"0": "count", "1": "mean"},
        "ndvi": {"0": "3.0", "1": "0.5"},
        "brightness": {"0": "0.2", "1": "0.4"},
    }
