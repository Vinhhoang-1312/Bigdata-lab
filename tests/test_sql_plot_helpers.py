from __future__ import annotations

import pandas as pd

from scripts.sql_plot_helpers import records_to_frame


def test_records_to_frame_builds_small_local_plot_table():
    result = records_to_frame([{"country": "Austria", "n": 2}])
    assert isinstance(result, pd.DataFrame)
    assert result.to_dict(orient="records") == [{"country": "Austria", "n": 2}]
