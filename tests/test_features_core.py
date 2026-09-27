# Unit tests that do not need Spark / full data

from __future__ import annotations

import numpy as np

from scripts.lib.features_core import compute_from_arrays
from scripts.lib.dataset_layout import tile_id_from_patch_id


def test_tile_id():
    pid = "S2B_MSIL2A_20180421T100029_N9999_R122_T33TWM_00_00"
    assert tile_id_from_patch_id(pid) == "S2B_MSIL2A_20180421T100029_N9999_R122_T33TWM"


def test_ndvi_range_and_brightness():
    # High NIR, low red → high NDVI
    b02 = np.full((120, 120), 1000, dtype=np.uint16)
    b03 = np.full((120, 120), 1200, dtype=np.uint16)
    b04 = np.full((120, 120), 800, dtype=np.uint16)
    b08 = np.full((120, 120), 3000, dtype=np.uint16)
    feats = compute_from_arrays(b02, b03, b04, b08)
    assert -1.0 <= feats["ndvi"] <= 1.0
    assert -1.0 <= feats["ndwi"] <= 1.0
    assert feats["ndvi"] > 0.4
    assert abs(feats["brightness"] - np.mean([feats["mean_b02"], feats["mean_b03"], feats["mean_b04"], feats["mean_b08"]])) < 1e-6
