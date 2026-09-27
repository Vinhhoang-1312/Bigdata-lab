# Unit tests that do not need Spark / full data

from __future__ import annotations

import numpy as np

from scripts.lib.features_core import compute_from_arrays, compute_richer_from_hdf5_image
from scripts.lib.dataset_layout import tile_id_from_patch_id


def test_tile_id():
    pid = "S2B_MSIL2A_20180421T100029_N9999_R122_T33TWM_00_00"
    assert tile_id_from_patch_id(pid) == "S2B_MSIL2A_20180421T100029_N9999_R122_T33TWM"


def test_ndvi_range_and_brightness():
    # High NIR, low red -> high NDVI
    b02 = np.full((120, 120), 1000, dtype=np.uint16)
    b03 = np.full((120, 120), 1200, dtype=np.uint16)
    b04 = np.full((120, 120), 800, dtype=np.uint16)
    b08 = np.full((120, 120), 3000, dtype=np.uint16)
    feats = compute_from_arrays(b02, b03, b04, b08)
    assert -1.0 <= feats["ndvi"] <= 1.0
    assert -1.0 <= feats["ndwi"] <= 1.0
    assert feats["ndvi"] > 0.4
    assert abs(feats["brightness"] - np.mean([feats["mean_b02"], feats["mean_b03"], feats["mean_b04"], feats["mean_b08"]])) < 1e-6


def test_compute_richer_from_hdf5_image_uses_20m_grid_and_ndvi_percentiles():
    image = np.zeros((12, 120, 120), dtype=np.float32)
    image[1] = 1000   # B02
    image[2] = 2000   # B03
    image[3] = 3000   # B04
    image[7] = 5000   # B08
    image[4] = 6000   # B05
    image[5] = 7000   # B06
    image[6] = 8000   # B07
    image[8] = 9000   # B8A
    image[10] = 4000  # B11
    image[11] = 2000  # B12
    image[3, :, :60] = 1000  # vary NDVI across patch

    out = compute_richer_from_hdf5_image(image)

    assert abs(out["mean_b05"] - 0.6) < 1e-6
    assert abs(out["std_b12"] - 0.0) < 1e-6
    assert abs(out["ndmi"] - ((0.9 - 0.4) / (0.9 + 0.4 + 1e-6))) < 1e-6
    assert abs(out["ndbi"] - ((0.4 - 0.9) / (0.4 + 0.9 + 1e-6))) < 1e-6
    assert out["ndvi_p10"] < out["ndvi_p50"] < out["ndvi_p90"]
