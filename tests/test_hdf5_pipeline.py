from pathlib import Path

import h5py
import numpy as np
import pandas as pd

from scripts.hdf5_manifest import build_manifest
from scripts.hdf5_features import compute_hdf5_row


def test_build_manifest_joins_index_and_metadata(tmp_path: Path):
    index = tmp_path / "index.csv"
    pd.DataFrame(
        {
            "s2_folder": ["patch_a", "patch_b"],
            "s2_hdf5_file": ["bigearthnet_train_p0.hdf5"] * 2,
            "index": [3, 4],
        }
    ).to_csv(index, index=False)
    meta = tmp_path / "metadata.parquet"
    pd.DataFrame(
        {
            "patch_id": ["patch_a", "patch_b"],
            "s2v1_name": ["patch_a", "patch_b"],
            "labels": [["Arable land"], ["Pastures"]],
            "split": ["train", "train"],
            "country": ["Austria", "Ireland"],
            "contains_seasonal_snow": [False, False],
            "contains_cloud_or_shadow": [False, False],
        }
    ).to_parquet(meta)
    out = build_manifest([index], meta, {"bigearthnet_train_p0.hdf5"})
    assert list(out.patch_id) == ["patch_a", "patch_b"]
    assert list(out.hdf5_index) == [3, 4]


def test_compute_hdf5_row_selects_sentinel_bands(tmp_path: Path):
    p = tmp_path / "x.hdf5"
    images = np.zeros((1, 12, 120, 120), dtype=np.float32)
    images[:, 1] = 1000  # B02
    images[:, 2] = 2000  # B03
    images[:, 3] = 3000  # B04
    images[:, 7] = 5000  # B08
    with h5py.File(p, "w") as h5:
        h5.create_dataset("images", data=images)
    row = compute_hdf5_row(
        patch_id="p",
        country="Austria",
        split="train",
        labels=["Arable land"],
        hdf5_path=str(p),
        hdf5_index=0,
    )
    assert row["error_flag"] is False
    assert abs(row["mean_b02"] - 0.1) < 1e-6
    assert abs(row["mean_b08"] - 0.5) < 1e-6
