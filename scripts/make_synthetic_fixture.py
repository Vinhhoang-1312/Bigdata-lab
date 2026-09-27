#!/usr/bin/env python3
"""Create a tiny BigEarthNet-shaped fixture so the pipeline can smoke-test without the real archive."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.lib.constants import BANDS_10M, PATCH_HW
from scripts.lib.dataset_layout import band_filename, tile_id_from_patch_id
from scripts.lib.paths import data_incoming


def _write_tif(path: Path, arr: np.ndarray) -> None:
    import rasterio
    from rasterio.transform import from_origin

    path.parent.mkdir(parents=True, exist_ok=True)
    transform = from_origin(0, 0, 10, 10)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=arr.shape[0],
        width=arr.shape[1],
        count=1,
        dtype=arr.dtype,
        transform=transform,
    ) as dst:
        dst.write(arr, 1)


def make_patch(patch_id: str, country: str, split: str, labels: list[str], seed: int) -> dict:
    rng = np.random.default_rng(seed)
    # Simulate vegetated vs water-ish reflectance
    base = 1500 + rng.integers(0, 500)
    if "Inland waters" in labels or "Marine waters" in labels:
        b02 = np.full((PATCH_HW, PATCH_HW), base + 200, dtype=np.uint16)
        b03 = np.full((PATCH_HW, PATCH_HW), base + 400, dtype=np.uint16)
        b04 = np.full((PATCH_HW, PATCH_HW), base + 100, dtype=np.uint16)
        b08 = np.full((PATCH_HW, PATCH_HW), base - 200, dtype=np.uint16)
    else:
        b02 = np.full((PATCH_HW, PATCH_HW), base, dtype=np.uint16)
        b03 = np.full((PATCH_HW, PATCH_HW), base + 100, dtype=np.uint16)
        b04 = np.full((PATCH_HW, PATCH_HW), base + 50, dtype=np.uint16)
        b08 = np.full((PATCH_HW, PATCH_HW), base + 800, dtype=np.uint16)
    # light noise
    for a in (b02, b03, b04, b08):
        a += rng.integers(0, 50, size=a.shape, dtype=np.uint16)
    return {
        "patch_id": patch_id,
        "country": country,
        "split": split,
        "labels": labels,
        "bands": {"B02": b02, "B03": b03, "B04": b04, "B08": b08},
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--n-per-country", type=int, default=8)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    out = Path(args.out) if args.out else data_incoming()
    images = out / "BigEarthNet-S2"
    if images.exists():
        # only clear if it looks like our fixture (safe-ish)
        pass

    countries = ["Austria", "Portugal", "Finland"]
    splits = ["train", "validation", "test"]
    class_pool = [
        ["Arable land"],
        ["Pastures"],
        ["Coniferous forest"],
        ["Mixed forest"],
        ["Inland waters"],
        ["Arable land", "Pastures"],
    ]

    rows = []
    k = 0
    for country in countries:
        for i in range(args.n_per_country):
            split = splits[i % len(splits)]
            labels = class_pool[i % len(class_pool)]
            # Unique plausible patch_id; tile_id = all but last two tokens
            patch_id = (
                f"S2A_MSIL2A_20170613T101031_N9999_R022_"
                f"T{30 + countries.index(country)}TUM_{i:02d}_00"
            )
            item = make_patch(patch_id, country, split, labels, seed=1000 + k)
            tile = tile_id_from_patch_id(patch_id)
            for band in BANDS_10M:
                path = images / tile / patch_id / band_filename(patch_id, band)
                _write_tif(path, item["bands"][band])
            rows.append(
                {
                    "patch_id": patch_id,
                    "labels": labels,
                    "split": split,
                    "country": country,
                    "s1_name": f"S1_{patch_id}",
                    "s2v1_name": f"v1_{patch_id}",
                    "contains_seasonal_snow": False,
                    "contains_cloud_or_shadow": False,
                }
            )
            k += 1

    meta = pd.DataFrame(rows)
    meta_path = out / "metadata.parquet"
    meta.to_parquet(meta_path, index=False)
    print(f"Wrote fixture metadata: {meta_path} ({len(meta)} patches)")
    print(f"Images root: {images}")
    print("Next: python scripts/00_build_subset.py --n-patches 24 --n-countries 3")


if __name__ == "__main__":
    main()
