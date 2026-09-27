"""Build a small, official-split manifest for the selected BigEarthNet HDF5 shards."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def build_manifest(
    index_files: list[str | Path],
    metadata_path: str | Path,
    selected_shards: set[str],
    hdf5_root: str | Path | None = None,
) -> pd.DataFrame:
    """Join HDF5 row indexes to official V2 metadata without downloading images."""
    parts = []
    for path in index_files:
        frame = pd.read_csv(path)
        frame = frame[frame["s2_hdf5_file"].isin(selected_shards)].copy()
        frame["hdf5_index"] = frame["index"].astype("int64")
        parts.append(frame.drop(columns=["index"]))
    index = pd.concat(parts, ignore_index=True)
    meta = pd.read_parquet(metadata_path)
    keep = [
        "patch_id", "labels", "split", "country", "s2v1_name",
        "contains_seasonal_snow", "contains_cloud_or_shadow",
    ]
    meta = meta[keep]
    out = index.merge(meta, left_on="s2_folder", right_on="s2v1_name", how="inner")
    out = out[(~out.contains_seasonal_snow) & (~out.contains_cloud_or_shadow)]
    out = out.drop_duplicates("patch_id").reset_index(drop=True)
    if hdf5_root is not None:
        root = Path(hdf5_root)
        out["hdf5_path"] = out["s2_hdf5_file"].map(lambda name: str(root / name))
    return out[
        ["patch_id", "country", "split", "labels", "s2_folder",
         "s2_hdf5_file", "hdf5_index"]
        + (["hdf5_path"] if hdf5_root is not None else [])
    ]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--index-dir", type=Path, default=Path(r"D:\BigEarthNet-partial\index"))
    ap.add_argument("--metadata", type=Path, default=Path(r"D:\BigEarthNet-partial\metadata\V2\metadata.parquet"))
    ap.add_argument("--hdf5-root", type=Path, default=Path(r"D:\BigEarthNet-partial\hdf5"))
    ap.add_argument("--out", type=Path, default=Path(r"D:\BigEarthNet-partial\manifest\manifest.parquet"))
    args = ap.parse_args()
    selected = {
        "bigearthnet_train_p0.hdf5", "bigearthnet_train_p1.hdf5",
        "bigearthnet_val_p0.hdf5", "bigearthnet_test_p0.hdf5",
    }
    files = [args.index_dir / f"bigearthnet_hdf5_{s}.csv" for s in ("train", "val", "test")]
    manifest = build_manifest(files, args.metadata, selected, args.hdf5_root)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    manifest.to_parquet(args.out, index=False)
    summary = {
        "rows": int(len(manifest)),
        "countries": sorted(manifest.country.unique().tolist()),
        "splits": manifest.split.value_counts().to_dict(),
        "shards": manifest.s2_hdf5_file.value_counts().to_dict(),
        "out": str(args.out),
    }
    Path("reports").mkdir(exist_ok=True)
    Path("reports/hdf5_manifest_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
