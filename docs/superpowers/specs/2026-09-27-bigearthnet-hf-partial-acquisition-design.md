# BigEarthNet Hugging Face Partial Acquisition - Design Spec

**Course:** BDA501 Individual Assignment 2  
**Date:** 2026-09-27  
**Scope:** Use only a small, reproducible subset from `lc-col/bigearthnet`; never download the complete repository.

## Goal

Acquire the smallest practical BigEarthNet subset on drive `D:` and feed it into the existing Spark pipeline. The subset must preserve official train/validation/test information and reach the assignment target when the selected shards contain enough rows.

## Source and interpretation

The Hugging Face repository is a converted HDF5 mirror. Its CSV files are small indexes; image pixels live in compressed HDF5 shards. `load_dataset()` is used for the indexes, while `hf_hub_download()` downloads only explicitly selected shard files.

## Acquisition policy

- Destination root: `D:\BigEarthNet-partial`.
- Download the three split index CSV files first; these are only metadata indexes.
- Join the index with official BigEarthNet v2 `metadata.parquet` using the v1 patch identifier (`s2_folder` / `s2v1_name`).
- Build a manifest for the smallest shard set that contains at least 50,000 usable patches from at least 3 countries and all official splits.
- Start with one shard per split (`train_p0`, `val_p0`, `test_p0`). Download another shard only if the manifest check proves it is necessary.
- Keep the selected patch IDs and shard names in versioned text/Parquet manifests so the download is reproducible.
- Never call `snapshot_download()` without `allow_patterns`; never download all HDF5 files.

## Data conversion

HDF5 stores 12 Sentinel-2 bands as arrays. The converter selects B02, B03, B04, and B08, verifies shape and value range, and writes only selected patches as GeoTIFF files under the existing `<TILE>/<PATCH>/<PATCH>_Bxx.tif` contract. If HDF5 values are already reflectance, it scales them to the repository's uint16 reflectance-times-10^4 convention; otherwise it records the detected convention and stops rather than silently changing values.

## Pipeline integration

The existing scripts remain the source of truth after materialization:

```text
D:\BigEarthNet-partial\raw_tif
        -> 01_ingest_and_pack.py
        -> 02_feature_engineering.py
        -> 03_spark_sql_analysis.py
        -> 04_classification.py
```

`BEN_DATA_ROOT` or `--data-root` points the pipeline to D:. No large data is copied into the C: workspace.

## Important grading boundary

One shard alone can run a technical demo but normally contains only one split, so it cannot support the full official test evaluation. The default plan therefore treats one shard per split as one small subset and adds shards only after checking the 50,000-patch and three-country conditions.

## Acceptance criteria

- Total downloaded HDF5 bytes are recorded and are less than the complete repository.
- Manifest records exact files, sizes, SHA256 values, patch count, country count, and split counts.
- No full HDF5 snapshot is present on D:.
- Materialized data contains only B02/B03/B04/B08 for selected patches.
- Existing Spark pipeline can run with `--data-root D:\BigEarthNet-partial`.
- Report clearly states the mirror source, selected shard names, conversion step, and any limitation if the subset is below the grading floor.
