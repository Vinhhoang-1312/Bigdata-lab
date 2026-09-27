# BigEarthNet Hugging Face Partial Acquisition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Download and materialize only the minimum reproducible BigEarthNet subset on `D:\BigEarthNet-partial`, then run the existing Spark assignment pipeline.

**Architecture:** Small HF CSV indexes and official v2 metadata select patch IDs and the minimum shard set. Selected HDF5 records are converted to the existing GeoTIFF layout on D:, after which the current Steps 1-4 run unchanged through `BEN_DATA_ROOT`.

**Tech Stack:** Python 3.10+, `huggingface_hub`, `datasets`, `h5py`, `rasterio`, `numpy`, pandas, PySpark 3.x.

**Spec:** `docs/superpowers/specs/2026-09-27-bigearthnet-hf-partial-acquisition-design.md`

## Global Constraints

- Never download the complete Hugging Face repository.
- Use `D:\BigEarthNet-partial` for all downloaded and materialized data.
- Download another HDF5 shard only when the manifest check proves it is needed.
- Preserve official train/validation/test values; do not create a random split.
- Use only B02, B03, B04, and B08 in the Spark assignment pipeline.
- Record downloaded bytes, file names, SHA256 values, patch counts, country counts, and split counts.
- Do not commit downloaded HDF5, TIFF, or Parquet binaries.

## Review Focus

- HDF5 values are float32 rather than the repository's uint16 convention: inspect range and fail on an unknown convention; test in Task 3.
- `s2_folder` and `s2v1_name` do not join: report missing joins and stop before download; test in Task 2.
- One shard has only one split: manifest must not claim full classification; test in Task 2.
- A selected patch has no complete four-band record: skip it with an auditable error log; test in Task 3.
- D: is unavailable or too small: fail before large downloads with a clear capacity message; test in Task 1.

### Task 1: Acquisition environment and destination checks

**Files:**
- Modify: `requirements.txt`
- Modify: `README.md`
- Create: `scripts/lib/hf_acquisition.py`
- Test: `tests/test_hf_acquisition.py`

**Interfaces:**
- `validate_destination(root: Path, minimum_free_bytes: int) -> None`
- `download_index_files(root: Path) -> dict[str, Path]`
- `download_shards(root: Path, filenames: list[str]) -> list[Path]`

- [ ] Add `datasets`, `huggingface_hub`, and `h5py` dependencies.
- [ ] Implement D: destination validation without creating data in the C: workspace.
- [ ] Use `hf_hub_download(..., repo_type="dataset", local_dir=..., filename=...)` for explicit files only.
- [ ] Write an acquisition manifest containing file size and SHA256.
- [ ] Add mocked tests proving allow-listed files are requested and unknown files are rejected.

### Task 2: Select the minimum shard set

**Files:**
- Create: `scripts/00_acquire_hf_subset.py`
- Create: `data/subset/hf_candidate_manifest.parquet` (generated, not binary source data)
- Test: `tests/test_hf_acquisition.py`

**Interfaces:**
- `build_candidate_manifest(index_dir: Path, metadata_path: Path) -> pandas.DataFrame`
- `select_minimum_shards(manifest: pandas.DataFrame, target_patches: int = 50000, min_countries: int = 3) -> dict`

- [ ] Load only the three small CSV indexes.
- [ ] Join `s2_folder` to official `s2v1_name`, preserving `patch_id`, `labels`, `country`, and official `split`.
- [ ] Evaluate one shard per split first; add shards in deterministic size order only when required.
- [ ] Emit `selected_shards.txt`, `selected_patch_ids.txt`, and `hf_candidate_manifest.parquet`.
- [ ] Stop with a clear report if the minimum shard set cannot satisfy 50,000 patches and 3 countries.

### Task 3: Download and convert selected HDF5 records

**Files:**
- Create: `scripts/01_materialize_hf_subset.py`
- Modify: `scripts/lib/dataset_layout.py` only if D: layout needs an explicit raw root
- Test: `tests/test_hf_materializer.py`

**Interfaces:**
- `materialize_shard(shard_path: Path, rows: pandas.DataFrame, raw_root: Path) -> dict`
- `write_band_tiff(array: numpy.ndarray, path: Path, value_mode: str) -> None`

- [ ] Open HDF5 datasets `images`, `labels19`, and the CSV-provided row index with h5py.
- [ ] Select the documented Sentinel-2 positions for B02/B03/B04/B08.
- [ ] Verify shape `(120, 120)` and value range before writing; convert to uint16 reflectance-times-10^4 only when the range confirms reflectance input.
- [ ] Write only selected patch folders and four TIFF files per patch; write a missing/corrupt error log.
- [ ] Produce `materialization_stats.json` with rows requested, rows written, skipped rows, bands, and bytes.

### Task 4: Integrate D: with the existing pipeline

**Files:**
- Modify: `README.md`
- Modify: `scripts/run_pipeline.py` only if an acquisition/materialization pre-step is needed
- Test: existing `tests/test_features_core.py` plus a 24-patch smoke run

- [ ] Run subset selection against the D: metadata path.
- [ ] Run `01_ingest_and_pack.py --data-root D:\BigEarthNet-partial --limit 500`.
- [ ] Run feature, SQL, and classification scripts against the same root.
- [ ] Verify all outputs remain under the configured local mirror and no raw data is copied into C:.

### Task 5: Evidence and report update

**Files:**
- Modify: `reports/REPORT_OUTLINE.md`
- Create: `reports/HF_PARTIAL_ACQUISITION.md`

- [ ] Record source URL, selected shard names, exact sizes, checksums, conversion assumptions, and final counts.
- [ ] Add a limitation note if the selected subset is below 50,000 or lacks an official split.
- [ ] Add reproducibility commands using `D:\BigEarthNet-partial` and `BEN_DATA_ROOT`.

## Verification Gate

- [ ] No call downloads a full snapshot or unlisted HDF5 file.
- [ ] The manifest and SHA256 file exist before Spark processing.
- [ ] Four required bands exist for every materialized patch used by Step 1.
- [ ] Official split values are preserved.
- [ ] The final report states clearly whether the subset meets the 50,000/3-country grading floor.
