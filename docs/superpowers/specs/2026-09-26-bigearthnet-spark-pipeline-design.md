# BigEarthNet Spark Pipeline — Design Spec

**Course:** BDA501 Individual Assignment 2  
**Date:** 2026-09-26  
**Scope constraint:** Do **not** download full BigEarthNet-S2 (~59 GiB). Process a graded-compliant subset locally/HDFS.

## Goal

End-to-end Apache Spark pipeline on BigEarthNet-S2 v2.0 (recommended patches only): storage packing → executor-side features → Spark SQL analyses → DataFrame/SQL classifiers. Meet the ≥50 000 patches / ≥3 countries floor without full-dataset download. Full dataset remains optional bonus only if shared HDFS already has it.

## Defaults (decided)

| Decision | Choice | Why |
|---|---|---|
| Patch count | **60 000** recommended patches (buffer over 50k) | Safe against corrupt drops; still far below full set |
| Countries | **Top-3 countries by patch count** in `metadata.parquet` after filter | Guarantees ≥3 countries + enough volume |
| Bands | B02, B03, B04, B08 only (10 m) | Spec requirement; avoid size mismatch |
| Compute | PySpark 3.x + `rasterio` on executors via `mapPartitions` | Spec forbids driver loops / MLlib requirement |
| Storage root | `/bigearthnet/{raw,packed,metadata,features,results}/` | Exact HDFS layout from brief |
| Local debug | **500** patches first, then scale to 60k | Spec pitfall: debug small, then scale |
| Data source priority | (1) course HDFS shared copy → (2) shared NAS/`tar.zst` selective extract → (3) never full personal download | Honors “chỉ download 1 phần” |
| Split | Official `split` column only | Avoid −30% Step 4 deduction |
| Classifiers | Rule-based (≤3 features) + nearest-centroid + majority baseline | Full Step 4 without MLlib |
| Deliverables | 4 step notebooks/scripts + README + requirements + PDF report ≤8 pages | Match grading rubric |

## Approaches considered

1. **Full download + process everything** — highest bonus potential; rejected (user constraint + disk/time).
2. **Single-country HF/LMDB sample** — easy download; fails ≥3 countries rule.
3. **Metadata-first subset (recommended)** — download tiny `metadata.parquet`, select patches/tiles, pull only those from shared HDFS or selective `tar` extract. Meets grade floor with minimal bytes.

## Architecture

```
metadata.parquet (local, small)
        │
        ▼
[0] subset: ≥3 countries, 60k patch_ids + tile list
        │
        ▼
[1] raw GeoTIFF (selected only) → pack → /bigearthnet/packed (Parquet, partition=country)
        │
        ▼
[2] mapPartitions(MemoryFile) → features Parquet → /bigearthnet/features
        │
        ├──► [3] temp views + SQL → /bigearthnet/results (+ charts)
        └──► [4] train-only stats → rule + centroid → metrics → /bigearthnet/results
```

### Step 0 — Subset builder (critical for partial download)

1. Load `metadata.parquet` (recommended patches; no snow/cloud/shadow set).
2. Count patches per country; pick top-3; sample/select **60 000** rows preserving official `split` ratios as much as possible (stratify by country×split).
3. Derive `tile_id` = `patch_id` without last two `_`-tokens.
4. Emit:
   - `data/subset/patch_ids.txt`
   - `data/subset/tiles.txt`
   - `data/subset/subset_metadata.parquet`
   - printed counts: total, per country, per split
5. Materialize raw images:
   - **Preferred:** `hdfs dfs -cp` / get only listed tiles from course store into `/bigearthnet/raw/`.
   - **Fallback:** `tar --use-compress-program=unzstd -xf BigEarthNet-S2.tar.zst` with tile path members only (archive may live on lab server; do not keep full extract).
   - **Debug:** copy 500 patches manually.

### Step 1 — Packing

- Input: raw `<TILE>/<PATCH>/<PATCH>_{B02,B03,B04,B08}.tif` + subset metadata.
- Join metadata; read 4 band files as binary; one Parquet row per patch: `patch_id, country, split, labels, b02, b03, b04, b08` (bytes).
- Partition by `country`. Tune `repartition` so output file count ≪ input file count.
- Report before/after: patch counts, file counts, `hdfs dfs -du -s -h`, `-count`, block size, replication.

### Step 2 — Features (executors only)

Per patch, decode with `rasterio.io.MemoryFile`, cast to float32 reflectance (`/1e4`), compute:

- `mean_b02..b08`, `std_b02..b08`
- `ndvi`, `ndwi` (pixel-wise then mean; ε=1e-6)
- `brightness` = mean of four band means
- `error_flag` on corrupt decode (do not fail job)

Output schema explicit; write `/bigearthnet/features` partitioned by country.  
Validation: row counts in/out; null/NaN/Inf; `describe()`; NDVI/NDWI ∈ [-1,1]; NumPy check on 3 random patches; runtime for ≥2 partition counts + Spark UI screenshot.

### Step 3 — Spark SQL (≥5 analyses, ≥3 charts)

Register temp views; save every result under `/bigearthnet/results/sql_*`.

1. Class distribution + labels-per-patch  
2. Patches per country and country×split  
3. Top-10 co-occurring class pairs  
4. Mean NDVI/brightness by country; mean features by class  
5. Custom: e.g. NDVI distribution by split or single-label vs multi-label rate  

Interpret class imbalance explicitly in report.

### Step 4 — Classification (no leakage)

1. From train label frequencies, pick **3–5** frequent classes (brief says “3–5”; use exactly that range — not 35). Keep patches whose `labels` contain **exactly one** of these as target.
2. Official `split` only.
3. Rule baseline: `CASE WHEN` on ≤3 features; thresholds from **train** only (quantiles / mean±k·std).
4. Nearest centroid: standardize with train mean/std; `groupBy` centroids; `crossJoin` broadcast centroids; distance; window `row_number` for prediction.
5. Metrics in Spark: confusion (`groupBy().pivot()`), accuracy, per-class P/R/F1, macro-F1 vs majority baseline.
6. Full feature set vs reduced set (≤5 features chosen on **validation** by class mean separation or |corr| with one-hot targets — careful: selection on val only, fit still train-only).
7. Discussion: confusion reasons, useful features, limits of centroids on multi-spectral summaries.

## Drop-in contract (when dataset arrives)

```
Lab2-Bigdata/data/incoming/
  metadata.parquet
  BigEarthNet-S2/<TILE>/<PATCH>/<PATCH>_B0x.tif
```

Auto-discovered by `scripts/lib/dataset_layout.py` (also `BEN_DATA_ROOT`).  
Then: `python scripts/run_pipeline.py --n-patches 60000`

## Repo layout

```
Lab2-Bigdata/
  README.md
  requirements.txt
  docs/DATASET_AZ.md
  data/incoming/        # USER DROP ZONE
  data/subset/          # manifests only
  scripts/00..04_*.py + make_synthetic_fixture.py + run_pipeline.py
  scripts/lib/
  hdfs_mirror/bigearthnet/   # local stand-in for HDFS roots
  reports/
```
## Non-goals

- Spark MLlib (bonus only if time)
- 20 m/60 m bands
- Full 549 488 patches
- Driver-side `collect`/`toPandas` for feature compute
- Random train/test split

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| No course HDFS | Selective extract from lab NAS; document exact byte source |
| Corrupt TIFFs | `error_flag`; filter before Step 4 |
| Class imbalance / too few single-label patches | Choose frequent classes; report exact kept counts; if &lt;N, widen to 5 classes or loosen only with documented justification |
| uint16 wrap | Always float before NDVI/NDWI |
| Small-files packing OOM | `binaryFiles` + mapPartitions; control partition size |
| Leakage | Freeze train stats; val only for hyperparams/feature subset; test once |

## Success criteria (grading-aligned)

- Exact patch counts logged at every step  
- HDFS evidence for packing win  
- Features on executors + NumPy match  
- ≥3 SQL charts + imbalance commentary  
- Both classifiers + majority baseline; no random split  
- Reproducible README commands  
- Cite AI assistance in report  

## AI citation (required by brief)

Document in README/report that Cursor/Composer assisted planning and code scaffolding; student remains responsible for oral defense.
