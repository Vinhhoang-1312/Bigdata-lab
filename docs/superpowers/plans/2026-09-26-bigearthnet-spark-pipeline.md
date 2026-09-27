# BigEarthNet Distributed Spark Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reproducible Spark-only BigEarthNet-S2 v2.0 pipeline on a **60 000-patch / ≥3-country subset** (no full 59 GiB download), covering packing, executor features, SQL analysis, and DataFrame/SQL classification for BDA501 Assignment 2.

**Architecture:** Metadata-first subset → HDFS raw (selected tiles) → Parquet pack (partitioned by country) → `mapPartitions` + rasterio MemoryFile features → Spark SQL results → train-only rule + nearest-centroid classifiers. Driver never loops over rasters.

**Tech Stack:** Python 3.10+, PySpark 3.x, rasterio, numpy, pyarrow, pandas (validation/plots only), matplotlib/seaborn (charts), HDFS (or local `file://` mirror of the same path layout for dry-run).

## Global Constraints

- Use only RDD / DataFrame / Spark SQL (no MLlib required).
- Feature compute on executors (`mapPartitions` or UDF); no `collect()`/`toPandas()` for raster processing.
- Official `split` column only; train-only statistics for model fitting.
- Process ≥50 000 patches from ≥3 countries; log exact counts every step.
- HDFS roots: `/bigearthnet/{raw,packed,metadata,features,results}/`.
- Bands: B02, B03, B04, B08 only; cast to float before indices; ε=1e-6.
- Corrupt files → `error_flag=true`, do not crash.
- Brief OCR note: dataset has **19** classes → treat “35 frequent classes” / “35 features” as **3–5** classes / **≤5** features.
- Do not commit large binaries; only manifests under `data/subset/`.
- Cite AI tools in README/report.

---

## File Structure

| Path | Responsibility |
|---|---|
| `requirements.txt` | Pinned deps |
| `README.md` | Env, HDFS layout, run commands, AI citation |
| `conf/spark-defaults.example.conf` | Example executor memory, `spark.sql.shuffle.partitions` |
| `scripts/00_build_subset.py` | Metadata → 60k subset manifests |
| `scripts/01_ingest_and_pack.py` | Raw → packed Parquet + storage stats |
| `scripts/02_feature_engineering.py` | Executor features + validation |
| `scripts/03_spark_sql_analysis.py` | ≥5 SQL analyses + chart exports |
| `scripts/04_classification.py` | Rule + centroid + metrics |
| `scripts/lib/spark_session.py` | Shared SparkSession builder |
| `scripts/lib/paths.py` | HDFS/local path constants |
| `scripts/lib/features_core.py` | Pure numpy feature fn (shared Spark + NumPy check) |
| `notebooks/01..04_*.ipynb` | Thin demo notebooks (optional; scripts are source of truth) |
| `reports/figures/` | Charts for PDF |
| `data/subset/` | `subset_metadata.parquet`, `patch_ids.txt`, `tiles.txt`, `COUNTS.md` |

---

### Task 1: Repo scaffold + Spark helpers

**Files:**
- Create: `requirements.txt`
- Create: `README.md` (stub sections)
- Create: `conf/spark-defaults.example.conf`
- Create: `scripts/lib/paths.py`
- Create: `scripts/lib/spark_session.py`
- Create: `data/subset/.gitkeep`
- Create: `reports/figures/.gitkeep`

**Interfaces:**
- Produces: `get_spark(app_name: str) -> SparkSession`; `HDFS_ROOT = "/bigearthnet"`; path helpers `raw_path()`, `packed_path()`, `metadata_path()`, `features_path()`, `results_path()`; env `BEN_USE_LOCAL=1` remaps to `file:///.../hdfs_mirror/bigearthnet`.

- [ ] **Step 1: Write `requirements.txt`**

```
pyspark>=3.4,<4
rasterio>=1.3
numpy>=1.24
pandas>=2.0
pyarrow>=14
matplotlib>=3.7
seaborn>=0.13
pyyaml>=6
```

- [ ] **Step 2: Implement `scripts/lib/paths.py` and `spark_session.py`**

```python
# paths.py — BEN_USE_LOCAL remaps /bigearthnet → <repo>/hdfs_mirror/bigearthnet
# spark_session.py — builder with parquet + Kryo; optional spark.master from env
```

- [ ] **Step 3: Stub README with run order `00→04` and AI citation placeholder**

- [ ] **Step 4: Verify import**

Run: `python -c "from scripts.lib.spark_session import get_spark; print('ok')"`  
Expected: `ok` (Spark may lazy-init on first use)

---

### Task 2: Subset builder (no full download)

**Files:**
- Create: `scripts/00_build_subset.py`
- Create: `data/subset/COUNTS.md` (generated)

**Interfaces:**
- Consumes: local path to `metadata.parquet` (CLI `--metadata`)
- Produces: `data/subset/subset_metadata.parquet`, `patch_ids.txt`, `tiles.txt`, printed/written counts
- CLI: `--n-patches 60000 --n-countries 3 --seed 42`

- [ ] **Step 1: Implement selection logic**

```python
# 1) read metadata.parquet
# 2) country counts → take top 3 countries
# 3) within those rows, stratified sample by country×split totaling n_patches
# 4) tile_id = "_".join(patch_id.split("_")[:-2])
# 5) write manifests + COUNTS.md with exact numbers
```

- [ ] **Step 2: Dry-run on metadata only**

Run: `python scripts/00_build_subset.py --metadata /path/to/metadata.parquet --n-patches 60000`  
Expected: `COUNTS.md` shows ≥3 countries, total = 60000 (or all available if fewer), split breakdown.

- [ ] **Step 3: Document data acquisition in README**

Priority order:
1. Course HDFS: copy only tiles listed in `tiles.txt` → `/bigearthnet/raw/`
2. Lab `tar.zst`: `tar --use-compress-program=unzstd -xf BigEarthNet-S2.tar.zst -T tiles_as_members.txt` (tile directories only)
3. Debug: 500 patches under `hdfs_mirror/.../raw/`

Do **not** instruct a personal full 59 GiB download as the default path.

---

### Task 3: Step 1 — Ingest + pack Parquet

**Files:**
- Create: `scripts/01_ingest_and_pack.py`

**Interfaces:**
- Consumes: `/bigearthnet/raw`, `data/subset/subset_metadata.parquet`
- Produces: `/bigearthnet/packed` (partitioned by `country`), `/bigearthnet/metadata/subset_metadata.parquet`, `/bigearthnet/results/step1_storage_stats.json`

- [ ] **Step 1: Implement packing**

```python
# Join metadata; for each patch build paths to 4 TIFFs
# Prefer Spark binaryFile / sc.binaryFiles per band then join on patch_id
# Schema: patch_id STRING, country STRING, split STRING, labels ARRAY<STRING>,
#         b02 BINARY, b03 BINARY, b04 BINARY, b08 BINARY
# Write parquet.partitionBy("country"); coalesce/repartition for fewer files
# Before/after: subprocess or FileSystem count + du; write JSON stats
```

- [ ] **Step 2: Debug on 500 patches**

Run with `--limit 500`.  
Expected: packed rows = successful reads; stats JSON has file_count_before ≫ file_count_after.

- [ ] **Step 3: Scale to full subset (≥50k)**

Run without limit. Log exact patch count per country. Fail if `< 50000`.

---

### Task 4: Shared feature kernel + Step 2 engineering

**Files:**
- Create: `scripts/lib/features_core.py`
- Create: `scripts/02_feature_engineering.py`

**Interfaces:**
- Consumes: packed Parquet
- Produces: `/bigearthnet/features` partitioned by country; `/bigearthnet/results/step2_validation.json`
- `features_core.compute_from_arrays(b02,b03,b04,b08) -> dict`
- `features_core.compute_from_bytes(...) -> dict | error_flag`

- [ ] **Step 1: Implement pure NumPy kernel**

```python
EPS = 1e-6
# reflectance = arr.astype(np.float32) / 1e4
# means, stds (ddof=0 to match Spark population std if documented)
# ndvi = mean((nir-red)/(nir+red+EPS)); ndwi = mean((green-nir)/(green+nir+EPS))
# brightness = mean([mu_b02, mu_b03, mu_b04, mu_b08])
```

- [ ] **Step 2: Spark `mapPartitions` decode + DataFrame schema**

```python
# For each row: MemoryFile(bytes).open() → read band; on exception error_flag=True, null features
# Explicit StructType; toDF; write features
# Benchmark: run with spark.sql.shuffle.partitions in {8, 32} (or 2 configs); record wall time
```

- [ ] **Step 3: Validation block**

- Input vs output counts  
- Null/NaN/Inf per column  
- `df.describe()`  
- Assert NDVI/NDWI within [-1, 1] for non-error rows (allow tiny float slack)  
- Sample 3 `patch_id`s; driver loads **only those 3** packed rows; NumPy recompute must match Spark within 1e-5  
- Save validation JSON + note Spark UI screenshot path for report  

- [ ] **Step 4: Run 500 then 60k**

Expected: output rows ≈ input; `error_flag` rate reported; counts in log.

---

### Task 5: Step 3 — Spark SQL analyses + charts

**Files:**
- Create: `scripts/03_spark_sql_analysis.py`

**Interfaces:**
- Consumes: features (+ labels)
- Produces: Parquet/CSV under `/bigearthnet/results/sql_*` and PNGs under `reports/figures/`

- [ ] **Step 1: Register temp views `patches`, `labels_exploded`**

- [ ] **Step 2: Implement five analyses as SQL strings**

1. Class distribution + labels_per_patch histogram data  
2. Counts by country; country×split  
3. Top-10 co-occurring pairs (`labels` self-explode + `c1 < c2`)  
4. Mean NDVI/brightness by country; mean features by class  
5. Custom: single-label rate by country OR NDVI by split  

Save each with `df.write.mode("overwrite").parquet(...)`.

- [ ] **Step 3: Plot ≥3 charts** (matplotlib/seaborn from **aggregated** result tables only — small collects OK)

Suggested: class bar chart; country×split stacked; NDVI by country box/bar.

- [ ] **Step 4: Write short interpretation bullets into `reports/figures/FINDINGS.md`** (class imbalance focus)

---

### Task 6: Step 4 — Classification (DataFrame/SQL)

**Files:**
- Create: `scripts/04_classification.py`

**Interfaces:**
- Consumes: features where `error_flag = false`
- Produces: `/bigearthnet/results/clf_*` (predictions, confusion, metrics JSON)

- [ ] **Step 1: Build single-label task**

```python
# On train split only: explode labels → frequency → pick top K in [3,5]
# Keep patches where size(filter(labels, x -> x in selected)) == 1
# target = that unique label; report exact counts train/val/test
```

- [ ] **Step 2: Majority + rule baselines**

```python
# majority_class = mode(target) on train
# Choose ≤3 features (e.g. ndvi, brightness, mean_b08)
# Thresholds from train approxQuantile / group means
# SQL CASE WHEN ... END AS pred_rule
```

- [ ] **Step 3: Nearest-centroid**

```python
# train mean/std per feature; standardize all splits with train stats only
# centroids = train.groupBy("target").agg(avg(...))
# broadcast(centroids); crossJoin; euclidean distance; Window.partitionBy(patch_id).orderBy(dist)
# pred = row_number == 1
```

- [ ] **Step 4: Evaluation in Spark**

Confusion via `groupBy("target").pivot("prediction").count()`; accuracy; per-class precision/recall/F1; macro-F1; compare majority / rule / centroid.

- [ ] **Step 5: Feature-set comparison**

Full ~11 features vs reduced ≤5 chosen on **validation** (class centroid separation score). Refit centroids on train only; report test macro-F1 both ways.

- [ ] **Step 6: Discussion stub** `reports/DISCUSSION_NOTES.md` — confused classes, feature importance proxy (|centroid gap|), limits.

---

### Task 7: README polish + report skeleton

**Files:**
- Modify: `README.md`
- Create: `reports/REPORT_OUTLINE.md` (maps to ≤8-page PDF sections)

- [ ] **Step 1: README complete runbook** — env (nodes/cores/mem placeholders), exact commands, how to capture `hdfs dfs -du/-count/-ls`, Spark UI screenshots checklist, AI citation.

- [ ] **Step 2: Report outline** — environment, pipeline diagram, storage table, feature validation, SQL+charts, classification tables, discussion, reproducibility.

- [ ] **Step 3: End-to-end smoke** — 500-patch local mirror through steps 1–4; confirm artifacts exist.

---

### Task 8: Verification gate (before claiming done)

- [ ] Counts: every step logs patch_n; final ≥50 000 and ≥3 countries  
- [ ] No driver raster loop in Step 2 code path  
- [ ] No random split; train-only stats verified by code review  
- [ ] Results under `/bigearthnet/results/`  
- [ ] ≥3 figures + FINDINGS  
- [ ] Both classifiers + majority baseline metrics present  

---

## Spec coverage checklist

| Brief requirement | Task |
|---|---|
| HDFS layout + packing stats | Task 3 |
| Executor features + validation + partition timing | Task 4 |
| ≥3 SQL analyses (we do 5) + charts | Task 5 |
| Rule + centroid + metrics + feature compare + discussion | Task 6 |
| Code + README + requirements + report | Tasks 1, 7 |
| Partial download / no full 59 GiB | Task 2 |
| ≥50k / ≥3 countries | Tasks 2–3, 8 |

## Execution handoff

Plan saved to `docs/superpowers/plans/2026-09-26-bigearthnet-spark-pipeline.md`.

**Two execution options:**

1. **Subagent-Driven (recommended)** — fresh subagent per task, review between tasks  
2. **Inline Execution** — execute tasks in this session with checkpoints  

Which approach?
