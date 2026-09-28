# BDA501 Lab2 — Distributed BigEarthNet-S2 v2.0 with Apache Spark

End-to-end pipeline: **pack small files → executor features → Spark SQL → DataFrame/SQL classifiers**.

## Partial HDF5 run (assignment path)

The reproducible assignment run uses only four HDF5 shards from the Hugging Face
mirror on `D:\\BigEarthNet-partial`: `train_p0`, `train_p1`, `val_p0`, and
`test_p0` (about 17 GB compressed). The full repository is never downloaded.
`scripts/hdf5_manifest.py` joins the CSV row index to official V2 metadata, and
`scripts/hdf5_features.py` reads arrays directly on Spark executors with
`mapPartitions` (B02/B03/B04/B08, eleven features, explicit schema and error
flag). TIFF packing remains only a small-files pilot.

Deep dataset notes: [`docs/DATASET_AZ.md`](docs/DATASET_AZ.md)  
Design: [`docs/superpowers/specs/2026-09-26-bigearthnet-spark-pipeline-design.md`](docs/superpowers/specs/2026-09-26-bigearthnet-spark-pipeline-design.md)

## Drop-in data (when download finishes)

Put files here:

```text
data/incoming/
  metadata.parquet
  BigEarthNet-S2/
    <TILE>/<PATCH_ID>/<PATCH_ID>_B02.tif
    ...
```

Also accepted: `BEN_DATA_ROOT=/path/to/folder` with the same layout.

**Do not need the full 59 GiB.** Prefer:

1. Download `metadata.parquet` first  
2. `python scripts/00_build_subset.py` → read `data/subset/tiles.txt`  
3. Extract / copy **only those tiles** into `data/incoming/BigEarthNet-S2/`

## Quick start (laptop / local Spark)

For the selected HDF5 shards, run (PowerShell):

```powershell
$env:PYTHONPATH = (Get-Location).Path
python scripts/hdf5_manifest.py
python scripts/decompress_hdf5.py D:\BigEarthNet-partial\hdf5
python scripts/hdf5_features.py --manifest D:\BigEarthNet-partial\manifest\manifest.parquet
python scripts/03_spark_sql_analysis.py
python scripts/04_classification.py --n-classes 3
python scripts/write_final_report.py
```

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
pip install -r requirements.txt

# Optional: smoke test WITHOUT real dataset
python scripts/make_synthetic_fixture.py
python scripts/run_pipeline.py --n-patches 24 --limit 24 --skip-benchmark --n-classes 3

# Real subset (after drop-in)
set BEN_USE_LOCAL=1
python scripts/00_build_subset.py --n-patches 60000
python scripts/01_ingest_and_pack.py --limit 500
python scripts/02_feature_engineering.py --skip-benchmark
# then scale without --limit / full partition benchmark
python scripts/run_pipeline.py --n-patches 60000 --skip-benchmark
```

Or one-shot: `python scripts/run_pipeline.py --n-patches 60000`

## Cluster / HDFS

```bash
set BEN_USE_LOCAL=0
set BEN_HDFS_ROOT=/bigearthnet
set SPARK_MASTER=yarn
# stage raw tiles to /bigearthnet/raw then:
python scripts/01_ingest_and_pack.py --skip-stage-raw
```

HDFS layout produced:

```text
/bigearthnet/raw|packed|metadata|features|results/
```

Evidence commands (put outputs in the report):

```bash
hdfs dfs -count /bigearthnet/raw /bigearthnet/packed
hdfs dfs -du -s -h /bigearthnet/raw /bigearthnet/packed
hdfs getconf -confKey dfs.blocksize
hdfs getconf -confKey dfs.replication
```

## Scripts

| Script | Step |
|---|---|
| `00_build_subset.py` | Top-3 countries × N patches + manifests |
| `01_ingest_and_pack.py` | Pack B02/B03/B04/B08 → Parquet by country |
| `02_feature_engineering.py` | `mapPartitions` + validation + partition timing |
| `03_spark_sql_analysis.py` | ≥5 SQL analyses + charts |
| `04_classification.py` | Majority / rule / nearest-centroid (no leakage) |

## Environment variables

| Var | Default | Meaning |
|---|---|---|
| `BEN_USE_LOCAL` | `1` | Write under `hdfs_mirror/bigearthnet/` |
| `BEN_DATA_ROOT` | — | Override drop-in root |
| `BEN_HDFS_ROOT` | `/bigearthnet` | Cluster root |
| `BEN_SHUFFLE_PARTITIONS` | `32` | Spark shuffle partitions |
| `SPARK_MASTER` | `local[*]` if local | e.g. `yarn` |

## Grading safeguards built in

- Features on **executors** (`mapPartitions`), corrupt → `error_flag`  
- Official **`split`** only; train-only means/std/thresholds/centroids  
- Target = **3–5** frequent classes (dataset has 19 total; brief “35” treated as OCR for 3–5)  
- Exact counts logged under `reports/` and `data/subset/COUNTS.md`

## AI assistance (cite in PDF)

Planning and scaffolding assisted by **Cursor / Composer**. You must be able to explain every script orally.

## Report checklist

- Nodes / cores / memory  
- Storage before/after packing  
- Feature validation + NumPy match + Spark UI screenshots  
- ≥3 charts + class imbalance commentary  
- Confusion matrices, macro-F1 vs majority baseline  
- Discussion of confused classes / limits

## Evidence and audit helpers

After the main HDF5 run, the remaining assignment evidence can be produced with:

```powershell
$env:PYTHONPATH = (Get-Location).Path
python scripts/hdf5_partition_benchmark.py --partitions 4 8
```

This writes `reports/step2_partition_benchmark.json` and `reports/spark_ui_evidence.json` without overwriting the production feature output. The strict requirement checklist is in `reports/REQUIREMENT_AUDIT.md`.

## Assignment 3 - Spark MLlib Land-Cover Classification

This branch contains the completed **BDA501 Individual Assignment 3** pipeline. It continues from Assignment 2 and uses the official `split` column only. No `randomSplit`, `CrossValidator` default folds, or `TrainValidationSplit` default random folds are used.

### Assignment 3 run commands

PowerShell, from the repo root:

```powershell
.\.venv\Scripts\python.exe scripts\05_a3_features_v2.py --partitions 16
.\.venv\Scripts\python.exe scripts\06_a3_multiclass.py
.\.venv\Scripts\python.exe scripts\07_a3_multilabel.py
.\.venv\Scripts\python.exe scripts\08_a3_unsupervised.py
.\.venv\Scripts\python.exe scripts\09_a3_scalability_deploy.py
.\.venv\Scripts\python.exe scripts\write_a3_report.py
.\.venv\Scripts\python.exe scripts\plot_a3_scalability.py
.\.venv\Scripts\python.exe scripts\render_a3_report_pdf.py
```

### Assignment 3 outputs

| Output | Purpose |
|---|---|
| `hdfs_mirror/bigearthnet/features_v2/` | richer Step 1 feature table |
| `hdfs_mirror/bigearthnet/models/a3_best_pipeline_model/` | saved best PipelineModel |
| `hdfs_mirror/bigearthnet/predictions/` | held-out-country batch predictions partitioned by country |
| `reports/a3_final_report.pdf` | final 5-page PDF report for submission |
| `reports/a3_final_report.md` | editable report source |
| `reports/a3_requirement_audit.md` | requirement-by-requirement checkpoint |
| `reports/a3_step*.json` | raw evidence for each step |
| `reports/figures/a3_pca_scatter.png` | PCA plot |
| `reports/figures/a3_scalability.png` | scaling plot |

### Assignment 3 reproducibility notes

- Fixed random seed is configured in `scripts/a3_config.py`.
- Shared Spark ML helpers are in `scripts/a3_ml_utils.py`.
- Local Windows Spark uses `.hadoop/bin/winutils.exe` and `.hadoop/bin/hadoop.dll` for local Parquet/model writes.
- The partial dataset contains 52,607 patches across 4 countries. It has 18 of 19 labels available, so the scripts request 35 frequent classes but transparently report the usable classes/counts in the JSON evidence and final report.

### Assignment 3 submission

Submit:

1. Code ZIP downloaded from GitHub branch `assignment-3-work`.
2. `reports/a3_final_report.pdf`.
3. Optional supporting evidence: `reports/a3_requirement_audit.md`, `reports/a3_step*.json`, and the two `reports/figures/a3_*.png` images.
