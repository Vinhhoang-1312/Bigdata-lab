# BDA501 Individual Assignment 2 Requirement Audit

This audit is based on `individual_assigmnent_2.pdf` and the verified project artifacts in this repository. Status values are strict: DONE means evidence exists, PARTIAL means the project has related evidence but not the full literal requirement, and MISSING means no real evidence was found.

| Requirement | Evidence | Status |
|-------------|----------|--------|
| Process at least 50,000 patches end-to-end | `reports/hdf5_manifest_summary.json`, `reports/hdf5_step2_validation.json`: 52,607 rows | DONE |
| Use at least 3 countries | Austria, Finland, Ireland, Lithuania | DONE |
| Use official train/validation/test split | Manifest and Step 4 counts preserve train/validation/test | DONE |
| Use B02, B03, B04, B08 10 m bands | `scripts/hdf5_features.py` selects HDF5 indices for B02/B03/B04/B08 | DONE |
| HDFS-style layout `/bigearthnet/{raw,packed,metadata,features,results}` | `hdfs_mirror/bigearthnet/{raw,packed,metadata,features,results}` exists locally | PARTIAL |
| Store images and metadata in real HDFS | `reports/hdfs_environment.json` says `hdfs` command is not available; current evidence is local mirror | MISSING |
| Small files packed into Parquet by country | `scripts/01_ingest_and_pack.py`, `hdfs_mirror/bigearthnet/packed`, `reports/step1_storage_stats.json` | PARTIAL |
| Full 52,607 original TIFF patches packed into Parquet | Not performed because original TIFF dataset was not downloaded; main source is HDF5 | MISSING |`n| Full 52,607 HDF5-derived image-band rows packed into Parquet | `reports/full_raw_band_pack_evidence.json`: 52,607 rows, B02/B03/B04/B08 raw uint16 bytes, 136 files, 6.05 GB on D: | DONE |
| Before/after file count and size | Pilot: 96 raw files / 2.66 MiB -> 8 packed files / 2.06 MiB | DONE for pilot |
| HDFS block size | `reports/hdfs_environment.json`: cannot obtain, no HDFS CLI/service available | MISSING |
| HDFS replication factor | `reports/hdfs_environment.json`: cannot obtain, no HDFS CLI/service available | MISSING |
| HDFS count/du/ls or NameNode UI evidence | Local mirror stats exist; real HDFS command evidence unavailable | PARTIAL |
| Step 2 one row per patch with explicit schema | `scripts/hdf5_features.py`, `FEATURE_SCHEMA`, Parquet output under features | DONE |
| Feature computation on executors | `mapPartitions` in `scripts/hdf5_features.py` | DONE |
| No driver-side image loop | HDF5 image rows are read inside partition function | DONE |
| Corrupt input yields error flag | `error_flag` and `error_message` in schema; validation has 0 errors | DONE |
| Input vs output row counts | Main validation: 52,607 in / 52,607 out | DONE |
| Null/NaN/Inf per feature | `reports/hdf5_feature_profile.json`: total null/NaN/Inf = 0 | DONE |
| `describe()` for all features | `reports/hdf5_feature_profile.json` includes count/mean/stddev/min/max | DONE |
| NDVI and NDWI in [-1, 1] | `reports/hdf5_step2_validation.json`: bad NDVI = 0, bad NDWI = 0 | DONE |
| NumPy recomputation for 3 patches | `reports/hdf5_step2_validation.json`: 3 checks, max diff 0.0 | DONE |
| Runtime for at least two partition counts | `reports/step2_partition_benchmark.json`: 4 and 8 partitions | DONE |
| Feature equivalence across partition benchmark outputs | `reports/step2_partition_benchmark.json`: max abs diff overall 0.0 | DONE |
| Spark UI evidence image | `reports/figures/spark_ui_step2.png` generated from real Spark statusTracker job/stage/task evidence; not a browser screenshot | DONE for evidence panel / PARTIAL for literal browser screenshot |
| Spark SQL class distribution and labels per patch | `sql_class_distribution`, `sql_labels_per_patch` | DONE |
| Spark SQL patches per country and country x split | `sql_patches_by_country`, `sql_patches_by_country_split` | DONE |
| Spark SQL top-10 co-occurring pairs | `sql_top10_cooccurring_pairs` | DONE |
| Spark SQL mean NDVI/brightness by country and features by class | `sql_mean_features_by_country`, `sql_mean_features_by_class` | DONE |
| One own SQL question | `sql_custom_single_label_rate_by_country`, `sql_custom_ndvi_by_split` | DONE |
| Save SQL results to results path | `hdfs_mirror/bigearthnet/results/sql_*` | DONE |
| At least three charts | `reports/figures/01_class_distribution.png`, `02_country_split.png`, `03_ndvi_by_country.png` | DONE |
| Step 4 picks 3-5 frequent classes | Pastures, Arable land, Marine waters | DONE |
| Keep patches containing exactly one selected target | `scripts/04_classification.py`, `reports/step4_metrics.json` task counts | DONE |
| Use official split, no random split | Step 4 uses train/validation/test from data | DONE |
| Train-only statistics for classifiers | `reports/step4_metrics.json` stores train thresholds and train standardisation | DONE |
| Rule baseline with <=3 features | Training-derived spectral rule | DONE |
| Nearest centroid classifier | Full and reduced centroid metrics exist | DONE |
| Evaluation in Spark | Result folders under `hdfs_mirror/bigearthnet/results/clf_*` and script uses Spark DataFrames | DONE |
| Confusion matrix | `clf_confusion_majority`, `clf_confusion_rule`, `clf_confusion_centroid`, `clf_confusion_centroid_reduced` | DONE |
| Accuracy, precision, recall, F1, macro-F1 | `reports/step4_metrics.json` | DONE |
| Majority-class baseline comparison | Majority baseline included | DONE |
| Full feature set vs reduced 3-5 features | 11-feature centroid vs 5-feature centroid | DONE |
| Discussion of confused classes, important features, limits | `reports/final_report.md`, `reports/DISCUSSION_NOTES.md` | DONE |
| Code scripts per step | `scripts/00_*.py` through `04_*.py`, HDF5 helper scripts | DONE |
| README with run commands | `README.md` | DONE |
| `requirements.txt` | Present | DONE |
| Report 4-8 pages PDF | `reports/final_report.pdf`: regenerated from verified evidence, 8 pages | DONE |
| AI acknowledgement | `reports/final_report.md` includes AI acknowledgement | DONE |

## Final verdict

SUBSTANTIALLY COMPLETE WITH EXPLICIT DEVIATION.

The core HDF5-direct pipeline is complete and verified for 52,607 patches. Additional Step 1 evidence now includes full 52,607-row HDF5-derived raw band Parquet packing, but the original downloaded TIFF files were still not packed because the original TIFF dataset was not downloaded. Real HDFS block size and replication are also missing because this environment exposes local filesystem/mirror evidence, not a running HDFS CLI/service.


