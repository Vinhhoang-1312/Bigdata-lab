# BDA501 Individual Assignment 2 Final Report Notes

## Distributed BigEarthNet Processing with Apache Spark

Student: Tran Phan Vinh Hoang  
Project path: `C:\Users\DELL\Documents\Bigdata-Lab\Lab2-Bigdata`  
Report date: 27 September 2026

## 1. Introduction and dataset scope

This project implements an Apache Spark pipeline for BigEarthNet-S2 land-cover analysis. The assignment asks for at least 50,000 patches from at least three countries. The submitted main experiment uses four HDF5 shards from `D:\BigEarthNet-partial`, not the full BigEarthNet download.

The main run processes 52,607 patches from four countries: Austria, Finland, Ireland and Lithuania. The official train, validation and test split is preserved.

| Item | Value |
|---|---:|
| Total patches | 52,607 |
| Countries | 4 |
| Train rows | 25,291 |
| Validation rows | 13,884 |
| Test rows | 13,432 |
| Observed labels | 18 |

The four selected shards are `bigearthnet_train_p0.hdf5`, `bigearthnet_train_p1.hdf5`, `bigearthnet_val_p0.hdf5`, and `bigearthnet_test_p0.hdf5`. This keeps the workload large enough for the assignment while avoiding the full dataset download.

## 2. Environment

The verified run uses local Spark on Windows through PySpark. The project writes outputs into a local `hdfs_mirror/bigearthnet` directory. This mirror follows the required BigEarthNet folder structure, but it is not proof of a running HDFS cluster.

`reports/hdfs_environment.json` records that the `hdfs` and `hadoop` commands were not available in this environment. Therefore real HDFS block size, replication factor and live DataNode count could not be obtained. These values were not invented.

| Environment item | Verified value |
|---|---|
| Main execution mode | Local Spark / PySpark |
| Real HDFS CLI available | No |
| HDFS block size | Not available |
| HDFS replication factor | Not available |
| Local mirror root | `hdfs_mirror/bigearthnet` |
| Spark UI during benchmark | `http://127.0.0.1:4040` |

## 3. Step 1 storage and ingestion

The assignment asks for storing metadata and images in HDFS and packing small TIFF band files into Parquet. The project now has three storage evidence paths:

1. The main path is HDF5-direct. It reads the selected HDF5 shards directly on Spark executors.
2. A smaller TIFF-to-Parquet pilot demonstrates the small-files packing mechanism.
3. A full 52,607-row HDF5-derived raw band-byte Parquet pack stores B02/B03/B04/B08 arrays as binary columns partitioned by country-like folders on D:.

The pilot result is real and measured:

| Storage stage | Patches | Files | Size |
|---|---:|---:|---:|
| Raw TIFF pilot | 24 | 96 | 2.66 MiB |
| Packed Parquet pilot | 24 | 8 | 2.06 MiB |

This pilot reduced file count from 96 to 8 and reduced size from 2.66 MiB to 2.06 MiB. The packing step took 26.147 seconds.

Additional full packing evidence: `reports/full_raw_band_pack_evidence.json` records 52,607 packed rows, 136 output files and 6.05 GB under `D:\BigEarthNet-partial\packed_raw_band_parquet_full`. This is full image-band packing from HDF5-derived raw uint16 bytes, not original downloaded TIFF-file packing.

Important limitation: the full 52,607-patch main run was not a full original-TIFF-to-Parquet packing run. It was HDF5-direct plus HDF5-derived raw band packing evidence. The final submission should state this clearly.

## 4. Step 2 distributed feature engineering

The main feature job is implemented in `scripts/hdf5_features.py`. It uses Spark `mapPartitions`. Each executor opens an HDF5 shard, reads the indexed image row, selects B02, B03, B04 and B08, and returns one typed row per patch.

The feature row contains 11 features:

- mean and standard deviation for B02, B03, B04 and B08
- NDVI
- NDWI
- brightness

The output schema also includes `error_flag` and `error_message`, so a corrupt row can be recorded without crashing the whole job.

## 5. Validation and partition performance

Main validation evidence:

| Validation item | Result |
|---|---:|
| Input rows | 52,607 |
| Output rows | 52,607 |
| Error rows | 0 |
| Bad NDVI rows | 0 |
| Bad NDWI rows | 0 |
| NumPy checks | 3 / 3 exact |
| Main elapsed time from original run | 76.136 s |

`reports/hdf5_feature_profile.json` adds a full feature profile for the main output:

| Profile item | Result |
|---|---:|
| Rows profiled | 52,607 |
| Feature columns | 11 |
| Total null / NaN / Inf values | 0 |
| Spark describe rows | count, mean, stddev, min, max |

A new benchmark tested the same 52,607-patch HDF5-direct feature job with two partition counts. Outputs were written under `data/benchmark`, not over the production feature table.

| Partitions | Input rows | Output rows | Errors | Runtime |
|---:|---:|---:|---:|---:|
| 4 | 52,607 | 52,607 | 0 | 60.545 s |
| 8 | 52,607 | 52,607 | 0 | 49.085 s |

The 8-partition run was faster on this machine by 18.928 percent relative to the 4-partition run. The feature outputs were equivalent: the joined row count was 52,607 and the maximum absolute difference across all 11 features was 0.0. This is a measured local result, not a claim of linear scalability.

Spark UI evidence status: `reports/figures/spark_ui_step2.png` is now generated from real Spark `statusTracker` job/stage/task evidence. It is an evidence panel, not a literal browser screenshot. The raw job/stage/task data remains in `reports/step2_partition_benchmark.json`.

## 6. Step 3 Spark SQL analysis

The SQL stage writes results under `hdfs_mirror/bigearthnet/results`. It reports 52,607 patches, four countries and 18 observed labels.

The project contains 10 SQL outputs:

| SQL output | Purpose |
|---|---|
| `sql_class_distribution` | exploded class distribution |
| `sql_class_distribution_by_split` | class distribution by official split |
| `sql_labels_per_patch` | label count per patch |
| `sql_patches_by_country` | patch count by country |
| `sql_patches_by_country_split` | country x split counts |
| `sql_top10_cooccurring_pairs` | top co-occurring label pairs |
| `sql_mean_features_by_country` | mean spectral features by country |
| `sql_mean_features_by_class` | mean spectral features by class |
| `sql_custom_single_label_rate_by_country` | own question: single-label rate |
| `sql_custom_ndvi_by_split` | own question: NDVI and brightness by split |

Three charts are available in `reports/figures`:

- `01_class_distribution.png`
- `02_country_split.png`
- `03_ndvi_by_country.png`

The class distribution is long-tailed. BigEarthNet is multi-label, so exploded label counts do not sum to the number of patches. This is why macro-F1 is important later in classification.

## 7. Step 4 classification

The classification task uses three target classes: Pastures, Arable land and Marine waters. The official split is used; no random split is introduced.

| Split | Rows |
|---|---:|
| Train | 16,835 |
| Validation | 7,551 |
| Test | 6,371 |

The evaluated methods are:

- majority-class baseline
- training-derived rule baseline
- nearest centroid with all 11 features
- nearest centroid with 5 reduced features

Test metrics:

| Model | Accuracy | Macro-F1 |
|---|---:|---:|
| Majority baseline | 0.554 | 0.238 |
| Training-derived rule | 0.817 | 0.858 |
| Centroid full, 11 features | 0.800 | 0.837 |
| Centroid reduced, 5 features | 0.809 | 0.853 |

The reduced feature set is `ndwi`, `mean_b08`, `ndvi`, `brightness`, and `std_b08`.

Marine waters is easiest because its spectral statistics are far from vegetation classes. The main confusion is between Pastures and Arable land. The reduced centroid improves macro-F1 over the full centroid, likely because it removes weaker or redundant dimensions.

## 8. Discussion and limitations

The strongest part of the project is the HDF5-direct Spark pipeline. It processes more than 50,000 patches, computes features on executors, validates the output, runs SQL analyses, creates charts and evaluates Spark-based classifiers.

The main limitation is Step 1 storage compliance. The assignment describes full-scale packing of original TIFF files in HDFS. This project demonstrates TIFF-to-Parquet packing as a pilot and adds full 52,607-row HDF5-derived raw band-byte Parquet packing. However, it still does not prove that the original BigEarthNet TIFF files from the tarball were downloaded and packed.

The second limitation is infrastructure evidence. A real HDFS service was not available from this environment, so block size, replication factor and live DataNode count are missing. A Spark UI screenshot was also not captured, although Spark job/stage/task evidence was recorded from the benchmark run.

## 9. Reproducibility and AI acknowledgement

Key evidence files:

| File | Purpose |
|---|---|
| `reports/hdf5_manifest_summary.json` | dataset size and split counts |
| `reports/hdf5_step2_validation.json` | main feature validation |
| `reports/hdf5_feature_profile.json` | null/NaN/Inf and describe() profile |
| `reports/step2_partition_benchmark.json` | 4 vs 8 partition benchmark |
| `reports/spark_ui_evidence.json` | Spark UI/status evidence and screenshot status |
| `reports/figures/spark_ui_step2.png` | rendered Spark job/stage/task evidence panel |
| `reports/full_raw_band_pack_evidence.json` | full 52,607-row HDF5-derived raw band Parquet packing |
| `reports/hdfs_environment.json` | HDFS availability and local mirror evidence |
| `reports/step3_summary.json` | SQL summary |
| `reports/step4_metrics.json` | classification metrics |
| `reports/REQUIREMENT_AUDIT.md` | strict requirement checklist |

Generative AI was used to help plan, organise, code and polish the project. The counts, timings, validation checks and metrics in this report come from local project artifacts and command outputs.

## Final verdict

SUBSTANTIALLY COMPLETE WITH EXPLICIT DEVIATION.

The project is not fully literal-compliant because real HDFS block/replication evidence is unavailable and the original downloaded TIFF files were not packed. However, the project now includes full 52,607-row HDF5-derived image-band Parquet packing, plus the core Spark data-processing, SQL and classification evidence.


