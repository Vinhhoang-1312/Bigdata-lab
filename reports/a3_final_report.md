# BDA501 Individual Assignment 3 Report

Land-Cover Classification at Scale with Spark MLlib.

## 1. Environment and data

- Platform: Windows local Spark / PySpark MLlib, local HDFS mirror `hdfs_mirror/bigearthnet`.
- Dataset source: BigEarthNet HDF5 partial data from Assignment 2.
- Rows used: 52,607 patches across 4 countries.
- Official split counts: {'train': 25291, 'validation': 13884, 'test': 13432}.
- No random split or random-fold cross-validation was used; train/validation/test columns are preserved.
- AI assistance: OpenAI Codex helped implement and verify scripts; all code/results are reproducible from this repo.

## 2. Step 1: Richer features

Features v2 saved to `file:///C:/Users/DELL/Documents/Bigdata-Lab/Lab2-Bigdata/hdfs_mirror/bigearthnet/features_v2`.
Final feature count: 28.
The new 20 m bands B05, B06, B07, B8A, B11 and B12 are processed on their own 60 x 60 grid. NDMI, NDBI, and NDVI p10/p50/p90 are computed per patch.
Validation: 0 feature errors; null counts and NaN counts are all zero; index range failures: {'ndvi': 0, 'ndwi': 0, 'ndmi': 0, 'ndbi': 0, 'ndvi_p10': 0, 'ndvi_p50': 0, 'ndvi_p90': 0}.

## 3. Step 2: Multi-class classification

Selected frequent classes from train: 18 available classes in this subset.
The pipeline for every model is `StringIndexer -> VectorAssembler -> StandardScaler -> classifier`, with inverse-frequency `weightCol` from train only.
Validation tuning results:

| Model | Best config | Validation macro-F1 | Train time (s) |
|---|---:|---:|---:|
| lr | `{'regParam': 0.0, 'elasticNetParam': 0.0}` | 0.735 | 20.537 |
| rf | `{'numTrees': 40, 'maxDepth': 8}` | 0.845 | 5.544 |
| gbt_ovr | `{'maxIter': 10, 'maxDepth': 3}` | 0.767 | 83.295 |

Best model: `rf`. Test accuracy=0.958, weighted-F1=0.955, macro-F1=0.511.
Assignment 2 baselines are included in `reports/a3_step2_multiclass.json` for comparison.
Top RF features include: ndvi_p10, mean_b11, mean_b12, brightness, mean_b03, ndwi, mean_b08, mean_b8a, ndvi_p90, mean_b06.
Top-k retraining summary:

| k | Test accuracy | Macro-F1 | Weighted-F1 |
|---:|---:|---:|---:|
| 5 | 0.793 | 0.439 | 0.861 |
| 10 | 0.815 | 0.443 | 0.881 |

## 4. Step 3: Multi-label classification

All 19 CORINE labels are encoded as binary columns. One Logistic Regression binary classifier is trained per class using one train-fitted feature-preparation PipelineModel. Thresholds are selected on validation only.
Test micro-F1=0.627; macro-F1=0.397.
Marine waters and Pastures are strongest; rare or visually mixed classes such as Coastal wetlands, Beaches/dunes/sands, and Industrial/commercial units are weak from patch-level summaries.

## 5. Step 4: Unsupervised analysis

PCA explained variance for PC1/PC2: [0.7208, 0.1228].
PCA plot: `C:\Users\DELL\Documents\Bigdata-Lab\Lab2-Bigdata\reports\figures\a3_pca_scatter.png`.
Best KMeans k by silhouette: 3 with silhouette=0.789.
The contingency table shows one cluster dominated by Marine waters and others mostly vegetation/agriculture; clusters do not perfectly match land-cover classes because labels are multi-label and features lose spatial texture.

## 6. Step 5: Scalability and deployment

Scaling experiments used stratified `sampleBy`, two partition counts, and caching. Plot: `reports/figures/a3_scalability.png`.

| Train fraction | Partitions | Train rows | Time (s) | Test macro-F1 |
|---:|---:|---:|---:|---:|
| 0.25 | 4 | 1567 | 8.763 | 0.507 |
| 0.25 | 8 | 1567 | 5.01 | 0.515 |
| 0.5 | 4 | 3240 | 5.793 | 0.547 |
| 0.5 | 8 | 3240 | 5.261 | 0.538 |
| 1.0 | 4 | 6567 | 4.139 | 0.626 |
| 1.0 | 8 | 6567 | 5.42 | 0.585 |

Model saved to `file:///C:/Users/DELL/Documents/Bigdata-Lab/Lab2-Bigdata/hdfs_mirror/bigearthnet/models/a3_best_pipeline_model` and reloaded metrics matched exactly: `True`.
Batch predictions saved to `file:///C:/Users/DELL/Documents/Bigdata-Lab/Lab2-Bigdata/hdfs_mirror/bigearthnet/predictions` partitioned by country.
Held-out country `Lithuania` accuracy=0.955, macro-F1=0.513.

## 7. Reproducibility commands

```powershell
.\.venv\Scripts\python.exe scripts\05_a3_features_v2.py --partitions 16
.\.venv\Scripts\python.exe scripts\06_a3_multiclass.py
.\.venv\Scripts\python.exe scripts\07_a3_multilabel.py
.\.venv\Scripts\python.exe scripts\08_a3_unsupervised.py
.\.venv\Scripts\python.exe scripts\09_a3_scalability_deploy.py
```

## 8. Limitation / deviation notes

- The provided partial dataset contains 18 of 19 class labels and the single-label top-class task has fewer than 35 usable exact-one frequent classes. The scripts request 35 but transparently report the available classes/counts.
- Windows local Spark required local Hadoop native helpers (`winutils.exe`, `hadoop.dll`) for Parquet writes.

