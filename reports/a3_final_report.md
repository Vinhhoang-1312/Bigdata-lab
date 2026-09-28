# BDA501 - Big Data Analytics

## Individual Assignment 3

**Land-Cover Classification at Scale with Apache Spark MLlib**

**Student:** Tran Phan Vinh Hoang

**Date:** 28 September 2026

## Abstract

This report processes 52,607 BigEarthNet-S2 patches from 4 countries with the official split. Random Forest reached test accuracy 0.958, weighted-F1 0.955, and macro-F1 0.511. Multi-label micro-F1 was 0.627. PCA, KMeans, scaling, persistence and held-out-country inference are also evaluated.

## 1. Environment and data

Local Spark/PySpark MLlib processed 52,607 patches. Split counts were 25,291 train, 13,884 validation and 13,432 test. No random split or random-fold tuning was used.

## 2. Richer features

Features v2 contains 28 columns with 0 errors, no null/NaN values and no range failures. New columns include six 20 m band means/std values, NDMI, NDBI and NDVI p10/p50/p90.

## 3. Multi-class classification

### Model selection and validation

The pipeline is StringIndexer -> VectorAssembler -> StandardScaler -> classifier with train-only inverse-frequency weights.

| Model | Best validation macro-F1 | Best configuration |
|---|---:|---|
| lr | 0.735 | {'regParam': 0.0, 'elasticNetParam': 0.0} |
| rf | 0.845 | {'numTrees': 40, 'maxDepth': 8} |
| gbt_ovr | 0.767 | {'maxIter': 10, 'maxDepth': 3} |

### Assignment 2 baselines

The selected RF test result was accuracy 0.958, weighted-F1 0.955, and macro-F1 0.511. A2 majority, spectral-rule and nearest-centroid baselines are compared in the PDF; they use a smaller three-class task.

## 4. Multi-label classification

Nineteen binary classifiers with validation-tuned thresholds reached micro-F1 0.627 and macro-F1 0.397.

## 5. Unsupervised analysis

PC1/PC2 explained 72.1% and 12.3%. KMeans selected k=3 with silhouette 0.789.

## 6. Scalability and deployment

The best model was trained on 25%, 50% and 100% samples with 4 and 8 partitions. The saved PipelineModel reproduced test metrics exactly.
Held-out Lithuania accuracy was 0.955; macro-F1 was 0.513.

## 7. Discussion and limitations

The partial data contains 18 of 19 labels and only 13 exact-one training classes. Patch statistics lose spatial texture. Results use local Spark and an HDFS mirror.

## 8. Conclusion

All five required MLlib stages are implemented with fixed seeds, official splits and reproducible JSON evidence.

**AI assistance statement.** OpenAI Codex helped implement, verify and present the project; all numerical results come from submitted artifacts.
