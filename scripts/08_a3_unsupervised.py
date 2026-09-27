#!/usr/bin/env python3
"""Assignment 3 Step 4 — PCA and KMeans unsupervised analysis."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import matplotlib.pyplot as plt
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import PCA, StandardScaler, VectorAssembler
from pyspark.sql import functions as F

from scripts.a3_config import ASSIGNMENT3_FEATURE_COLS, ASSIGNMENT3_KMEANS_KS, ASSIGNMENT3_N_CLASSES, ASSIGNMENT3_PCA_K, ASSIGNMENT3_SEED
from scripts.a3_ml_utils import build_single_label_task, load_features_v2, selected_frequent_classes
from scripts.lib.io_stats import write_json
from scripts.lib.paths import features_v2_path, figures_dir, local_hdfs_mirror, results_path
from scripts.lib.spark_session import get_spark


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--features-uri", default=features_v2_path())
    args = ap.parse_args()

    spark = get_spark("BEN-A3-Step4-Unsupervised")
    feats = load_features_v2(spark, args.features_uri).cache()
    selected = selected_frequent_classes(feats, ASSIGNMENT3_N_CLASSES)
    task = build_single_label_task(feats, selected).cache()
    train = task.filter(F.col("split") == "train").cache()
    all_df = task.cache()

    prep_pca = Pipeline(stages=[
        VectorAssembler(inputCols=ASSIGNMENT3_FEATURE_COLS, outputCol="raw_features", handleInvalid="keep"),
        StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
        PCA(k=ASSIGNMENT3_PCA_K, inputCol="features", outputCol="pca"),
    ]).fit(train)
    pca_model = prep_pca.stages[-1]
    pca_df = prep_pca.transform(all_df).select("patch_id", "split", "target", "pca")
    pca_rows = pca_df.sample(False, 0.20, seed=ASSIGNMENT3_SEED).limit(5000).collect()
    xs = [float(r["pca"][0]) for r in pca_rows]
    ys = [float(r["pca"][1]) for r in pca_rows]
    labels = [r["target"] for r in pca_rows]
    label_to_idx = {lab: i for i, lab in enumerate(sorted(set(labels)))}
    colors = [label_to_idx[x] for x in labels]
    fig, ax = plt.subplots(figsize=(8, 6))
    scatter = ax.scatter(xs, ys, c=colors, s=8, cmap="tab20", alpha=0.65)
    ax.set_title("Assignment 3 PCA: first two components coloured by class")
    ax.set_xlabel("PC1")
    ax.set_ylabel("PC2")
    pca_png = figures_dir() / "a3_pca_scatter.png"
    fig.tight_layout()
    fig.savefig(pca_png, dpi=160)
    plt.close(fig)

    prep = Pipeline(stages=[
        VectorAssembler(inputCols=ASSIGNMENT3_FEATURE_COLS, outputCol="raw_features", handleInvalid="keep"),
        StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
    ]).fit(train)
    train_x = prep.transform(train).cache()
    all_x = prep.transform(all_df).cache()
    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="prediction", metricName="silhouette", distanceMeasure="squaredEuclidean")
    k_rows = []
    best = None
    best_model = None
    for k in ASSIGNMENT3_KMEANS_KS:
        model = KMeans(k=k, seed=ASSIGNMENT3_SEED, featuresCol="features", predictionCol="prediction").fit(train_x)
        pred = model.transform(all_x).cache()
        score = evaluator.evaluate(pred)
        rec = {"k": k, "silhouette": score}
        k_rows.append(rec)
        if best is None or score > best["silhouette"]:
            best = rec
            best_model = model
    best_pred = best_model.transform(all_x).cache()
    contingency = [r.asDict() for r in best_pred.groupBy("prediction").pivot("target").count().fillna(0).orderBy("prediction").collect()]
    best_pred.select("patch_id", "country", "split", "target", "prediction").write.mode("overwrite").parquet(f"{results_path().rstrip('/')}/a3_kmeans_best")

    report = {
        "step": "A3-Step4",
        "pca": {"explained_variance": [float(x) for x in pca_model.explainedVariance], "plot": str(pca_png)},
        "kmeans": {"k_grid": k_rows, "best_k": best, "cluster_class_contingency": contingency},
        "discussion": "Clusters are expected to partially match broad vegetation/water/built-up gradients, not exact land-cover classes, because patch-level spectral summaries lose spatial texture and many labels co-occur.",
    }
    write_json(Path("reports") / "a3_step4_unsupervised.json", report)
    write_json(local_hdfs_mirror() / "results" / "a3_step4_unsupervised.json", report)
    print(json.dumps(report, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()

