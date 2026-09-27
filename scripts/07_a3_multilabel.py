#!/usr/bin/env python3
"""Assignment 3 Step 3 — multi-label binary relevance with Spark MLlib."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from scripts.a3_config import ASSIGNMENT3_FEATURE_COLS, ASSIGNMENT3_LABELS_19, ASSIGNMENT3_SEED, ASSIGNMENT3_THRESHOLD_GRID
from scripts.a3_ml_utils import load_features_v2
from scripts.lib.io_stats import write_json
from scripts.lib.paths import features_v2_path, local_hdfs_mirror, results_path
from scripts.lib.spark_session import get_spark

prob1 = F.udf(lambda v: float(v[1]), DoubleType())


def add_binary_labels(df):
    out = df
    for i, lab in enumerate(ASSIGNMENT3_LABELS_19):
        out = out.withColumn(f"y_{i}", F.array_contains(F.col("labels"), lab).cast("double"))
    return out


def f1_counts(df, label_col: str, pred_col: str) -> dict:
    tp = df.filter((F.col(label_col) == 1.0) & (F.col(pred_col) == 1.0)).count()
    fp = df.filter((F.col(label_col) == 0.0) & (F.col(pred_col) == 1.0)).count()
    fn = df.filter((F.col(label_col) == 1.0) & (F.col(pred_col) == 0.0)).count()
    tn = df.filter((F.col(label_col) == 0.0) & (F.col(pred_col) == 0.0)).count()
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn, "precision": precision, "recall": recall, "f1": f1}


def micro_macro(per_class: list[dict]) -> dict:
    tp = sum(x["test"]["tp"] for x in per_class)
    fp = sum(x["test"]["fp"] for x in per_class)
    fn = sum(x["test"]["fn"] for x in per_class)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    micro = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    macro = sum(x["test"]["f1"] for x in per_class) / len(per_class) if per_class else 0.0
    return {"micro_f1": micro, "macro_f1": macro, "micro_precision": precision, "micro_recall": recall}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--features-uri", default=features_v2_path())
    args = ap.parse_args()

    spark = get_spark("BEN-A3-Step3-Multilabel")
    feats = add_binary_labels(load_features_v2(spark, args.features_uri)).cache()
    train = feats.filter(F.col("split") == "train").cache()
    val = feats.filter(F.col("split") == "validation").cache()
    test = feats.filter(F.col("split") == "test").cache()

    prep = Pipeline(stages=[
        VectorAssembler(inputCols=ASSIGNMENT3_FEATURE_COLS, outputCol="raw_features", handleInvalid="keep"),
        StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
    ]).fit(train)
    train_x = prep.transform(train).cache()
    val_x = prep.transform(val).cache()
    test_x = prep.transform(test).cache()

    per_class = []
    for i, lab in enumerate(ASSIGNMENT3_LABELS_19):
        y = f"y_{i}"
        pos = train_x.filter(F.col(y) == 1.0).count()
        neg = train_x.filter(F.col(y) == 0.0).count()
        if pos == 0:
            per_class.append({"class": lab, "train_positive": 0, "skipped": True, "best_threshold": None, "validation": {}, "test": {"tp": 0, "fp": 0, "fn": 0, "tn": test_x.count(), "precision": 0.0, "recall": 0.0, "f1": 0.0}})
            continue
        train_w = train_x.withColumn("weight", F.when(F.col(y) == 1.0, F.lit((pos + neg) / (2.0 * pos))).otherwise(F.lit((pos + neg) / (2.0 * neg))))
        lr = LogisticRegression(labelCol=y, featuresCol="features", weightCol="weight", maxIter=50, regParam=0.01, elasticNetParam=0.0, standardization=False)
        t0 = time.time()
        model = lr.fit(train_w)
        val_scored = model.transform(val_x).withColumn("score", prob1(F.col("probability"))).cache()
        test_scored = model.transform(test_x).withColumn("score", prob1(F.col("probability"))).cache()
        threshold_rows = []
        best_threshold = 0.5
        best_val = {"f1": -1.0}
        for threshold in ASSIGNMENT3_THRESHOLD_GRID:
            v = val_scored.withColumn("pred", (F.col("score") >= F.lit(threshold)).cast("double"))
            stats = f1_counts(v, y, "pred")
            threshold_rows.append({"threshold": threshold, **stats})
            if stats["f1"] > best_val["f1"]:
                best_threshold = threshold
                best_val = stats
        t = test_scored.withColumn("pred", (F.col("score") >= F.lit(best_threshold)).cast("double"))
        test_stats = f1_counts(t, y, "pred")
        per_class.append({
            "class": lab,
            "train_positive": pos,
            "train_negative": neg,
            "training_time_sec": round(time.time() - t0, 3),
            "best_threshold": best_threshold,
            "validation": best_val,
            "validation_threshold_grid": threshold_rows,
            "test": test_stats,
            "discussion_hint": "Low F1 usually means this class is rare or not separable from patch-level spectral statistics alone.",
        })

    report = {
        "step": "A3-Step3",
        "labels_19": ASSIGNMENT3_LABELS_19,
        "feature_preparation": "VectorAssembler -> StandardScaler fitted on train only, then reused for every binary classifier",
        "per_class": per_class,
        "summary": micro_macro(per_class),
    }
    write_json(Path("reports") / "a3_step3_multilabel.json", report)
    write_json(local_hdfs_mirror() / "results" / "a3_step3_multilabel.json", report)
    test_x.select("patch_id", "country", "split", "labels", *[f"y_{i}" for i in range(len(ASSIGNMENT3_LABELS_19))]).write.mode("overwrite").parquet(f"{results_path().rstrip('/')}/a3_multilabel_encoded")
    print(json.dumps(report, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()

