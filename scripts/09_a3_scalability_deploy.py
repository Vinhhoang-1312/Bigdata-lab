#!/usr/bin/env python3
"""Assignment 3 Step 5 — scaling, persistence, reload, and held-out-country inference."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import functions as F

from scripts.a3_config import (
    ASSIGNMENT3_FEATURE_COLS,
    ASSIGNMENT3_HELD_OUT_COUNTRY,
    ASSIGNMENT3_N_CLASSES,
    ASSIGNMENT3_PARTITION_COUNTS,
    ASSIGNMENT3_SCALE_FRACTIONS,
    ASSIGNMENT3_SEED,
)
from scripts.a3_ml_utils import (
    add_class_weights,
    build_single_label_task,
    evaluate_multiclass,
    load_features_v2,
    make_base_pipeline,
    selected_frequent_classes,
)
from scripts.lib.io_stats import write_json
from scripts.lib.paths import features_v2_path, local_hdfs_mirror, models_path, predictions_path
from scripts.lib.spark_session import get_spark


def _read_best_step2() -> str:
    path = Path("reports") / "a3_step2_multiclass.json"
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8")).get("best_model_name", "rf")
        except Exception:
            return "rf"
    return "rf"


def _classifier(name: str):
    if name == "lr":
        return LogisticRegression(labelCol="label", featuresCol="features", weightCol="weight", family="multinomial", maxIter=60, regParam=0.01, elasticNetParam=0.0, standardization=False)
    # RF is also used as a practical fallback if OVR-GBT was best, because Step 5 repeatedly trains at scale.
    return RandomForestClassifier(labelCol="label", featuresCol="features", weightCol="weight", seed=ASSIGNMENT3_SEED, numTrees=40, maxDepth=8)


def stratified_fraction_map(train):
    return {r["target"]: 1.0 for r in train.select("target").distinct().collect()}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--features-uri", default=features_v2_path())
    ap.add_argument("--held-out-country", default=ASSIGNMENT3_HELD_OUT_COUNTRY)
    args = ap.parse_args()

    spark = get_spark("BEN-A3-Step5-ScaleDeploy")
    feats = load_features_v2(spark, args.features_uri).cache()
    selected = selected_frequent_classes(feats.filter(F.col("country") != args.held_out_country), ASSIGNMENT3_N_CLASSES)
    task = build_single_label_task(feats, selected).cache()
    train_base = task.filter((F.col("split") == "train") & (F.col("country") != args.held_out_country)).cache()
    test_base = task.filter((F.col("split") == "test") & (F.col("country") != args.held_out_country)).cache()
    heldout = task.filter(F.col("country") == args.held_out_country).cache()

    best_name = _read_best_step2()
    practical_name = "lr" if best_name == "lr" else "rf"
    scaling_rows = []
    for fraction in ASSIGNMENT3_SCALE_FRACTIONS:
        fractions = {r["target"]: fraction for r in train_base.select("target").distinct().collect()}
        sampled = train_base.sampleBy("target", fractions, seed=ASSIGNMENT3_SEED).cache() if fraction < 1.0 else train_base.cache()
        for partitions in ASSIGNMENT3_PARTITION_COUNTS:
            train_part = sampled.repartition(partitions, "target").cache()
            weighted_train = add_class_weights(train_part, train_part).cache()
            t0 = time.time()
            model = make_base_pipeline(_classifier(practical_name)).fit(weighted_train)
            train_sec = time.time() - t0
            pred = model.transform(test_base).cache()
            metrics = evaluate_multiclass(pred)
            scaling_rows.append({
                "model_used": practical_name,
                "requested_best_from_step2": best_name,
                "train_fraction": fraction,
                "partitions": partitions,
                "train_rows": train_part.count(),
                "training_time_sec": round(train_sec, 3),
                "test_macro_f1": metrics["macro_f1"],
                "test_weighted_f1": metrics["weighted_f1"],
                "test_accuracy": metrics["accuracy"],
            })

    weighted_full = add_class_weights(train_base, train_base).cache()
    final_model = make_base_pipeline(_classifier(practical_name)).fit(weighted_full)
    model_uri = f"{models_path().rstrip('/')}/a3_best_pipeline_model"
    final_model.write().overwrite().save(model_uri)
    reloaded = PipelineModel.load(model_uri)
    before = evaluate_multiclass(final_model.transform(test_base).cache())
    after = evaluate_multiclass(reloaded.transform(test_base).cache())
    reproducible = abs(before["macro_f1"] - after["macro_f1"]) < 1e-12 and abs(before["accuracy"] - after["accuracy"]) < 1e-12

    held_pred = reloaded.transform(heldout).cache()
    pred_uri = predictions_path()
    held_pred.select("patch_id", "country", "split", "target", "label", "prediction", "probability").write.mode("overwrite").partitionBy("country").parquet(pred_uri)
    held_metrics = evaluate_multiclass(held_pred) if heldout.count() else {"n": 0, "accuracy": 0.0, "macro_f1": 0.0, "weighted_f1": 0.0, "per_class": [], "confusion_rows": []}

    report = {
        "step": "A3-Step5",
        "held_out_country": args.held_out_country,
        "selected_top_classes": selected,
        "scaling": {"uses_cache": True, "sampleBy_stratified_on_target": True, "rows": scaling_rows},
        "persistence": {"model_uri": model_uri, "reload_reproduces_test_metrics_exactly": reproducible, "before": before, "after": after},
        "batch_inference": {"predictions_uri": pred_uri, "held_out_country_metrics": held_metrics, "discussion": "Accuracy/F1 on a country excluded from training estimates domain shift across countries."},
    }
    write_json(Path("reports") / "a3_step5_scalability_deployment.json", report)
    write_json(local_hdfs_mirror() / "results" / "a3_step5_scalability_deployment.json", report)
    print(json.dumps(report, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()

