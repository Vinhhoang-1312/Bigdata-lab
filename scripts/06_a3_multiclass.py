#!/usr/bin/env python3
"""Assignment 3 Step 2 — leakage-free multi-class Spark MLlib pipelines."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.sql import functions as F

from scripts.a3_config import (
    ASSIGNMENT3_FEATURE_COLS,
    ASSIGNMENT3_GBT_GRID,
    ASSIGNMENT3_LR_GRID,
    ASSIGNMENT3_N_CLASSES,
    ASSIGNMENT3_RF_GRID,
    ASSIGNMENT3_SEED,
    ASSIGNMENT3_TOP_K,
)
from scripts.a3_ml_utils import (
    add_class_weights,
    build_single_label_task,
    evaluate_multiclass,
    grid_for_model,
    label_index_mapping,
    load_features_v2,
    make_base_pipeline,
    make_classifiers,
    selected_frequent_classes,
    split_frames,
    tune_on_validation,
)
from scripts.lib.io_stats import write_json
from scripts.lib.paths import features_v2_path, local_hdfs_mirror, results_path
from scripts.lib.spark_session import get_spark


def _safe_read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _feature_importance(model) -> list[dict]:
    last = model.stages[-1]
    if hasattr(last, "featureImportances"):
        vals = last.featureImportances.toArray().tolist()
        return sorted([{"feature": c, "importance": float(v)} for c, v in zip(ASSIGNMENT3_FEATURE_COLS, vals)], key=lambda x: x["importance"], reverse=True)
    return []


def _lr_coefficients(model) -> list[dict]:
    last = model.stages[-1]
    if hasattr(last, "coefficientMatrix"):
        arr = last.coefficientMatrix.toArray()
        per_feature = abs(arr).mean(axis=0).tolist()
        return sorted([{"feature": c, "mean_abs_coef": float(v)} for c, v in zip(ASSIGNMENT3_FEATURE_COLS, per_feature)], key=lambda x: x["mean_abs_coef"], reverse=True)
    return []


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--features-uri", default=features_v2_path())
    args = ap.parse_args()

    spark = get_spark("BEN-A3-Step2-Multiclass")
    feats = load_features_v2(spark, args.features_uri).cache()
    selected = selected_frequent_classes(feats, ASSIGNMENT3_N_CLASSES)
    task = build_single_label_task(feats, selected).cache()
    weighted = add_class_weights(task.filter(F.col("split") == "train"), task).cache()
    train, val, test = split_frames(weighted)
    train.cache(); val.cache(); test.cache()

    class_counts = {
        split: [r.asDict() for r in weighted.filter(F.col("split") == split).groupBy("target").count().orderBy(F.desc("count")).collect()]
        for split in ["train", "validation", "test"]
    }

    lr, rf, ovr = make_classifiers(ASSIGNMENT3_SEED)
    jobs = [
        ("lr", lr, grid_for_model("lr", lr, ASSIGNMENT3_LR_GRID)),
        ("rf", rf, grid_for_model("rf", rf, ASSIGNMENT3_RF_GRID)),
        ("gbt_ovr", ovr, grid_for_model("gbt_ovr", ovr, ASSIGNMENT3_GBT_GRID)),
    ]
    tuned = []
    for name, clf, grid in jobs:
        print(f"Tuning {name}: {len(grid)} configs")
        tuned.append(tune_on_validation(name, clf, grid, train, val))

    best = max(tuned, key=lambda x: x["best"]["validation_macro_f1"])
    best_model = best["pipeline_model"]
    test_pred = best_model.transform(test).cache()
    test_metrics = evaluate_multiclass(test_pred)

    interpretations = {
        "rf_feature_importances": next((_feature_importance(x["pipeline_model"]) for x in tuned if x["model_name"] == "rf"), []),
        "lr_coefficients": next((_lr_coefficients(x["pipeline_model"]) for x in tuned if x["model_name"] == "lr"), []),
    }
    ranking = interpretations["rf_feature_importances"] or interpretations["lr_coefficients"]
    ranked_features = [r["feature"] for r in ranking] or ASSIGNMENT3_FEATURE_COLS

    topk_results = []
    for k in ASSIGNMENT3_TOP_K:
        cols = ranked_features[:k]
        t0 = time.time()
        lr_k, rf_k, ovr_k = make_classifiers(ASSIGNMENT3_SEED)
        clf_k = {"lr": lr_k, "rf": rf_k, "gbt_ovr": ovr_k}[best["model_name"]]
        model_k = make_base_pipeline(clf_k, cols).fit(train)
        pred_k = model_k.transform(test).cache()
        metrics_k = evaluate_multiclass(pred_k)
        topk_results.append({"k": k, "features": cols, "training_time_sec": round(time.time() - t0, 3), "test_metrics": metrics_k})

    a2 = _safe_read_json(Path("reports") / "step4_metrics.json")
    report = {
        "step": "A3-Step2",
        "rules": {"random_split_used": False, "validation_tuning_only": True, "test_used_once_after_selection": True},
        "selected_top_classes": selected,
        "class_counts_per_split": class_counts,
        "grid_results": [{"model_name": x["model_name"], "best": x["best"], "grid_results": x["grid_results"]} for x in tuned],
        "best_model_name": best["model_name"],
        "label_index_mapping": label_index_mapping(best_model),
        "test_metrics": test_metrics,
        "assignment2_baselines": a2.get("test_metrics", {}),
        "interpretation": interpretations,
        "top_k_retrain": topk_results,
    }
    out = Path("reports") / "a3_step2_multiclass.json"
    write_json(out, report)
    write_json(local_hdfs_mirror() / "results" / "a3_step2_multiclass.json", report)
    task.write.mode("overwrite").parquet(f"{results_path().rstrip('/')}/a3_multiclass_task")
    print(json.dumps(report, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()


