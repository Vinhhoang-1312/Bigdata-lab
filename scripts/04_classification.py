#!/usr/bin/env python3
"""Step 4 — Rule-based + nearest-centroid classifiers (DataFrame / Spark SQL only)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.lib.constants import DEFAULT_N_CLASSES, FEATURE_COLS
from scripts.lib.io_stats import write_json
from scripts.lib.paths import features_path, local_hdfs_mirror, results_path
from scripts.lib.spark_session import get_spark


def build_single_label_task(feats, n_classes: int):
    train = feats.filter(F.col("split") == "train")
    freq = (
        train.select(F.explode("labels").alias("label"))
        .groupBy("label")
        .count()
        .orderBy(F.desc("count"))
    )
    selected = [r["label"] for r in freq.limit(n_classes).collect()]
    print(f"Selected target classes ({len(selected)}): {selected}")

    # Keep patches whose labels intersect selected in exactly one class
    # target = that class
    exploded = feats.select(
        "patch_id",
        "country",
        "split",
        *FEATURE_COLS,
        F.explode("labels").alias("label"),
    ).filter(F.col("label").isin(selected))

    counts = exploded.groupBy("patch_id").agg(F.countDistinct("label").alias("n_sel"))
    single = counts.filter(F.col("n_sel") == 1).select("patch_id")
    task = (
        exploded.join(single, on="patch_id", how="inner")
        .withColumnRenamed("label", "target")
        .dropDuplicates(["patch_id"])
    )
    return task, selected


def majority_baseline(train_df):
    row = (
        train_df.groupBy("target")
        .count()
        .orderBy(F.desc("count"))
        .limit(1)
        .collect()[0]
    )
    return row["target"]


def fit_rule_thresholds(train_df):
    """Derive simple thresholds from train only (quantiles / class means)."""
    # Use NDVI + brightness + mean_b08
    stats = train_df.select(
        F.expr("percentile_approx(ndvi, 0.5)").alias("ndvi_med"),
        F.expr("percentile_approx(brightness, 0.5)").alias("bri_med"),
        F.expr("percentile_approx(mean_b08, 0.5)").alias("nir_med"),
    ).collect()[0]

    # Class means for ordering (wet/veg vs bright)
    class_means = (
        train_df.groupBy("target")
        .agg(
            F.avg("ndvi").alias("ndvi"),
            F.avg("brightness").alias("brightness"),
            F.avg("mean_b08").alias("mean_b08"),
        )
        .collect()
    )
    # Sort classes by NDVI ascending for a crude ordered CASE
    ordered = sorted(class_means, key=lambda r: r["ndvi"])
    return {
        "ndvi_med": float(stats["ndvi_med"]),
        "bri_med": float(stats["bri_med"]),
        "nir_med": float(stats["nir_med"]),
        "ordered_targets": [r["target"] for r in ordered],
        "class_means": [
            {
                "target": r["target"],
                "ndvi": float(r["ndvi"]),
                "brightness": float(r["brightness"]),
                "mean_b08": float(r["mean_b08"]),
            }
            for r in ordered
        ],
    }


def apply_rule(df, thresholds: dict):
    """CASE WHEN on ≤3 features; maps NDVI tertiles-ish onto ordered classes."""
    ordered = thresholds["ordered_targets"]
    if len(ordered) == 0:
        return df.withColumn("pred_rule", F.lit(None))
    # Build nested CASE: low NDVI → first class, mid → middle, high → last (extend for 3–5)
    # Thresholds between consecutive class mean NDVIs
    means = [c["ndvi"] for c in thresholds["class_means"]]
    cuts = []
    for i in range(len(means) - 1):
        cuts.append(0.5 * (means[i] + means[i + 1]))

    expr = F.lit(ordered[-1])
    for i in reversed(range(len(cuts))):
        expr = F.when(F.col("ndvi") < F.lit(cuts[i]), F.lit(ordered[i])).otherwise(expr)
    # Tie-break with brightness / nir (still ≤3 features total in the rule story)
    # Keep NDVI-primary rule; document bri/nir medians used for discussion variants
    return df.withColumn("pred_rule", expr)


def standardize_with_train(train_df, all_df, feature_cols):
    aggs = []
    for c in feature_cols:
        aggs += [F.avg(c).alias(f"{c}__mu"), F.stddev_pop(c).alias(f"{c}__sd")]
    stats = train_df.agg(*aggs).collect()[0]
    out = all_df
    mu_sd = {}
    for c in feature_cols:
        mu = float(stats[f"{c}__mu"])
        sd = float(stats[f"{c}__sd"]) or 1.0
        mu_sd[c] = {"mu": mu, "sd": sd}
        out = out.withColumn(f"z_{c}", (F.col(c) - F.lit(mu)) / F.lit(sd))
    return out, mu_sd


def nearest_centroid(all_z, train_z, feature_cols, pred_col="pred_centroid"):
    zcols = [f"z_{c}" for c in feature_cols]
    centroid_aggs = [F.avg(c).alias(c) for c in zcols]
    centroids = train_z.groupBy("target").agg(*centroid_aggs)
    centroids = F.broadcast(centroids)

    # Rename centroid columns
    c_renamed = centroids.withColumnRenamed("target", "centroid_target")
    for c in zcols:
        c_renamed = c_renamed.withColumnRenamed(c, f"c_{c}")

    crossed = all_z.crossJoin(c_renamed)
    dist = None
    for c in zcols:
        term = (F.col(c) - F.col(f"c_{c}")) ** 2
        dist = term if dist is None else (dist + term)
    crossed = crossed.withColumn("dist2", dist)
    w = Window.partitionBy("patch_id").orderBy(F.col("dist2").asc())
    pred = (
        crossed.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("patch_id", F.col("centroid_target").alias(pred_col))
    )
    return all_z.join(pred, on="patch_id", how="left"), centroids


def evaluate(df, target_col, pred_col, labels):
    scored = df.filter(F.col(pred_col).isNotNull())
    total = scored.count()
    correct = scored.filter(F.col(target_col) == F.col(pred_col)).count()
    accuracy = correct / total if total else 0.0

    conf = (
        scored.groupBy(target_col)
        .pivot(pred_col, labels)
        .count()
        .fillna(0)
    )

    # Per-class precision / recall / F1
    per_class = []
    for lab in labels:
        tp = scored.filter((F.col(target_col) == lab) & (F.col(pred_col) == lab)).count()
        fp = scored.filter((F.col(target_col) != lab) & (F.col(pred_col) == lab)).count()
        fn = scored.filter((F.col(target_col) == lab) & (F.col(pred_col) != lab)).count()
        prec = tp / (tp + fp) if (tp + fp) else 0.0
        rec = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
        per_class.append(
            {"class": lab, "precision": prec, "recall": rec, "f1": f1, "tp": tp, "fp": fp, "fn": fn}
        )
    macro_f1 = sum(x["f1"] for x in per_class) / len(per_class) if per_class else 0.0
    return {
        "n": total,
        "accuracy": accuracy,
        "macro_f1": macro_f1,
        "per_class": per_class,
        "confusion": conf,
    }


def select_reduced_features(train_df, val_df, feature_cols, k: int = 5):
    """Pick ≤5 features by mean class-separation on VALIDATION (selection only)."""
    # Separation score: variance of class means / mean within-class variance (proxy)
    scores = []
    for c in feature_cols:
        class_mu = val_df.groupBy("target").agg(F.avg(c).alias("mu"), F.variance(c).alias("var"))
        rows = class_mu.collect()
        if len(rows) < 2:
            scores.append((c, 0.0))
            continue
        mus = [float(r["mu"]) for r in rows]
        vars_ = [float(r["var"]) if r["var"] is not None else 0.0 for r in rows]
        between = float(pd_var(mus))
        within = sum(vars_) / len(vars_) if vars_ else 1.0
        scores.append((c, between / (within + 1e-12)))
    scores.sort(key=lambda x: x[1], reverse=True)
    return [c for c, _ in scores[:k]], scores


def pd_var(xs):
    m = sum(xs) / len(xs)
    return sum((x - m) ** 2 for x in xs) / len(xs)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--n-classes", type=int, default=DEFAULT_N_CLASSES, choices=range(3, 6))
    args = ap.parse_args()

    spark = get_spark("BEN-Step4-Classify")
    feats = spark.read.parquet(features_path()).filter(~F.col("error_flag"))

    task, selected = build_single_label_task(feats, args.n_classes)
    task.write.mode("overwrite").parquet(f"{results_path().rstrip('/')}/clf_task_rows")

    train = task.filter(F.col("split") == "train")
    val = task.filter(F.col("split") == "validation")
    test = task.filter(F.col("split") == "test")

    counts = {
        "train": train.count(),
        "validation": val.count(),
        "test": test.count(),
        "classes": selected,
    }
    print("Task counts:", counts)

    majority = majority_baseline(train)
    test_maj = test.withColumn("pred_majority", F.lit(majority))
    maj_metrics = evaluate(test_maj, "target", "pred_majority", selected)

    thr = fit_rule_thresholds(train)
    test_rule = apply_rule(test, thr)
    rule_metrics = evaluate(test_rule, "target", "pred_rule", selected)

    # Full feature centroid
    all_z, mu_sd = standardize_with_train(train, task, list(FEATURE_COLS))
    train_z = all_z.filter(F.col("split") == "train")
    test_z = all_z.filter(F.col("split") == "test")
    test_cent, _ = nearest_centroid(test_z, train_z, list(FEATURE_COLS))
    cent_metrics = evaluate(test_cent, "target", "pred_centroid", selected)

    # Reduced features chosen on validation
    reduced, sep_scores = select_reduced_features(train, val, list(FEATURE_COLS), k=5)
    all_zr, mu_sd_r = standardize_with_train(train, task, reduced)
    train_zr = all_zr.filter(F.col("split") == "train")
    test_zr = all_zr.filter(F.col("split") == "test")
    test_cent_r, _ = nearest_centroid(test_zr, train_zr, reduced, pred_col="pred_centroid_reduced")
    cent_r_metrics = evaluate(test_cent_r, "target", "pred_centroid_reduced", selected)

    # Persist confusion matrices
    maj_metrics["confusion"].write.mode("overwrite").parquet(
        f"{results_path().rstrip('/')}/clf_confusion_majority"
    )
    rule_metrics["confusion"].write.mode("overwrite").parquet(
        f"{results_path().rstrip('/')}/clf_confusion_rule"
    )
    cent_metrics["confusion"].write.mode("overwrite").parquet(
        f"{results_path().rstrip('/')}/clf_confusion_centroid"
    )
    cent_r_metrics["confusion"].write.mode("overwrite").parquet(
        f"{results_path().rstrip('/')}/clf_confusion_centroid_reduced"
    )

    def strip_conf(m):
        return {k: v for k, v in m.items() if k != "confusion"}

    report = {
        "step": 4,
        "counts": counts,
        "majority_class": majority,
        "rule_thresholds": thr,
        "train_standardize": mu_sd,
        "reduced_features": reduced,
        "separation_scores_validation": sep_scores,
        "test_metrics": {
            "majority": strip_conf(maj_metrics),
            "rule": strip_conf(rule_metrics),
            "centroid_full": strip_conf(cent_metrics),
            "centroid_reduced": strip_conf(cent_r_metrics),
        },
        "discussion_hints": [
            "Confused classes often share similar NDVI/brightness (e.g. forest subtypes).",
            "Hand-crafted band stats lose spatial texture → centroid limits.",
            "Reduced set chosen on validation; metrics reported on test once.",
        ],
    }
    write_json(Path("reports") / "step4_metrics.json", report)
    write_json(local_hdfs_mirror() / "results" / "step4_metrics.json", report)
    Path("reports/DISCUSSION_NOTES.md").write_text(
        "\n".join(
            [
                "# Classification discussion notes",
                "",
                f"- Majority class: `{majority}`",
                f"- Rule uses NDVI cuts between train class means: {thr['ordered_targets']}",
                f"- Reduced features (val separation): {reduced}",
                "- See step4_metrics.json for per-class P/R/F1 and macro-F1.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()
