"""Shared Spark ML helpers for Assignment 3."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Iterable

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, OneVsRest, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.sql import DataFrame, functions as F

from scripts.a3_config import ASSIGNMENT3_FEATURE_COLS, ASSIGNMENT3_N_CLASSES
from scripts.lib.io_stats import write_json


def load_features_v2(spark, uri: str) -> DataFrame:
    return spark.read.parquet(uri).filter(~F.col("error_flag"))


def selected_frequent_classes(features: DataFrame, n_classes: int = ASSIGNMENT3_N_CLASSES) -> list[str]:
    rows = (
        features.filter(F.col("split") == "train")
        .select(F.explode("labels").alias("target"))
        .groupBy("target")
        .count()
        .orderBy(F.desc("count"), F.asc("target"))
        .limit(n_classes)
        .collect()
    )
    return [r["target"] for r in rows]


def build_single_label_task(features: DataFrame, selected: list[str]) -> DataFrame:
    exploded = features.select(
        "patch_id", "country", "split", "labels", *ASSIGNMENT3_FEATURE_COLS, F.explode("labels").alias("target")
    ).filter(F.col("target").isin(selected))
    counts = exploded.groupBy("patch_id").agg(F.countDistinct("target").alias("n_selected"))
    single = counts.filter(F.col("n_selected") == 1).select("patch_id")
    return exploded.join(single, "patch_id", "inner").dropDuplicates(["patch_id"])


def add_class_weights(train: DataFrame, all_df: DataFrame, target_col: str = "target") -> DataFrame:
    total = train.count()
    n_classes = train.select(target_col).distinct().count()
    weights = train.groupBy(target_col).count().withColumn(
        "weight", F.lit(float(total)) / (F.lit(float(n_classes)) * F.col("count"))
    ).select(target_col, "weight")
    return all_df.join(F.broadcast(weights), target_col, "left").fillna({"weight": 1.0})


def split_frames(task: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    return (
        task.filter(F.col("split") == "train"),
        task.filter(F.col("split") == "validation"),
        task.filter(F.col("split") == "test"),
    )


def make_base_pipeline(classifier, feature_cols: list[str] | None = None) -> Pipeline:
    feature_cols = feature_cols or ASSIGNMENT3_FEATURE_COLS
    indexer = StringIndexer(inputCol="target", outputCol="label", handleInvalid="keep")
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features", handleInvalid="keep")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
    return Pipeline(stages=[indexer, assembler, scaler, classifier])


def grid_for_model(model_name: str, classifier, grid_conf: dict) -> list:
    builder = ParamGridBuilder()
    if model_name == "lr":
        builder = builder.addGrid(classifier.regParam, grid_conf["regParam"]).addGrid(classifier.elasticNetParam, grid_conf["elasticNetParam"])
    elif model_name == "rf":
        builder = builder.addGrid(classifier.numTrees, grid_conf["numTrees"]).addGrid(classifier.maxDepth, grid_conf["maxDepth"])
    elif model_name == "gbt_ovr":
        base = classifier.getClassifier()
        builder = builder.addGrid(base.maxIter, grid_conf["maxIter"]).addGrid(base.maxDepth, grid_conf["maxDepth"])
    else:
        raise ValueError(model_name)
    return builder.build()


def macro_f1_from_predictions(pred: DataFrame, label_col: str = "label", pred_col: str = "prediction") -> float:
    labels = [r[label_col] for r in pred.select(label_col).distinct().collect()]
    per = per_class_metrics(pred, label_col, pred_col, labels)
    return sum(x["f1"] for x in per) / len(per) if per else 0.0


def per_class_metrics(pred: DataFrame, label_col: str, pred_col: str, labels: Iterable[float]) -> list[dict]:
    out = []
    for lab in labels:
        tp = pred.filter((F.col(label_col) == lab) & (F.col(pred_col) == lab)).count()
        fp = pred.filter((F.col(label_col) != lab) & (F.col(pred_col) == lab)).count()
        fn = pred.filter((F.col(label_col) == lab) & (F.col(pred_col) != lab)).count()
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
        out.append({"label_index": float(lab), "precision": precision, "recall": recall, "f1": f1, "tp": tp, "fp": fp, "fn": fn})
    return out


def evaluate_multiclass(pred: DataFrame) -> dict:
    pred = pred.cache()
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    labels = [r["label"] for r in pred.select("label").distinct().collect()]
    per = per_class_metrics(pred, "label", "prediction", labels)
    confusion = pred.groupBy("label").pivot("prediction").count().fillna(0).orderBy("label")
    return {
        "n": pred.count(),
        "accuracy": evaluator_acc.evaluate(pred),
        "weighted_f1": evaluator_f1.evaluate(pred),
        "macro_f1": sum(x["f1"] for x in per) / len(per) if per else 0.0,
        "per_class": per,
        "confusion_rows": [r.asDict() for r in confusion.collect()],
    }


def tune_on_validation(model_name: str, classifier, grid, train: DataFrame, val: DataFrame, feature_cols: list[str] | None = None) -> dict:
    rows = []
    best = None
    best_score = -1.0
    best_model = None
    for i, params in enumerate(grid):
        est = classifier.copy(params)
        pipe = make_base_pipeline(est, feature_cols)
        t0 = time.time()
        model = pipe.fit(train)
        train_sec = time.time() - t0
        pred = model.transform(val).cache()
        score = macro_f1_from_predictions(pred)
        param_map = {p.name: v for p, v in params.items()}
        rec = {"model": model_name, "config_id": i, "params": param_map, "validation_macro_f1": score, "training_time_sec": round(train_sec, 3)}
        rows.append(rec)
        if score > best_score:
            best = rec
            best_score = score
            best_model = model
    return {"model_name": model_name, "grid_results": rows, "best": best, "pipeline_model": best_model}


def make_classifiers(seed: int):
    lr = LogisticRegression(labelCol="label", featuresCol="features", weightCol="weight", family="multinomial", maxIter=60, standardization=False)
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", weightCol="weight", seed=seed)
    gbt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=10, maxDepth=3, seed=seed)
    ovr = OneVsRest(labelCol="label", featuresCol="features", classifier=gbt, weightCol="weight", parallelism=1)
    return lr, rf, ovr


def label_index_mapping(model) -> list[dict]:
    labels = model.stages[0].labels
    return [{"label_index": float(i), "class": lab} for i, lab in enumerate(labels)]


def save_json_report(path: Path, payload: dict) -> None:
    write_json(path, payload)

