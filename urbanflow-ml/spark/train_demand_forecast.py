#!/usr/bin/env python3
"""Train the experimental global GBT demand forecast model."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, functions as F

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from forecasting.demand_frame import build_demand_frame
from forecasting.features import FEATURE_COLUMNS, build_features
from forecasting.model import DEFAULT_CANDIDATES, build_pipeline, predict, select_params

DEFAULT_CONFIG = "/workspace/urbanflow-ml/config/settings.example.yaml"
DEFAULT_ZONE_LOOKUP = "/workspace/urbanflow-foundation/fixtures/bronze/zone_lookup.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--zone-lookup", default=DEFAULT_ZONE_LOOKUP)
    parser.add_argument("--run-id", default=None)
    return parser.parse_args()


def read_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def git_sha() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short=8", "HEAD"],
            cwd=Path(__file__).resolve().parents[2],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()
    except Exception:
        return "nogit"


def config_hash(config: dict) -> str:
    payload = json.dumps(config, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:12]


def default_run_id(config: dict) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{git_sha()}-{config_hash(config)}"


def path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    return bool(jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf).exists(jvm.org.apache.hadoop.fs.Path(path)))


def date_filter(frame, start_date: str, end_date: str):
    return frame.where((F.col("service_date") >= F.lit(start_date)) & (F.col("service_date") <= F.lit(end_date)))


def main() -> int:
    args = parse_args()
    config = read_config(args.config)
    forecast = config["forecast"]
    paths = config["paths"]
    run_id = args.run_id or default_run_id(config)
    run_root = paths["work"].rstrip("/") + f"/runs/{run_id}"
    model_path = f"{run_root}/model"
    validation_predictions_path = f"{run_root}/predictions/validation"
    test_predictions_path = f"{run_root}/predictions/test"
    metadata_path = f"{run_root}/metadata"

    spark = (
        SparkSession.builder.appName("UrbanFlow ML Demand Forecast Training")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        if path_exists(spark, run_root):
            raise FileExistsError(f"Run path already exists: {run_root}")

        silver = spark.read.parquet(paths["silver"])
        zone_lookup = spark.read.option("header", True).csv(args.zone_lookup)
        demand = build_demand_frame(
            silver,
            zone_lookup,
            forecast["source_warmup_start"],
            forecast["test_end"],
        )
        features = build_features(demand).cache()
        train = date_filter(features, forecast["train_start"], forecast["train_end"])
        validation = date_filter(features, forecast["validation_start"], forecast["validation_end"])
        test = date_filter(features, forecast["test_start"], forecast["test_end"])

        seed = int(forecast["seed"])
        selection = select_params(train, validation, DEFAULT_CANDIDATES, seed=seed)
        final_train = train.unionByName(validation)
        model = build_pipeline(seed=seed, params=selection.selected_params).fit(final_train)

        validation_predictions = predict(model, validation)
        test_predictions = predict(model, test)
        model.write().save(model_path)
        validation_predictions.write.mode("errorifexists").parquet(validation_predictions_path)
        test_predictions.write.mode("errorifexists").parquet(test_predictions_path)

        metadata = {
            "status": "PASS",
            "run_id": run_id,
            "run_root": run_root,
            "model_path": model_path,
            "validation_predictions_path": validation_predictions_path,
            "test_predictions_path": test_predictions_path,
            "config_path": args.config,
            "config_hash": config_hash(config),
            "git_sha": git_sha(),
            "feature_columns": FEATURE_COLUMNS,
            "selection": selection.as_metadata(),
            "rows": {
                "train": int(train.count()),
                "validation": int(validation.count()),
                "test": int(test.count()),
            },
        }
        spark.createDataFrame([(json.dumps(metadata, sort_keys=True),)], "metadata_json string").write.mode(
            "errorifexists"
        ).text(metadata_path)
        print(json.dumps(metadata, sort_keys=True))
        print("ML TRAINING = PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
