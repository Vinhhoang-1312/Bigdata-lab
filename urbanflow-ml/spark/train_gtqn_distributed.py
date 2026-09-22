#!/usr/bin/env python3
"""Train GTQN-Lite on two Spark executors with PyTorch DDP."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml
from pyspark.ml.torch.distributor import TorchDistributor
from pyspark.sql import SparkSession

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from forecasting.distributed import distributed_train, is_training_complete

DEFAULT_CONFIG = "/workspace/urbanflow-ml/config/settings.example.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--variant", choices=("no_graph", "fixed_graph", "full"), default="full")
    parser.add_argument("--max-epochs", type=int, default=None)
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args()


def path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    return bool(hadoop_path.getFileSystem(hadoop_conf).exists(hadoop_path))


def main() -> int:
    args = parse_args()
    with open(args.config, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    run_root = config["paths"]["work"].rstrip("/") + f"/runs/{args.run_id}"
    artifact_path = f"{run_root}/tensor/artifact.npz"
    result_path = f"{run_root}/training/{args.variant}/result.json"

    spark = (
        SparkSession.builder.appName(f"UrbanFlow GTQN DDP {args.variant}")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        if not path_exists(spark, artifact_path):
            raise FileNotFoundError(f"Tensor artifact does not exist: {artifact_path}")
        if args.resume and path_exists(spark, result_path):
            result = json.loads(spark.read.text(result_path).first().value)
            target_max_epochs = int(args.max_epochs or config["training"]["max_epochs"])
            if is_training_complete(result, target_max_epochs=target_max_epochs):
                print(json.dumps(result, sort_keys=True))
                print("ML DISTRIBUTED TRAINING = PASS (RESUMED COMPLETE)")
                return 0

        distributor = TorchDistributor(
            num_processes=int(config["training"]["num_processes"]),
            local_mode=False,
            use_gpu=False,
        )
        result = distributor.run(
            distributed_train,
            config,
            artifact_path,
            args.run_id,
            args.variant,
            args.resume,
            args.max_epochs,
        )

        if result["status"] != "PASS":
            raise RuntimeError(f"Distributed evidence failed: {json.dumps(result['distribution'], sort_keys=True)}")
        print(json.dumps(result, sort_keys=True))
        print("ML DISTRIBUTED TRAINING = PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
