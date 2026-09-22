#!/usr/bin/env python3
"""Build a compact, restart-safe tensor artifact from canonical Silver."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, functions as F

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from forecasting.demand_frame import build_demand_frame
from forecasting.graph import build_od_adjacency
from forecasting.scaling import RobustZoneScaler
from forecasting.tensor_data import build_training_arrays, dataset_fingerprint, save_tensor_artifact

DEFAULT_CONFIG = "/workspace/urbanflow-ml/config/settings.example.yaml"
DEFAULT_ZONE_LOOKUP = "s3a://urbanflow/bronze/zone_lookup/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--zone-lookup", default=DEFAULT_ZONE_LOOKUP)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args()


def path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    return bool(hadoop_path.getFileSystem(hadoop_conf).exists(hadoop_path))


def copy_local_to_s3a(spark: SparkSession, local_path: Path, destination: str) -> None:
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    source = jvm.org.apache.hadoop.fs.Path(local_path.as_uri())
    target = jvm.org.apache.hadoop.fs.Path(destination)
    target.getFileSystem(hadoop_conf).copyFromLocalFile(False, False, source, target)


def _split_config(forecast: dict) -> dict[str, str]:
    return {
        key: forecast[key]
        for key in (
            "train_start",
            "train_end",
            "validation_start",
            "validation_end",
            "test_start",
            "test_end",
        )
    }


def main() -> int:
    args = parse_args()
    with open(args.config, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    forecast = config["forecast"]
    run_root = config["paths"]["work"].rstrip("/") + f"/runs/{args.run_id}"
    artifact_path = f"{run_root}/tensor/artifact.npz"
    metadata_path = f"{run_root}/tensor/metadata"

    spark = (
        SparkSession.builder.appName("UrbanFlow ML Tensor Dataset")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        artifact_exists = path_exists(spark, artifact_path)
        metadata_exists = path_exists(spark, metadata_path)
        if args.resume and artifact_exists and metadata_exists:
            print(json.dumps({"status": "PASS", "stage": "tensor", "resumed": True, "artifact_path": artifact_path}))
            print("ML TENSOR-GO = PASS (RESUMED)")
            return 0
        if artifact_exists or metadata_exists:
            raise FileExistsError(f"Partial or existing tensor output at {run_root}; use a new run ID or --resume")

        silver = spark.read.parquet(config["paths"]["silver"])
        zone_lookup = spark.read.option("header", True).csv(args.zone_lookup)
        demand = build_demand_frame(
            silver,
            zone_lookup,
            forecast["source_warmup_start"],
            forecast["test_end"],
        )
        zone_ids = [
            int(row.pickup_zone_id)
            for row in demand.select("pickup_zone_id").distinct().orderBy("pickup_zone_id").collect()
        ]
        expected_zones = int(config["tensor"]["expected_zone_count"])
        if len(zone_ids) != expected_zones:
            raise ValueError(f"Expected {expected_zones} zones, found {len(zone_ids)}")

        compact_rows = (
            demand.groupBy("service_date", "half_hour_slot")
            .agg(
                F.sum("trip_count").alias("bucket_total"),
                F.sort_array(
                    F.collect_list(F.struct(F.col("pickup_zone_id"), F.col("trip_count")))
                ).alias("zone_counts"),
            )
            .withColumn("source_observed", F.col("bucket_total") > F.lit(0))
            .select("service_date", "half_hour_slot", "source_observed", "zone_counts")
            .orderBy("service_date", "half_hour_slot")
            .collect()
        )
        demand_rows = [row.asDict(recursive=True) for row in compact_rows]
        arrays, metadata = build_training_arrays(demand_rows, zone_ids, _split_config(forecast))

        edge_rows = (
            silver.where(
                (F.to_date("pickup_datetime") >= F.to_date(F.lit(forecast["train_start"])))
                & (F.to_date("pickup_datetime") <= F.to_date(F.lit(forecast["train_end"])))
            )
            .groupBy("pickup_zone_id", "dropoff_zone_id")
            .agg(F.count(F.lit(1)).alias("edge_count"))
            .collect()
        )
        edges = [
            {
                "pickup_datetime": forecast["train_start"] + "T00:00:00",
                "pickup_zone_id": int(row.pickup_zone_id),
                "dropoff_zone_id": int(row.dropoff_zone_id),
                "edge_count": int(row.edge_count),
            }
            for row in edge_rows
        ]
        adjacency = build_od_adjacency(edges, zone_ids, forecast["train_end"])
        train_left, train_right = metadata["split_indices"]["train"]
        scaler = RobustZoneScaler.fit(arrays["counts"][train_left:train_right])
        arrays["fixed_adjacency"] = adjacency
        arrays["scaler_center"] = scaler.center
        arrays["scaler_scale"] = scaler.scale
        metadata["dataset_fingerprint"] = dataset_fingerprint(arrays, metadata["split_indices"])
        metadata.update(
            {
                "status": "PASS",
                "stage": "tensor",
                "run_id": args.run_id,
                "artifact_path": artifact_path,
                "source_silver": config["paths"]["silver"],
                "incomplete_data_warning": "October and December 2022 contain only sparse source rows",
            }
        )

        with tempfile.TemporaryDirectory(prefix="urbanflow-tensor-") as directory:
            local_artifact = Path(directory) / "artifact.npz"
            save_tensor_artifact(local_artifact, arrays, metadata)
            copy_local_to_s3a(spark, local_artifact, artifact_path)
        spark.createDataFrame([(json.dumps(metadata, sort_keys=True),)], "value string").coalesce(1).write.mode(
            "errorifexists"
        ).text(metadata_path)

        print(json.dumps(metadata, sort_keys=True))
        print("ML TENSOR-GO = PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
