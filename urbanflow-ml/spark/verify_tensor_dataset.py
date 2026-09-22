#!/usr/bin/env python3
"""Read a tensor artifact back from S3A and verify its full contract."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

import numpy as np
from pyspark.sql import SparkSession

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from forecasting.tensor_data import load_tensor_artifact


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", required=True)
    parser.add_argument("--expected-zones", type=int, default=265)
    return parser.parse_args()


def copy_s3a_to_local(spark: SparkSession, source: str, local_path: Path) -> None:
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    source_path = jvm.org.apache.hadoop.fs.Path(source)
    target_path = jvm.org.apache.hadoop.fs.Path(local_path.as_uri())
    source_path.getFileSystem(hadoop_conf).copyToLocalFile(False, source_path, target_path, True)


def main() -> int:
    args = parse_args()
    spark = SparkSession.builder.appName("UrbanFlow ML Tensor Verification").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        with tempfile.TemporaryDirectory(prefix="urbanflow-verify-") as directory:
            local_path = Path(directory) / "artifact.npz"
            copy_s3a_to_local(spark, args.artifact, local_path)
            arrays, metadata = load_tensor_artifact(local_path)

        counts = arrays["counts"]
        adjacency = arrays["fixed_adjacency"]
        if counts.ndim != 2 or counts.shape[1] != args.expected_zones:
            raise ValueError(f"invalid count shape: {counts.shape}")
        if not np.isfinite(counts).all() or (counts < 0).any():
            raise ValueError("counts contain invalid values")
        if arrays["observed"].shape != counts.shape:
            raise ValueError("observed mask shape does not match counts")
        if adjacency.shape != (args.expected_zones, args.expected_zones):
            raise ValueError(f"invalid adjacency shape: {adjacency.shape}")
        if not np.allclose(adjacency.sum(axis=1), 1.0, atol=1e-5):
            raise ValueError("adjacency rows are not normalized")
        if arrays["scaler_center"].shape != (args.expected_zones,):
            raise ValueError("invalid scaler center shape")
        if arrays["scaler_scale"].shape != (args.expected_zones,) or (arrays["scaler_scale"] <= 0).any():
            raise ValueError("invalid scaler scale")

        summary = {
            "status": "PASS",
            "artifact": args.artifact,
            "dataset_fingerprint": metadata["dataset_fingerprint"],
            "counts_shape": list(counts.shape),
            "calendar_shape": list(arrays["calendar"].shape),
            "adjacency_shape": list(adjacency.shape),
            "split_indices": metadata["split_indices"],
            "unobserved_bucket_count": metadata["unobserved_bucket_count"],
        }
        print(json.dumps(summary, sort_keys=True))
        print("ML TENSOR VERIFY = PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
