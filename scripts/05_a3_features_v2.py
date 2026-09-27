#!/usr/bin/env python3
"""Assignment 3 Step 1 — richer BigEarthNet features from HDF5 with Spark mapPartitions."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import h5py
from pyspark.sql import Row, functions as F
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, LongType, StringType, StructField, StructType

from scripts.a3_config import ASSIGNMENT3_DEFAULT_MANIFEST, ASSIGNMENT3_FEATURE_COLS
from scripts.lib.features_core import compute_richer_from_hdf5_image
from scripts.lib.io_stats import write_json
from scripts.lib.paths import features_v2_path, local_hdfs_mirror
from scripts.lib.spark_session import get_spark

FEATURE_V2_SCHEMA = StructType([
    StructField("patch_id", StringType(), False),
    StructField("country", StringType(), True),
    StructField("split", StringType(), True),
    StructField("labels", ArrayType(StringType()), True),
    *[StructField(c, DoubleType(), True) for c in ASSIGNMENT3_FEATURE_COLS],
    StructField("error_flag", BooleanType(), False),
    StructField("error_message", StringType(), True),
    StructField("hdf5_file", StringType(), True),
    StructField("hdf5_index", LongType(), True),
])


def _partition(rows):
    handles = {}
    try:
        for row in rows:
            path = row.hdf5_path
            if path not in handles:
                handles[path] = h5py.File(path, "r")
            try:
                image = handles[path]["images"][int(row.hdf5_index)]
                feats = compute_richer_from_hdf5_image(image)
                yield Row(
                    patch_id=row.patch_id,
                    country=row.country,
                    split=row.split,
                    labels=row.labels,
                    **feats,
                    error_flag=False,
                    error_message=None,
                    hdf5_file=Path(path).name,
                    hdf5_index=int(row.hdf5_index),
                )
            except Exception as exc:  # noqa: BLE001
                yield Row(
                    patch_id=row.patch_id,
                    country=row.country,
                    split=row.split,
                    labels=row.labels,
                    **{c: None for c in ASSIGNMENT3_FEATURE_COLS},
                    error_flag=True,
                    error_message=f"{type(exc).__name__}: {exc}",
                    hdf5_file=Path(path).name,
                    hdf5_index=int(row.hdf5_index),
                )
    finally:
        for handle in handles.values():
            handle.close()


def validate_features(df):
    total = df.count()
    null_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ASSIGNMENT3_FEATURE_COLS]).collect()[0].asDict()
    nan_counts = df.select([F.sum(F.isnan(F.col(c)).cast("int")).alias(c) for c in ASSIGNMENT3_FEATURE_COLS]).collect()[0].asDict()
    bad_ranges = {
        "ndvi": df.filter((F.col("ndvi") < -1.001) | (F.col("ndvi") > 1.001)).count(),
        "ndwi": df.filter((F.col("ndwi") < -1.001) | (F.col("ndwi") > 1.001)).count(),
        "ndmi": df.filter((F.col("ndmi") < -1.001) | (F.col("ndmi") > 1.001)).count(),
        "ndbi": df.filter((F.col("ndbi") < -1.001) | (F.col("ndbi") > 1.001)).count(),
        "ndvi_p10": df.filter((F.col("ndvi_p10") < -1.001) | (F.col("ndvi_p10") > 1.001)).count(),
        "ndvi_p50": df.filter((F.col("ndvi_p50") < -1.001) | (F.col("ndvi_p50") > 1.001)).count(),
        "ndvi_p90": df.filter((F.col("ndvi_p90") < -1.001) | (F.col("ndvi_p90") > 1.001)).count(),
    }
    split_counts = {r["split"]: r["count"] for r in df.groupBy("split").count().collect()}
    country_counts = {r["country"]: r["count"] for r in df.groupBy("country").count().collect()}
    return {
        "rows": total,
        "errors": df.filter(F.col("error_flag")).count(),
        "countries": country_counts,
        "splits": split_counts,
        "n_features": len(ASSIGNMENT3_FEATURE_COLS),
        "feature_columns": ASSIGNMENT3_FEATURE_COLS,
        "null_counts": null_counts,
        "nan_counts": nan_counts,
        "bad_ranges": bad_ranges,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", type=Path, default=ASSIGNMENT3_DEFAULT_MANIFEST)
    ap.add_argument("--partitions", type=int, default=16)
    args = ap.parse_args()

    spark = get_spark("BEN-A3-Step1-FeaturesV2")
    manifest = spark.read.parquet(str(args.manifest))
    t0 = time.time()
    rdd = manifest.repartition(args.partitions).select(
        "patch_id", "country", "split", "labels", "hdf5_path", "hdf5_index"
    ).rdd.mapPartitions(_partition)
    out = spark.createDataFrame(rdd, schema=FEATURE_V2_SCHEMA)
    uri = features_v2_path()
    out.write.mode("overwrite").partitionBy("country").parquet(uri)
    saved = spark.read.parquet(uri).cache()
    validation = validate_features(saved)
    validation["elapsed_sec"] = round(time.time() - t0, 3)
    report = {"step": "A3-Step1", "mode": "hdf5-mapPartitions", "features_v2_uri": uri, "validation": validation}
    write_json(Path("reports") / "a3_step1_features_v2_validation.json", report)
    write_json(local_hdfs_mirror() / "results" / "a3_step1_features_v2_validation.json", report)
    print(json.dumps(report, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()

