"""Step 2 feature engineering directly from HDF5 arrays on Spark executors."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import h5py
import numpy as np
from pyspark.sql import Row, functions as F
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, StringType, StructField, StructType, LongType

from scripts.lib.constants import FEATURE_COLS
from scripts.lib.features_core import compute_from_arrays
from scripts.lib.io_stats import write_json
from scripts.lib.paths import features_path, local_hdfs_mirror
from scripts.lib.spark_session import get_spark


FEATURE_SCHEMA = StructType([
    StructField("patch_id", StringType(), False),
    StructField("country", StringType(), True),
    StructField("split", StringType(), True),
    StructField("labels", ArrayType(StringType()), True),
    *[StructField(c, DoubleType(), True) for c in FEATURE_COLS],
    StructField("error_flag", BooleanType(), False),
    StructField("error_message", StringType(), True),
    StructField("hdf5_file", StringType(), True),
    StructField("hdf5_index", LongType(), True),
])


def _reflectance(arr: np.ndarray) -> np.ndarray:
    # HDF5 mirrors differ: some store Sentinel-2 DN, others normalized float32.
    arr = np.asarray(arr, dtype=np.float32)
    return arr / np.float32(1e4) if float(np.nanmax(arr)) > 2.0 else arr


def compute_hdf5_row(*, patch_id, country, split, labels, hdf5_path, hdf5_index):
    """Read one row; this function is also used by unit tests."""
    try:
        with h5py.File(hdf5_path, "r") as h5:
            image = h5["images"][int(hdf5_index)]
        # TorchGeo/BigEarthNet order: B01,B02,B03,B04,B05,B06,B07,B08,B8A,B09,B11,B12.
        b02, b03, b04, b08 = (_reflectance(image[i]) for i in (1, 2, 3, 7))
        feats = compute_from_arrays(b02 * 1e4, b03 * 1e4, b04 * 1e4, b08 * 1e4)
        return {"patch_id": patch_id, "country": country, "split": split, "labels": labels,
                **{c: feats[c] for c in FEATURE_COLS}, "error_flag": False,
                "error_message": None, "hdf5_file": Path(hdf5_path).name,
                "hdf5_index": int(hdf5_index)}
    except Exception as exc:  # noqa: BLE001
        return {"patch_id": patch_id, "country": country, "split": split, "labels": labels,
                **{c: None for c in FEATURE_COLS}, "error_flag": True,
                "error_message": f"{type(exc).__name__}: {exc}",
                "hdf5_file": Path(hdf5_path).name, "hdf5_index": int(hdf5_index)}


def _partition(rows):
    handles = {}
    try:
        for row in rows:
            path = row.hdf5_path
            if path not in handles:
                handles[path] = h5py.File(path, "r")
            try:
                image = handles[path]["images"][int(row.hdf5_index)]
                bands = [_reflectance(image[i]) for i in (1, 2, 3, 7)]
                feats = compute_from_arrays(*(b * 1e4 for b in bands))
                yield Row(patch_id=row.patch_id, country=row.country, split=row.split, labels=row.labels,
                          **{c: feats[c] for c in FEATURE_COLS}, error_flag=False,
                          error_message=None, hdf5_file=Path(path).name, hdf5_index=int(row.hdf5_index))
            except Exception as exc:  # noqa: BLE001
                yield Row(patch_id=row.patch_id, country=row.country, split=row.split, labels=row.labels,
                          **{c: None for c in FEATURE_COLS}, error_flag=True,
                          error_message=f"{type(exc).__name__}: {exc}", hdf5_file=Path(path).name,
                          hdf5_index=int(row.hdf5_index))
    finally:
        for handle in handles.values():
            handle.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", type=Path, default=Path(r"D:\BigEarthNet-partial\manifest\manifest.parquet"))
    ap.add_argument("--partitions", type=int, default=16)
    args = ap.parse_args()
    spark = get_spark("BEN-HDF5-Step2")
    manifest = spark.read.parquet(str(args.manifest))
    n_in = manifest.count()
    rdd = manifest.repartition(args.partitions).select(
        "patch_id", "country", "split", "labels", "hdf5_path", "hdf5_index"
    ).rdd.mapPartitions(_partition)
    t0 = time.time()
    out = spark.createDataFrame(rdd, schema=FEATURE_SCHEMA)
    uri = features_path()
    out.write.mode("overwrite").partitionBy("country").parquet(uri)
    saved = spark.read.parquet(uri)
    sample_rows = manifest.orderBy("patch_id").limit(3).collect()
    checks = []
    for sample in sample_rows:
        expected = compute_hdf5_row(
            patch_id=sample.patch_id, country=sample.country, split=sample.split,
            labels=sample.labels, hdf5_path=sample.hdf5_path, hdf5_index=sample.hdf5_index,
        )
        actual = saved.filter(F.col("patch_id") == sample.patch_id).select(*FEATURE_COLS).collect()[0]
        diffs = {c: abs(float(actual[c]) - float(expected[c])) for c in FEATURE_COLS}
        checks.append({"patch_id": sample.patch_id, "max_abs_diff": max(diffs.values()),
                       "match": max(diffs.values()) < 1e-5})
    validation = {
        "n_in": n_in,
        "n_out": saved.count(),
        "n_error": saved.filter(F.col("error_flag")).count(),
        "n_countries": saved.select("country").distinct().count(),
        "n_splits": saved.select("split").distinct().count(),
        "numpy_checks": checks,
        "bad_ndvi_count": saved.filter((F.col("ndvi") < -1.001) | (F.col("ndvi") > 1.001)).count(),
        "bad_ndwi_count": saved.filter((F.col("ndwi") < -1.001) | (F.col("ndwi") > 1.001)).count(),
        "elapsed_sec": round(time.time() - t0, 3),
    }
    report = {"step": 2, "mode": "hdf5-direct-mapPartitions", "features_uri": uri, "validation": validation}
    write_json(Path("reports") / "hdf5_step2_validation.json", report)
    write_json(local_hdfs_mirror() / "results" / "hdf5_step2_validation.json", report)
    print(json.dumps(report, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
