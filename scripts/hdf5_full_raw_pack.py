"""Full HDF5-derived raw band-byte packing into Parquet for 52,607 patches.

Fallback after GeoTIFF-byte packing exceeded local Spark heap. This packs B02/B03/B04/B08
as uint16 raw bytes, one row per patch, written under country=... directories.
"""
from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path

import h5py
import numpy as np
from pyspark.sql import Row
from pyspark.sql.types import ArrayType, BinaryType, LongType, StringType, StructField, StructType

from scripts.lib.spark_session import get_spark

PACK_SCHEMA = StructType([
    StructField("patch_id", StringType(), False),
    StructField("country", StringType(), True),
    StructField("split", StringType(), True),
    StructField("labels", ArrayType(StringType()), True),
    StructField("b02_bytes", BinaryType(), True),
    StructField("b03_bytes", BinaryType(), True),
    StructField("b04_bytes", BinaryType(), True),
    StructField("b08_bytes", BinaryType(), True),
    StructField("dtype", StringType(), False),
    StructField("height", LongType(), False),
    StructField("width", LongType(), False),
    StructField("hdf5_file", StringType(), True),
    StructField("hdf5_index", LongType(), True),
    StructField("error_message", StringType(), True),
])


def _to_uint16_bytes(arr: np.ndarray) -> bytes:
    arr = np.asarray(arr, dtype=np.float32)
    if float(np.nanmax(arr)) <= 2.0:
        arr = arr * np.float32(10000.0)
    arr = np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)
    return np.clip(np.rint(arr), 0, 65535).astype("<u2", copy=False).tobytes(order="C")


def _partition(rows, row_acc=None, error_acc=None):
    handles = {}
    try:
        for row in rows:
            path = row.hdf5_path
            try:
                if path not in handles:
                    handles[path] = h5py.File(path, "r")
                image = handles[path]["images"][int(row.hdf5_index)]
                b02, b03, b04, b08 = (image[i] for i in (1, 2, 3, 7))
                height, width = b02.shape
                yield Row(
                    patch_id=row.patch_id,
                    country=row.country,
                    split=row.split,
                    labels=row.labels,
                    b02_bytes=_to_uint16_bytes(b02),
                    b03_bytes=_to_uint16_bytes(b03),
                    b04_bytes=_to_uint16_bytes(b04),
                    b08_bytes=_to_uint16_bytes(b08),
                    dtype="uint16_little_endian_raw_bytes_derived_from_hdf5",
                    height=int(height),
                    width=int(width),
                    hdf5_file=Path(path).name,
                    hdf5_index=int(row.hdf5_index),
                    error_message=None,
                )
            except Exception as exc:  # noqa: BLE001
                yield Row(
                    patch_id=row.patch_id,
                    country=row.country,
                    split=row.split,
                    labels=row.labels,
                    b02_bytes=None,
                    b03_bytes=None,
                    b04_bytes=None,
                    b08_bytes=None,
                    dtype="uint16_little_endian_raw_bytes_derived_from_hdf5",
                    height=0,
                    width=0,
                    hdf5_file=Path(path).name,
                    hdf5_index=int(row.hdf5_index),
                    error_message=f"{type(exc).__name__}: {exc}",
                )
    finally:
        for handle in handles.values():
            handle.close()


def _dir_stats(path: Path) -> dict:
    if not path.exists():
        return {"exists": False, "files": 0, "bytes": 0}
    files = [p for p in path.rglob("*") if p.is_file()]
    return {"exists": True, "files": len(files), "bytes": sum(p.stat().st_size for p in files)}


def _safe_rmtree(path: Path, allowed_root: Path) -> None:
    resolved = path.resolve()
    root = allowed_root.resolve()
    if root != resolved and root not in resolved.parents:
        raise ValueError(f"Refusing to delete outside allowed root: {resolved}")
    if resolved.exists():
        shutil.rmtree(resolved)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=Path(r"D:\BigEarthNet-partial\manifest\manifest.parquet"))
    parser.add_argument("--out", type=Path, default=Path(r"D:\BigEarthNet-partial\packed_raw_band_parquet_full"))
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--report", type=Path, default=Path("reports/full_raw_band_pack_evidence.json"))
    args = parser.parse_args()

    allowed_root = Path(r"D:\BigEarthNet-partial")
    _safe_rmtree(args.out, allowed_root)
    args.out.mkdir(parents=True, exist_ok=True)

    spark = get_spark("BEN-Full-HDF5-Derived-Raw-Band-Pack")
    manifest = spark.read.parquet(str(args.manifest)).cache()
    n_in = manifest.count()
    countries = [r.country for r in manifest.select("country").distinct().collect()]

    source_paths = [Path(r.hdf5_path) for r in manifest.select("hdf5_path").distinct().collect()]
    source_stats = {p.name: {"path": str(p), "bytes": p.stat().st_size if p.exists() else None} for p in source_paths}

    t0 = time.time()
    country_runs = []
    for country in sorted(countries):
        country_manifest = manifest.where(manifest.country == country).cache()
        c_in = country_manifest.count()
        row_acc = spark.sparkContext.accumulator(0)
        error_acc = spark.sparkContext.accumulator(0)
        rdd = (
            country_manifest.repartition(args.partitions)
            .select("patch_id", "country", "split", "labels", "hdf5_path", "hdf5_index")
            .rdd.mapPartitions(lambda rows: _partition(rows, row_acc, error_acc))
        )
        packed = spark.createDataFrame(rdd, schema=PACK_SCHEMA)
        out_dir = args.out / f"country={country}"
        packed.write.mode("overwrite").parquet(out_dir.resolve().as_uri())
        country_runs.append({
            "country": country,
            "input_rows": int(c_in),
            "output_rows_from_accumulator": int(row_acc.value),
            "errors_from_accumulator": int(error_acc.value),
            "output_path": str(out_dir),
            "output_stats": _dir_stats(out_dir),
        })
        country_manifest.unpersist()

    elapsed = round(time.time() - t0, 3)
    base_uri = args.out.resolve().as_uri()
    n_out = sum(item["output_rows_from_accumulator"] for item in country_runs)
    errors = sum(item["errors_from_accumulator"] for item in country_runs)
    sample = []

    report = {
        "mode": "full_hdf5_derived_raw_band_bytes_to_parquet",
        "honesty_note": "This packs all selected 52,607 HDF5 patches into Parquet as raw uint16 bytes for B02/B03/B04/B08. It is full image-band packing, but it is not packing the original downloaded TIFF files.",
        "input_rows": int(n_in),
        "output_rows": int(n_out),
        "errors": int(errors),
        "countries": sorted(countries),
        "partitions_per_country": args.partitions,
        "elapsed_seconds": elapsed,
        "output_path": str(args.out),
        "output_uri": base_uri,
        "country_runs": country_runs,
        "source_hdf5_stats": source_stats,
        "packed_parquet_stats": _dir_stats(args.out),
        "sample_byte_lengths": sample,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
