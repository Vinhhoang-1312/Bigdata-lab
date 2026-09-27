"""Full HDF5-derived TIFF-byte packing into Parquet for the selected 52,607 patches.

This does not use original BigEarthNet TIFF files from the 59 GiB tarball. It converts the
selected HDF5 image arrays into in-memory GeoTIFF bytes for B02/B03/B04/B08, then packs
one row per patch into Parquet partitioned by country.
"""
from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path

import h5py
import numpy as np
import rasterio
from rasterio.io import MemoryFile
from pyspark.sql import Row
from pyspark.sql.types import ArrayType, BinaryType, LongType, StringType, StructField, StructType

from scripts.lib.spark_session import get_spark

PACK_SCHEMA = StructType([
    StructField("patch_id", StringType(), False),
    StructField("country", StringType(), True),
    StructField("split", StringType(), True),
    StructField("labels", ArrayType(StringType()), True),
    StructField("b02_tif", BinaryType(), True),
    StructField("b03_tif", BinaryType(), True),
    StructField("b04_tif", BinaryType(), True),
    StructField("b08_tif", BinaryType(), True),
    StructField("dtype", StringType(), False),
    StructField("height", LongType(), False),
    StructField("width", LongType(), False),
    StructField("hdf5_file", StringType(), True),
    StructField("hdf5_index", LongType(), True),
    StructField("error_message", StringType(), True),
])


def _reflectance_to_uint16(arr: np.ndarray) -> np.ndarray:
    arr = np.asarray(arr, dtype=np.float32)
    if float(np.nanmax(arr)) <= 2.0:
        arr = arr * np.float32(10000.0)
    arr = np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)
    return np.clip(np.rint(arr), 0, 65535).astype(np.uint16)


def _to_tiff_bytes(arr: np.ndarray) -> bytes:
    arr = _reflectance_to_uint16(arr)
    height, width = arr.shape
    profile = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": 1,
        "dtype": "uint16",
        "compress": "deflate",
    }
    with MemoryFile() as memfile:
        with memfile.open(**profile) as dataset:
            dataset.write(arr, 1)
        return memfile.read()


def _partition(rows):
    handles = {}
    try:
        for row in rows:
            path = row.hdf5_path
            try:
                if path not in handles:
                    handles[path] = h5py.File(path, "r")
                image = handles[path]["images"][int(row.hdf5_index)]
                # TorchGeo/BigEarthNet order: B01,B02,B03,B04,B05,B06,B07,B08,B8A,B09,B11,B12.
                b02, b03, b04, b08 = (image[i] for i in (1, 2, 3, 7))
                height, width = b02.shape
                yield Row(
                    patch_id=row.patch_id,
                    country=row.country,
                    split=row.split,
                    labels=row.labels,
                    b02_tif=_to_tiff_bytes(b02),
                    b03_tif=_to_tiff_bytes(b03),
                    b04_tif=_to_tiff_bytes(b04),
                    b08_tif=_to_tiff_bytes(b08),
                    dtype="uint16_geotiff_bytes_derived_from_hdf5",
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
                    b02_tif=None,
                    b03_tif=None,
                    b04_tif=None,
                    b08_tif=None,
                    dtype="uint16_geotiff_bytes_derived_from_hdf5",
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
    parser.add_argument("--out", type=Path, default=Path(r"D:\BigEarthNet-partial\packed_tiff_parquet_full"))
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--report", type=Path, default=Path("reports/full_tiff_pack_evidence.json"))
    args = parser.parse_args()

    allowed_root = Path(r"D:\BigEarthNet-partial")
    _safe_rmtree(args.out, allowed_root)
    args.out.parent.mkdir(parents=True, exist_ok=True)

    spark = get_spark("BEN-Full-HDF5-Derived-TIFF-Pack")
    manifest = spark.read.parquet(str(args.manifest)).cache()
    n_in = manifest.count()

    source_paths = [Path(r.hdf5_path) for r in manifest.select("hdf5_path").distinct().collect()]
    source_stats = {p.name: {"path": str(p), "bytes": p.stat().st_size if p.exists() else None} for p in source_paths}

    t0 = time.time()
    rdd = (
        manifest.repartition(args.partitions)
        .select("patch_id", "country", "split", "labels", "hdf5_path", "hdf5_index")
        .rdd.mapPartitions(_partition)
    )
    packed = spark.createDataFrame(rdd, schema=PACK_SCHEMA)
    out_uri = args.out.resolve().as_uri()
    packed.write.mode("overwrite").partitionBy("country").parquet(out_uri)
    elapsed = round(time.time() - t0, 3)

    saved = spark.read.parquet(out_uri).cache()
    n_out = saved.count()
    errors = saved.where("error_message IS NOT NULL").count()
    countries = [r.country for r in saved.select("country").distinct().collect()]
    sample = saved.selectExpr("patch_id", "length(b02_tif) as b02_bytes", "length(b03_tif) as b03_bytes", "length(b04_tif) as b04_bytes", "length(b08_tif) as b08_bytes").orderBy("patch_id").limit(3).collect()
    sample_payload = [row.asDict() for row in sample]
    out_stats = _dir_stats(args.out)

    report = {
        "mode": "full_hdf5_derived_tiff_bytes_to_parquet",
        "honesty_note": "This packs all selected 52,607 HDF5 patches into Parquet as in-memory GeoTIFF bytes for B02/B03/B04/B08. It is not evidence that the original BigEarthNet tarball TIFF files were downloaded and packed.",
        "input_rows": int(n_in),
        "output_rows": int(n_out),
        "errors": int(errors),
        "countries": sorted(countries),
        "partitions": args.partitions,
        "elapsed_seconds": elapsed,
        "output_path": str(args.out),
        "output_uri": out_uri,
        "source_hdf5_stats": source_stats,
        "packed_parquet_stats": out_stats,
        "sample_tiff_byte_lengths": sample_payload,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()