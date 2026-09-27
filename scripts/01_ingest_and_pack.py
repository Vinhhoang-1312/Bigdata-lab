#!/usr/bin/env python3
"""Step 1 — Pack selected 10 m bands into Parquet (partitioned by country)."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.lib.constants import BANDS_10M
from scripts.lib.dataset_layout import band_filename, discover_dataset, tile_id_from_patch_id
from scripts.lib.io_stats import dir_file_stats, try_hdfs_count, write_json
from scripts.lib.paths import (
    local_hdfs_mirror,
    metadata_store_path,
    packed_path,
    results_path,
    spark_uri,
    subset_dir,
    use_local,
)
from scripts.lib.spark_session import get_spark
from scripts.ingest_helpers import metadata_records


def stage_raw_local(images_root: Path, patch_ids: list[str], dest: Path) -> dict:
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True, exist_ok=True)
    n_copied = 0
    missing = 0
    for patch_id in patch_ids:
        tile = tile_id_from_patch_id(patch_id)
        for band in BANDS_10M:
            src = images_root / tile / patch_id / band_filename(patch_id, band)
            if not src.is_file():
                missing += 1
                continue
            out = dest / tile / patch_id / src.name
            out.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, out)
            n_copied += 1
    return {
        "n_patch_ids": len(patch_ids),
        "n_files_copied": n_copied,
        "n_missing_band_files": missing,
        "dest": str(dest),
    }


def extract_patch_id():
    # path .../<TILE>/<PATCH_ID>/<PATCH_ID>_Bxx.tif → capture PATCH_ID folder
    return F.regexp_extract(F.col("path"), r"/([^/]+)/[^/]+$", 1)


def pack_bands(spark, images_uri: str, meta_df, out_uri: str):
    band_dfs = []
    for band in BANDS_10M:
        bdf = (
            spark.read.format("binaryFile")
            .option("pathGlobFilter", f"*_{band}.tif")
            .option("recursiveFileLookup", "true")
            .load(images_uri)
            .withColumn("patch_id", extract_patch_id())
            .select(F.col("patch_id"), F.col("content").alias(band.lower()))
        )
        band_dfs.append(bdf)

    joined = band_dfs[0]
    for bdf in band_dfs[1:]:
        joined = joined.join(bdf, on="patch_id", how="inner")

    packed = meta_df.join(joined, on="patch_id", how="inner").select(
        "patch_id", "country", "split", "labels", "b02", "b03", "b04", "b08"
    )
    (
        packed.repartition("country")
        .write.mode("overwrite")
        .partitionBy("country")
        .parquet(out_uri)
    )
    return spark.read.parquet(out_uri)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-root", default=None)
    ap.add_argument("--subset-meta", default=None)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument(
        "--skip-stage-raw",
        action="store_true",
        help="Read TIFFs directly from drop-in images_root (no copy into raw/)",
    )
    args = ap.parse_args()

    ds = discover_dataset(args.data_root)
    subset_meta = Path(args.subset_meta) if args.subset_meta else subset_dir() / "subset_metadata.parquet"
    if not subset_meta.is_file():
        raise FileNotFoundError(f"Run 00_build_subset.py first. Missing {subset_meta}")

    pdf = pd.read_parquet(subset_meta)
    if args.limit:
        pdf = pdf.head(args.limit).copy()
    patch_ids = pdf["patch_id"].astype(str).tolist()
    print(f"Step1 patches planned: {len(patch_ids)}")
    print(ds.describe())

    spark = get_spark("BEN-Step1-Pack")
    t0 = time.time()

    staging_info: dict = {}
    if use_local() and not args.skip_stage_raw:
        raw_dir = local_hdfs_mirror() / "raw"
        staging_info = stage_raw_local(ds.images_root, patch_ids, raw_dir)
        images_uri = spark_uri(raw_dir)
        before = dir_file_stats(raw_dir)
    else:
        images_uri = spark_uri(ds.images_root)
        before = dir_file_stats(ds.images_root) if use_local() else {"path": images_uri}
        h = try_hdfs_count(images_uri)
        if h:
            before["hdfs"] = h

    meta_schema = StructType(
        [
            StructField("patch_id", StringType(), False),
            StructField("country", StringType(), True),
            StructField("split", StringType(), True),
            StructField("labels", ArrayType(StringType()), True),
        ]
    )
    meta_df = spark.createDataFrame(metadata_records(pdf), schema=meta_schema)
    meta_df.write.mode("overwrite").parquet(f"{metadata_store_path().rstrip('/')}/subset_metadata")

    out_uri = packed_path()
    packed = pack_bands(spark, images_uri, meta_df, out_uri)
    n_out = packed.count()
    per_country = {
        r["country"]: r["count"]
        for r in packed.groupBy("country").count().collect()
    }

    if use_local():
        after = dir_file_stats(local_hdfs_mirror() / "packed")
    else:
        after = try_hdfs_count(out_uri) or {"path": out_uri}

    stats = {
        "step": 1,
        "n_patches_in_subset": len(patch_ids),
        "n_patches_packed": n_out,
        "per_country": per_country,
        "before_pack": before,
        "after_pack": after,
        "staging": staging_info,
        "images_uri": images_uri,
        "packed_uri": out_uri,
        "elapsed_sec": round(time.time() - t0, 3),
        "hdfs_evidence_cmds": [
            "hdfs dfs -count /bigearthnet/raw /bigearthnet/packed",
            "hdfs dfs -du -s -h /bigearthnet/raw /bigearthnet/packed",
            "hdfs dfs -ls /bigearthnet/packed",
            "hdfs getconf -confKey dfs.blocksize",
            "hdfs getconf -confKey dfs.replication",
        ],
    }
    write_json(Path("reports") / "step1_storage_stats.json", stats)
    write_json(local_hdfs_mirror() / "results" / "step1_storage_stats.json", stats)
    # touch results URI for cluster layout consistency
    print(json.dumps(stats, indent=2, default=str))
    if n_out < 50_000 and not args.limit:
        print(f"WARNING: packed {n_out} < 50000 required for full grade.", file=sys.stderr)
    spark.stop()


if __name__ == "__main__":
    main()
