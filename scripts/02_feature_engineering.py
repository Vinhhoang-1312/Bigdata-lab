#!/usr/bin/env python3
"""Step 2 — Distributed feature engineering on executors (mapPartitions)."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    ArrayType,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.lib.constants import FEATURE_COLS
from scripts.lib.features_core import compute_from_arrays, compute_from_bytes, decode_band_bytes
from scripts.lib.io_stats import write_json
from scripts.lib.paths import features_path, local_hdfs_mirror, packed_path, use_local
from scripts.lib.spark_session import get_spark
from scripts.feature_validation import describe_rows_to_dict


FEATURE_SCHEMA = StructType(
    [
        StructField("patch_id", StringType(), False),
        StructField("country", StringType(), True),
        StructField("split", StringType(), True),
        StructField("labels", ArrayType(StringType()), True),
        *[StructField(c, DoubleType(), True) for c in FEATURE_COLS],
        StructField("error_flag", BooleanType(), False),
        StructField("error_message", StringType(), True),
    ]
)


def _partition_fn(rows):
    for row in rows:
        feats = compute_from_bytes(row.b02, row.b03, row.b04, row.b08)
        yield Row(
            patch_id=row.patch_id,
            country=row.country,
            split=row.split,
            labels=row.labels,
            mean_b02=feats.get("mean_b02"),
            mean_b03=feats.get("mean_b03"),
            mean_b04=feats.get("mean_b04"),
            mean_b08=feats.get("mean_b08"),
            std_b02=feats.get("std_b02"),
            std_b03=feats.get("std_b03"),
            std_b04=feats.get("std_b04"),
            std_b08=feats.get("std_b08"),
            ndvi=feats.get("ndvi"),
            ndwi=feats.get("ndwi"),
            brightness=feats.get("brightness"),
            error_flag=bool(feats.get("error_flag")),
            error_message=feats.get("error_message"),
        )


def run_once(spark, packed_uri: str, out_uri: str, n_partitions: int | None) -> tuple[object, float]:
    packed = spark.read.parquet(packed_uri)
    n_in = packed.count()
    rdd = packed.select(
        "patch_id", "country", "split", "labels", "b02", "b03", "b04", "b08"
    ).rdd
    if n_partitions:
        rdd = rdd.repartition(n_partitions)
    t0 = time.time()
    feat_rdd = rdd.mapPartitions(_partition_fn)
    feat_df = spark.createDataFrame(feat_rdd, schema=FEATURE_SCHEMA)
    (
        feat_df.repartition("country")
        .write.mode("overwrite")
        .partitionBy("country")
        .parquet(out_uri)
    )
    elapsed = time.time() - t0
    out = spark.read.parquet(out_uri)
    return out, n_in, elapsed


def validate(spark, packed_uri: str, feat_df, n_in: int) -> dict:
    n_out = feat_df.count()
    err_n = feat_df.filter(F.col("error_flag")).count()
    ok = feat_df.filter(~F.col("error_flag"))

    null_stats = {}
    for c in FEATURE_COLS:
        null_stats[c] = {
            "nulls": ok.filter(F.col(c).isNull()).count(),
            "nan": ok.filter(F.isnan(F.col(c))).count(),
            "pos_inf": ok.filter(F.col(c) == float("inf")).count(),
            "neg_inf": ok.filter(F.col(c) == float("-inf")).count(),
        }

    desc_rows = [row.asDict() for row in ok.select(*FEATURE_COLS).describe().collect()]
    desc = describe_rows_to_dict(desc_rows)

    # NDVI/NDWI range check
    bad_ndvi = ok.filter((F.col("ndvi") < -1.001) | (F.col("ndvi") > 1.001)).count()
    bad_ndwi = ok.filter((F.col("ndwi") < -1.001) | (F.col("ndwi") > 1.001)).count()

    # NumPy recomputation on 3 random OK patches (driver loads ONLY these 3)
    sample_ids = [r.patch_id for r in ok.orderBy(F.rand(42)).limit(3).collect()]
    packed = spark.read.parquet(packed_uri)
    samples = packed.filter(F.col("patch_id").isin(sample_ids)).collect()
    spark_map = {
        r.patch_id: r
        for r in ok.filter(F.col("patch_id").isin(sample_ids)).collect()
    }
    numpy_checks = []
    for row in samples:
        arrays = [decode_band_bytes(getattr(row, b)) for b in ("b02", "b03", "b04", "b08")]
        np_feats = compute_from_arrays(*arrays)
        sp = spark_map[row.patch_id]
        diffs = {
            c: abs(float(getattr(sp, c)) - float(np_feats[c]))
            for c in FEATURE_COLS
        }
        numpy_checks.append(
            {
                "patch_id": row.patch_id,
                "max_abs_diff": max(diffs.values()),
                "diffs": diffs,
                "match": max(diffs.values()) < 1e-5,
            }
        )

    return {
        "n_in": n_in,
        "n_out": n_out,
        "n_error": err_n,
        "null_nan_inf": null_stats,
        "describe": desc,
        "bad_ndvi_count": bad_ndvi,
        "bad_ndwi_count": bad_ndwi,
        "numpy_checks": numpy_checks,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--partitions-a", type=int, default=8)
    ap.add_argument("--partitions-b", type=int, default=32)
    ap.add_argument("--skip-benchmark", action="store_true")
    args = ap.parse_args()

    spark = get_spark("BEN-Step2-Features")
    packed_uri = packed_path()
    out_uri = features_path()

    # Primary run (partitions-b)
    feat_df, n_in, elapsed_b = run_once(spark, packed_uri, out_uri, args.partitions_b)
    validation = validate(spark, packed_uri, feat_df, n_in)

    bench = {"partitions_b": args.partitions_b, "elapsed_b_sec": round(elapsed_b, 3)}
    if not args.skip_benchmark:
        # Second timing run to a temp path (do not clobber final features)
        tmp = out_uri.rstrip("/") + "_bench_a"
        _, _, elapsed_a = run_once(spark, packed_uri, tmp, args.partitions_a)
        bench.update({"partitions_a": args.partitions_a, "elapsed_a_sec": round(elapsed_a, 3)})
        # rewrite final with partitions_b already done above — re-run final to be safe
        feat_df, n_in, elapsed_b2 = run_once(spark, packed_uri, out_uri, args.partitions_b)
        bench["elapsed_b_rerun_sec"] = round(elapsed_b2, 3)
        validation = validate(spark, packed_uri, feat_df, n_in)

    report = {
        "step": 2,
        "packed_uri": packed_uri,
        "features_uri": out_uri,
        "benchmark": bench,
        "validation": validation,
        "spark_ui_note": "Capture Spark UI (Jobs / Stages) screenshots for both partition settings.",
    }
    write_json(Path("reports") / "step2_validation.json", report)
    write_json(local_hdfs_mirror() / "results" / "step2_validation.json", report)
    print(json.dumps(report, indent=2, default=str))

    if not all(c["match"] for c in validation["numpy_checks"]):
        print("WARNING: NumPy vs Spark mismatch on sample patches", file=sys.stderr)
    spark.stop()


if __name__ == "__main__":
    main()
