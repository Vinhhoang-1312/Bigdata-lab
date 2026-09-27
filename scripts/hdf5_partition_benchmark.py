"""Benchmark the HDF5-direct Step 2 feature job with multiple partition counts.

Writes separate Parquet outputs under data/benchmark so production features are not overwritten.
"""
from __future__ import annotations

import argparse
import json
import shutil
import time
import urllib.request
from pathlib import Path

from pyspark.sql import functions as F

from scripts.hdf5_features import FEATURE_SCHEMA, _partition
from scripts.lib.constants import FEATURE_COLS
from scripts.lib.spark_session import get_spark


def _safe_rmtree(path: Path, root: Path) -> None:
    resolved = path.resolve()
    root_resolved = root.resolve()
    if root_resolved not in resolved.parents and resolved != root_resolved:
        raise ValueError(f"Refusing to remove path outside benchmark root: {resolved}")
    if resolved.exists():
        shutil.rmtree(resolved)


def _fetch_json(url: str):
    try:
        with urllib.request.urlopen(url, timeout=5) as response:  # noqa: S310 - local Spark UI only
            return json.loads(response.read().decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}"}


def _collect_group_status(spark, group_id: str) -> list[dict]:
    tracker = spark.sparkContext.statusTracker()
    jobs = []
    for job_id in tracker.getJobIdsForGroup(group_id):
        info = tracker.getJobInfo(job_id)
        if info is None:
            jobs.append({"job_id": int(job_id), "available": False})
            continue
        stages = []
        for stage_id in info.stageIds:
            stage = tracker.getStageInfo(stage_id)
            if stage is None:
                stages.append({"stage_id": int(stage_id), "available": False})
            else:
                stages.append({
                    "stage_id": int(stage_id),
                    "name": stage.name,
                    "num_tasks": int(stage.numTasks),
                    "num_active_tasks": int(stage.numActiveTasks),
                    "num_completed_tasks": int(stage.numCompletedTasks),
                    "num_failed_tasks": int(stage.numFailedTasks),
                })
        jobs.append({"job_id": int(job_id), "status": str(info.status), "stage_ids": [int(x) for x in info.stageIds], "stages": stages})
    return sorted(jobs, key=lambda item: item["job_id"])


def _run_once(spark, manifest, partitions: int, benchmark_root: Path) -> dict:
    out_dir = benchmark_root / f"features_p{partitions}"
    _safe_rmtree(out_dir, benchmark_root)
    out_dir.parent.mkdir(parents=True, exist_ok=True)
    out_uri = out_dir.resolve().as_uri()

    group_id = f"hdf5_feature_partitions_{partitions}"
    spark.sparkContext.setJobGroup(group_id, f"HDF5 feature benchmark with {partitions} partitions")
    n_in = manifest.count()
    rdd = (
        manifest.repartition(partitions)
        .select("patch_id", "country", "split", "labels", "hdf5_path", "hdf5_index")
        .rdd.mapPartitions(_partition)
    )
    t0 = time.time()
    out = spark.createDataFrame(rdd, schema=FEATURE_SCHEMA)
    out.write.mode("overwrite").partitionBy("country").parquet(out_uri)
    elapsed = round(time.time() - t0, 3)

    saved = spark.read.parquet(out_uri).cache()
    n_out = saved.count()
    errors = saved.filter(F.col("error_flag")).count()
    bad_ndvi = saved.filter((F.col("ndvi") < -1.001) | (F.col("ndvi") > 1.001)).count()
    bad_ndwi = saved.filter((F.col("ndwi") < -1.001) | (F.col("ndwi") > 1.001)).count()
    sample_ids = [row.patch_id for row in saved.select("patch_id").orderBy("patch_id").limit(3).collect()]
    job_status = _collect_group_status(spark, group_id)
    spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
    spark.sparkContext.setLocalProperty("spark.job.description", None)
    return {
        "partitions": partitions,
        "elapsed_seconds": elapsed,
        "input_rows": int(n_in),
        "output_rows": int(n_out),
        "errors": int(errors),
        "bad_ndvi_count": int(bad_ndvi),
        "bad_ndwi_count": int(bad_ndwi),
        "output_path": str(out_dir),
        "output_uri": out_uri,
        "sample_patch_ids": sample_ids,
        "spark_job_group": group_id,
        "spark_jobs": job_status,
    }


def _compare_outputs(spark, left_uri: str, right_uri: str) -> dict:
    left = spark.read.parquet(left_uri).select("patch_id", *[F.col(c).alias(f"l_{c}") for c in FEATURE_COLS])
    right = spark.read.parquet(right_uri).select("patch_id", *[F.col(c).alias(f"r_{c}") for c in FEATURE_COLS])
    joined = left.join(right, "patch_id", "inner")
    exprs = [F.max(F.abs(F.col(f"l_{c}") - F.col(f"r_{c}"))).alias(c) for c in FEATURE_COLS]
    row = joined.select(*exprs).collect()[0]
    max_abs_by_feature = {c: float(row[c] or 0.0) for c in FEATURE_COLS}
    return {
        "joined_rows": int(joined.count()),
        "max_abs_diff_by_feature": max_abs_by_feature,
        "max_abs_diff_overall": max(max_abs_by_feature.values()) if max_abs_by_feature else 0.0,
        "equivalent_with_tolerance_1e_9": all(v <= 1e-9 for v in max_abs_by_feature.values()),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=Path(r"D:\BigEarthNet-partial\manifest\manifest.parquet"))
    parser.add_argument("--partitions", type=int, nargs="+", default=[4, 8])
    parser.add_argument("--benchmark-root", type=Path, default=Path("data/benchmark"))
    parser.add_argument("--report", type=Path, default=Path("reports/step2_partition_benchmark.json"))
    parser.add_argument("--spark-ui-report", type=Path, default=Path("reports/spark_ui_evidence.json"))
    args = parser.parse_args()

    spark = get_spark("BEN-HDF5-Partition-Benchmark")
    spark_ui_url = spark.sparkContext.uiWebUrl
    manifest = spark.read.parquet(str(args.manifest)).cache()
    dataset_rows = manifest.count()

    runs = []
    for p in args.partitions:
        runs.append(_run_once(spark, manifest, p, args.benchmark_root))

    comparison = {}
    if len(runs) >= 2:
        fastest = min(runs, key=lambda r: r["elapsed_seconds"])
        slowest = max(runs, key=lambda r: r["elapsed_seconds"])
        reduction = 0.0
        if slowest["elapsed_seconds"]:
            reduction = round((slowest["elapsed_seconds"] - fastest["elapsed_seconds"]) / slowest["elapsed_seconds"] * 100.0, 3)
        comparison = {
            "faster_setting": fastest["partitions"],
            "slowest_setting": slowest["partitions"],
            "runtime_reduction_percent_vs_slowest": reduction,
            "output_equivalence": _compare_outputs(spark, runs[0]["output_uri"], runs[1]["output_uri"]),
        }

    ui_payload = {
        "spark_ui_url_during_run": spark_ui_url,
        "screenshot_created": False,
        "screenshot_path": None,
        "reason_no_screenshot": "No browser/screenshot automation is available in this terminal session. Spark statusTracker job/stage/task evidence was captured while the application was alive.",
        "manual_screenshot_instructions": [
            "Run the benchmark command shown in reports/step2_partition_benchmark.json if a fresh UI is needed.",
            "Open the Spark UI URL printed in spark_ui_url_during_run, usually http://localhost:4040.",
            "Capture the Jobs tab and Stages tab for the BEN-HDF5-Partition-Benchmark application.",
            "Ensure the screenshot shows completed jobs/stages, task counts, executor activity, and durations.",
        ],
    }
    if spark_ui_url:
        api = spark_ui_url.rstrip("/") + "/api/v1"
        ui_payload["applications"] = _fetch_json(api + "/applications")
        apps = ui_payload.get("applications")
        if isinstance(apps, list) and apps:
            app_id = apps[0].get("id")
            ui_payload["selected_application_id"] = app_id
            ui_payload["jobs"] = _fetch_json(f"{api}/applications/{app_id}/jobs")
            ui_payload["stages"] = _fetch_json(f"{api}/applications/{app_id}/stages")
    else:
        ui_payload["applications"] = {"error": "Spark UI URL was not available"}

    report = {
        "dataset_rows": int(dataset_rows),
        "manifest": str(args.manifest),
        "partitions_tested": args.partitions,
        "command": "python scripts/hdf5_partition_benchmark.py --partitions " + " ".join(map(str, args.partitions)),
        "runs": runs,
        "comparison": comparison,
        "spark_ui_evidence_file": str(args.spark_ui_report),
    }

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")
    args.spark_ui_report.write_text(json.dumps(ui_payload, indent=2), encoding="utf-8")
    print(json.dumps({"report": str(args.report), "ui_report": str(args.spark_ui_report), "runs": runs, "comparison": comparison}, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()

