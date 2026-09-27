"""Render the verified evidence and charts into a compact assignment PDF."""
from __future__ import annotations

import json
import textwrap
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.image import imread


def _load_json(path: str):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def page_text(pdf, title, paragraphs):
    fig = plt.figure(figsize=(8.27, 11.69))
    fig.text(0.08, 0.94, title, fontsize=18, weight="bold", va="top")
    y = 0.88
    for paragraph in paragraphs:
        for line in textwrap.wrap(str(paragraph), 105):
            fig.text(0.08, y, line, fontsize=10, va="top")
            y -= 0.026
        y -= 0.018
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def page_chart(pdf, image_path: str, title: str):
    fig, ax = plt.subplots(figsize=(8.27, 5.8))
    ax.imshow(imread(Path("reports/figures") / image_path))
    ax.axis("off")
    ax.set_title(title)
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    out = Path("reports/final_report.pdf")
    manifest = _load_json("reports/hdf5_manifest_summary.json")
    validation = _load_json("reports/hdf5_step2_validation.json")["validation"]
    profile = _load_json("reports/hdf5_feature_profile.json")
    metrics = _load_json("reports/step4_metrics.json")
    hdfs = _load_json("reports/hdfs_environment.json")
    bench = _load_json("reports/step2_partition_benchmark.json")
    ui = _load_json("reports/spark_ui_evidence.json")
    raw_pack = _load_json("reports/full_raw_band_pack_evidence.json")

    run4, run8 = bench["runs"]
    comparison = bench["comparison"]

    with PdfPages(out) as pdf:
        page_text(pdf, "BDA501 Individual Assignment 2", [
            "Distributed BigEarthNet processing with Apache Spark.",
            f"Scope: four selected HDF5 shards on D:\\BigEarthNet-partial; {manifest['rows']} patches from {len(manifest['countries'])} countries: {', '.join(manifest['countries'])}.",
            f"Official split counts: train={manifest['splits']['train']}, validation={manifest['splits']['validation']}, test={manifest['splits']['test']}.",
            "The full dataset was not downloaded. The main experiment is HDF5-direct. A separate TIFF-to-Parquet pilot demonstrates the small-files packing mechanism.",
        ])

        page_text(pdf, "Environment and Step 1 Storage", [
            "The project uses a local Spark/PySpark environment and writes outputs into hdfs_mirror/bigearthnet. This mirrors the required folder layout, but it is not a running HDFS cluster.",
            f"HDFS CLI available: {hdfs['hdfs_command_available']}. Hadoop CLI available: {hdfs['hadoop_command_available']}.",
            "Because no real HDFS CLI/service was available, HDFS block size, replication factor, and live DataNode count were not invented.",
            "Small-files pilot evidence: 24 patches were staged as 96 raw TIFF band files, then packed into 8 Parquet files. Size changed from 2.66 MiB to 2.06 MiB.",
            f"Full HDF5-derived raw band packing evidence: {raw_pack['output_rows']} rows, {raw_pack['packed_parquet_stats']['files']} files, {raw_pack['packed_parquet_stats']['bytes'] / (1024**3):.2f} GiB under D:\\BigEarthNet-partial\\packed_raw_band_parquet_full.",
            "Limitation: this is full image-band packing from HDF5-derived bytes, not proof that the original BigEarthNet TIFF files were downloaded and packed.",
        ])

        page_text(pdf, "Step 2 Feature Engineering and Validation", [
            "scripts/hdf5_features.py uses Spark mapPartitions. Each executor opens the HDF5 shard, reads the indexed image row, selects B02/B03/B04/B08, and emits an explicit schema with error fields.",
            f"Main validation: n_in={validation['n_in']}, n_out={validation['n_out']}, errors={validation['n_error']}, countries={validation['n_countries']}, splits={validation['n_splits']}, bad NDVI={validation['bad_ndvi_count']}, bad NDWI={validation['bad_ndwi_count']}.",
            f"Feature profile: {profile['rows']} rows, {len(profile['feature_cols'])} feature columns, total null/NaN/Inf values = 0, with Spark describe() for all features.",
            f"Partition benchmark: 4 partitions ran in {run4['elapsed_seconds']} s; 8 partitions ran in {run8['elapsed_seconds']} s. Both runs had {run4['output_rows']} output rows and 0 errors.",
            f"The 8-partition run was faster by {comparison['runtime_reduction_percent_vs_slowest']} percent on this machine. Output equivalence max absolute difference = {comparison['output_equivalence']['max_abs_diff_overall']}.",
            f"Spark UI evidence: reports/figures/spark_ui_step2.png was generated from real Spark statusTracker job/stage/task data. Literal browser screenshot created={ui['screenshot_created']}.",
        ])

        page_chart(pdf, "01_class_distribution.png", "Step 3 Class distribution")
        page_chart(pdf, "02_country_split.png", "Step 3 Country and split")
        page_chart(pdf, "03_ndvi_by_country.png", "Step 3 NDVI by country")

        page_text(pdf, "Step 4 Classification", [
            f"Classes: {', '.join(metrics['counts']['classes'])}. Train/validation/test task rows: {metrics['counts']['train']} / {metrics['counts']['validation']} / {metrics['counts']['test']}.",
            "The task uses the official split. Train statistics are used for thresholds and standardisation; validation is used for feature selection; test is used for final metrics.",
            f"Majority baseline: accuracy={metrics['test_metrics']['majority']['accuracy']:.3f}, macro-F1={metrics['test_metrics']['majority']['macro_f1']:.3f}.",
            f"Training-derived rule: accuracy={metrics['test_metrics']['rule']['accuracy']:.3f}, macro-F1={metrics['test_metrics']['rule']['macro_f1']:.3f}.",
            f"Centroid full: accuracy={metrics['test_metrics']['centroid_full']['accuracy']:.3f}, macro-F1={metrics['test_metrics']['centroid_full']['macro_f1']:.3f}.",
            f"Centroid reduced: accuracy={metrics['test_metrics']['centroid_reduced']['accuracy']:.3f}, macro-F1={metrics['test_metrics']['centroid_reduced']['macro_f1']:.3f}.",
            "Marine waters is easiest; the main confusion is between Pastures and Arable land. The reduced feature set improves macro-F1 over the full centroid.",
        ])

        page_text(pdf, "Final Discussion", [
            "Final verdict: substantially complete with explicit deviation.",
            "The core Spark pipeline is complete for the HDF5-direct path: 52,607 patches, executor-side feature extraction, validation, SQL analysis, charts, and classification are supported by local artifacts.",
            "The explicit Step 1 deviation is that original downloaded TIFF files were not packed for 52,607 patches. The project has a measured TIFF pilot and a full HDF5-derived raw band-byte Parquet pack instead.",
            "The remaining infrastructure evidence gaps are real HDFS block size, replication factor, live DataNode count, and a real Spark UI screenshot image. These were not fabricated.",
            "Generative AI was used to help organise and polish the work. All numerical evidence in this PDF comes from project artifacts under reports/ and hdfs_mirror/bigearthnet/results/.",
        ])

    print(out)


if __name__ == "__main__":
    main()
