#!/usr/bin/env python3
"""Build Assignment 3 final markdown report from verified JSON artifacts."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"


def load(name: str) -> dict:
    return json.loads((REPORTS / name).read_text(encoding="utf-8"))


def pct(x: float) -> str:
    return f"{x:.3f}"


def main() -> None:
    s1 = load("a3_step1_features_v2_validation.json")
    s2 = load("a3_step2_multiclass.json")
    s3 = load("a3_step3_multilabel.json")
    s4 = load("a3_step4_unsupervised.json")
    s5 = load("a3_step5_scalability_deployment.json")

    lines = [
        "# BDA501 Individual Assignment 3 Report",
        "",
        "Land-Cover Classification at Scale with Spark MLlib.",
        "",
        "## 1. Environment and data",
        "",
        "- Platform: Windows local Spark / PySpark MLlib, local HDFS mirror `hdfs_mirror/bigearthnet`.",
        "- Dataset source: BigEarthNet HDF5 partial data from Assignment 2.",
        f"- Rows used: {s1['validation']['rows']:,} patches across {len(s1['validation']['countries'])} countries.",
        f"- Official split counts: {s1['validation']['splits']}.",
        "- No random split or random-fold cross-validation was used; train/validation/test columns are preserved.",
        "- AI assistance: OpenAI Codex helped implement and verify scripts; all code/results are reproducible from this repo.",
        "",
        "## 2. Step 1: Richer features",
        "",
        f"Features v2 saved to `{s1['features_v2_uri']}`.",
        f"Final feature count: {s1['validation']['n_features']}.",
        "The new 20 m bands B05, B06, B07, B8A, B11 and B12 are processed on their own 60 x 60 grid. NDMI, NDBI, and NDVI p10/p50/p90 are computed per patch.",
        f"Validation: {s1['validation']['errors']} feature errors; null counts and NaN counts are all zero; index range failures: {s1['validation']['bad_ranges']}.",
        "",
        "## 3. Step 2: Multi-class classification",
        "",
        f"Selected frequent classes from train: {len(s2['selected_top_classes'])} available classes in this subset.",
        "The pipeline for every model is `StringIndexer -> VectorAssembler -> StandardScaler -> classifier`, with inverse-frequency `weightCol` from train only.",
        "Validation tuning results:",
        "",
        "| Model | Best config | Validation macro-F1 | Train time (s) |",
        "|---|---:|---:|---:|",
    ]
    for model in s2["grid_results"]:
        best = model["best"]
        lines.append(f"| {model['model_name']} | `{best['params']}` | {pct(best['validation_macro_f1'])} | {best['training_time_sec']} |")
    tm = s2["test_metrics"]
    lines += [
        "",
        f"Best model: `{s2['best_model_name']}`. Test accuracy={pct(tm['accuracy'])}, weighted-F1={pct(tm['weighted_f1'])}, macro-F1={pct(tm['macro_f1'])}.",
        "Assignment 2 baselines are included in `reports/a3_step2_multiclass.json` for comparison.",
        "Top RF features include: " + ", ".join(x["feature"] for x in s2["interpretation"]["rf_feature_importances"][:10]) + ".",
        "Top-k retraining summary:",
        "",
        "| k | Test accuracy | Macro-F1 | Weighted-F1 |",
        "|---:|---:|---:|---:|",
    ]
    for row in s2["top_k_retrain"]:
        m = row["test_metrics"]
        lines.append(f"| {row['k']} | {pct(m['accuracy'])} | {pct(m['macro_f1'])} | {pct(m['weighted_f1'])} |")

    lines += [
        "",
        "## 4. Step 3: Multi-label classification",
        "",
        "All 19 CORINE labels are encoded as binary columns. One Logistic Regression binary classifier is trained per class using one train-fitted feature-preparation PipelineModel. Thresholds are selected on validation only.",
        f"Test micro-F1={pct(s3['summary']['micro_f1'])}; macro-F1={pct(s3['summary']['macro_f1'])}.",
        "Marine waters and Pastures are strongest; rare or visually mixed classes such as Coastal wetlands, Beaches/dunes/sands, and Industrial/commercial units are weak from patch-level summaries.",
        "",
        "## 5. Step 4: Unsupervised analysis",
        "",
        f"PCA explained variance for PC1/PC2: {[round(x, 4) for x in s4['pca']['explained_variance']]}.",
        f"PCA plot: `{s4['pca']['plot']}`.",
        f"Best KMeans k by silhouette: {s4['kmeans']['best_k']['k']} with silhouette={pct(s4['kmeans']['best_k']['silhouette'])}.",
        "The contingency table shows one cluster dominated by Marine waters and others mostly vegetation/agriculture; clusters do not perfectly match land-cover classes because labels are multi-label and features lose spatial texture.",
        "",
        "## 6. Step 5: Scalability and deployment",
        "",
        "Scaling experiments used stratified `sampleBy`, two partition counts, and caching.",
        "",
        "| Train fraction | Partitions | Train rows | Time (s) | Test macro-F1 |",
        "|---:|---:|---:|---:|---:|",
    ]
    for row in s5["scaling"]["rows"]:
        lines.append(f"| {row['train_fraction']} | {row['partitions']} | {row['train_rows']} | {row['training_time_sec']} | {pct(row['test_macro_f1'])} |")
    lines += [
        "",
        f"Model saved to `{s5['persistence']['model_uri']}` and reloaded metrics matched exactly: `{s5['persistence']['reload_reproduces_test_metrics_exactly']}`.",
        f"Batch predictions saved to `{s5['batch_inference']['predictions_uri']}` partitioned by country.",
        f"Held-out country `{s5['held_out_country']}` accuracy={pct(s5['batch_inference']['held_out_country_metrics']['accuracy'])}, macro-F1={pct(s5['batch_inference']['held_out_country_metrics']['macro_f1'])}.",
        "",
        "## 7. Reproducibility commands",
        "",
        "```powershell",
        ".\\.venv\\Scripts\\python.exe scripts\\05_a3_features_v2.py --partitions 16",
        ".\\.venv\\Scripts\\python.exe scripts\\06_a3_multiclass.py",
        ".\\.venv\\Scripts\\python.exe scripts\\07_a3_multilabel.py",
        ".\\.venv\\Scripts\\python.exe scripts\\08_a3_unsupervised.py",
        ".\\.venv\\Scripts\\python.exe scripts\\09_a3_scalability_deploy.py",
        "```",
        "",
        "## 8. Limitation / deviation notes",
        "",
        "- The provided partial dataset contains 18 of 19 class labels and the single-label top-class task has fewer than 35 usable exact-one frequent classes. The scripts request 35 but transparently report the available classes/counts.",
        "- Windows local Spark required local Hadoop native helpers (`winutils.exe`, `hadoop.dll`) for Parquet writes.",
    ]
    out = REPORTS / "a3_final_report.md"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(out)


if __name__ == "__main__":
    main()
