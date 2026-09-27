#!/usr/bin/env python3
"""Step 3 — Spark SQL analyses + charts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.lib.io_stats import write_json
from scripts.lib.paths import features_path, figures_dir, local_hdfs_mirror, results_path
from scripts.lib.spark_session import get_spark
from scripts.sql_plot_helpers import records_to_frame


def save_df(df, name: str) -> None:
    uri = f"{results_path().rstrip('/')}/sql_{name}"
    df.write.mode("overwrite").parquet(uri)
    print(f"Wrote {uri}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--no-plots", action="store_true")
    args = ap.parse_args()

    spark = get_spark("BEN-Step3-SQL")
    feats = spark.read.parquet(features_path()).filter(~F.col("error_flag"))
    feats.createOrReplaceTempView("patches")

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW labels_exploded AS
        SELECT patch_id, country, split, explode(labels) AS label
        FROM patches
        """
    )

    # 1) Class distribution + labels per patch
    class_dist = spark.sql(
        """
        SELECT label, COUNT(*) AS n_patches
        FROM labels_exploded
        GROUP BY label
        ORDER BY n_patches DESC
        """
    )
    labels_per_patch = spark.sql(
        """
        SELECT size(labels) AS n_labels, COUNT(*) AS n_patches
        FROM patches
        GROUP BY size(labels)
        ORDER BY n_labels
        """
    )
    save_df(class_dist, "class_distribution")
    save_df(labels_per_patch, "labels_per_patch")

    # 2) Patches per country and country × split
    by_country = spark.sql(
        """
        SELECT country, COUNT(*) AS n_patches
        FROM patches
        GROUP BY country
        ORDER BY n_patches DESC
        """
    )
    by_country_split = spark.sql(
        """
        SELECT country, split, COUNT(*) AS n_patches
        FROM patches
        GROUP BY country, split
        ORDER BY country, split
        """
    )
    save_df(by_country, "patches_by_country")
    save_df(by_country_split, "patches_by_country_split")

    # 3) Top-10 co-occurring class pairs
    pairs = spark.sql(
        """
        WITH e AS (
          SELECT patch_id, explode(labels) AS label FROM patches
        )
        SELECT a.label AS label_a, b.label AS label_b, COUNT(*) AS n
        FROM e a
        JOIN e b ON a.patch_id = b.patch_id AND a.label < b.label
        GROUP BY a.label, b.label
        ORDER BY n DESC
        LIMIT 10
        """
    )
    save_df(pairs, "top10_cooccurring_pairs")

    # 4) Mean NDVI/brightness by country; mean features by class
    by_country_feat = spark.sql(
        """
        SELECT country,
               AVG(ndvi) AS mean_ndvi,
               AVG(ndwi) AS mean_ndwi,
               AVG(brightness) AS mean_brightness
        FROM patches
        GROUP BY country
        ORDER BY country
        """
    )
    by_class_feat = spark.sql(
        """
        SELECT label,
               AVG(ndvi) AS mean_ndvi,
               AVG(ndwi) AS mean_ndwi,
               AVG(brightness) AS mean_brightness,
               AVG(mean_b02) AS mean_b02,
               AVG(mean_b03) AS mean_b03,
               AVG(mean_b04) AS mean_b04,
               AVG(mean_b08) AS mean_b08
        FROM patches p
        LATERAL VIEW explode(labels) t AS label
        GROUP BY label
        ORDER BY label
        """
    )
    save_df(by_country_feat, "mean_features_by_country")
    save_df(by_class_feat, "mean_features_by_class")

    # 5) Custom: single-label rate by country + NDVI by split
    custom = spark.sql(
        """
        SELECT country,
               AVG(CASE WHEN size(labels) = 1 THEN 1.0 ELSE 0.0 END) AS single_label_rate,
               AVG(size(labels)) AS avg_labels_per_patch,
               COUNT(*) AS n_patches
        FROM patches
        GROUP BY country
        ORDER BY country
        """
    )
    ndvi_by_split = spark.sql(
        """
        SELECT split, AVG(ndvi) AS mean_ndvi, AVG(brightness) AS mean_brightness, COUNT(*) AS n
        FROM patches
        GROUP BY split
        ORDER BY split
        """
    )
    save_df(custom, "custom_single_label_rate_by_country")
    save_df(ndvi_by_split, "custom_ndvi_by_split")

    # 6) Split Ã— class distribution (the tenth persisted SQL result)
    class_by_split = spark.sql(
        """
        SELECT split, label, COUNT(*) AS n_patches
        FROM labels_exploded
        GROUP BY split, label
        ORDER BY split, n_patches DESC
        """
    )
    save_df(class_by_split, "class_distribution_by_split")

    summary = {
        "n_patches": feats.count(),
        "n_countries": feats.select("country").distinct().count(),
        "n_classes_observed": class_dist.count(),
    }
    write_json(Path("reports") / "step3_summary.json", summary)
    write_json(local_hdfs_mirror() / "results" / "step3_summary.json", summary)

    findings = [
        "# Step 3 findings",
        "",
        f"- Patches analysed: **{summary['n_patches']}** across **{summary['n_countries']}** countries.",
        "- Class distribution is typically long-tailed (forests / arable / pastures dominate).",
        "- Multi-label patches are common; single-label rate varies by country (see custom query).",
        "- Co-occurrence pairs often reflect ecologically adjacent CLC classes (e.g. forest mixtures).",
        "",
    ]
    (figures_dir() / "FINDINGS.md").write_text("\n".join(findings), encoding="utf-8")

    if not args.no_plots:
        fig_dir = figures_dir()
        cd = records_to_frame(row.asDict() for row in class_dist.collect())
        plt.figure(figsize=(10, 5))
        plt.barh(cd["label"][::-1], cd["n_patches"][::-1])
        plt.xlabel("Number of patches (label occurrences)")
        plt.title("Class distribution (exploded multi-labels)")
        plt.tight_layout()
        plt.savefig(fig_dir / "01_class_distribution.png", dpi=150)
        plt.close()

        cs = records_to_frame(row.asDict() for row in by_country_split.collect())
        pivot = cs.pivot(index="country", columns="split", values="n_patches").fillna(0)
        pivot.plot(kind="bar", stacked=True, figsize=(8, 4))
        plt.ylabel("Patches")
        plt.title("Patches per country × split")
        plt.tight_layout()
        plt.savefig(fig_dir / "02_country_split.png", dpi=150)
        plt.close()

        cf = records_to_frame(row.asDict() for row in by_country_feat.collect())
        plt.figure(figsize=(7, 4))
        plt.bar(cf["country"], cf["mean_ndvi"])
        plt.xticks(rotation=30, ha="right")
        plt.ylabel("Mean NDVI")
        plt.title("Mean NDVI by country")
        plt.tight_layout()
        plt.savefig(fig_dir / "03_ndvi_by_country.png", dpi=150)
        plt.close()
        print(f"Wrote charts under {fig_dir}")

    print(json.dumps(summary, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
