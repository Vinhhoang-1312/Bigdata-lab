#!/usr/bin/env python3
"""Audit canonical Silver before any ML training is allowed."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from forecasting.data_quality import evaluate_month_coverage
from forecasting.schemas import SILVER_NON_NULL_COLUMNS, SILVER_REQUIRED_COLUMNS

DEFAULT_SILVER = "s3a://urbanflow/silver/trips/"
DEFAULT_RESULT_MD = ML_ROOT / "docs" / "results" / "silver-audit.md"


def month_label(year: int, month: int) -> str:
    return f"{int(year):04d}-{int(month):02d}"


def verify(args: argparse.Namespace) -> dict[str, Any]:
    from pyspark.sql import SparkSession, functions as F

    spark = (
        SparkSession.builder.appName("UrbanFlow ML Silver Audit")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        frame = spark.read.parquet(args.silver_path)
        columns = frame.columns
        missing_columns = sorted(set(SILVER_REQUIRED_COLUMNS) - set(columns))
        total_rows = frame.count()
        months_frame = (
            frame.groupBy("year", "month")
            .agg(F.count(F.lit(1)).alias("rows"))
            .orderBy("year", "month")
        )
        monthly_rows = [
            {
                "month": month_label(row["year"], row["month"]),
                "rows": int(row["rows"]),
            }
            for row in months_frame.collect()
        ]
        coverage = evaluate_month_coverage([item["month"] for item in monthly_rows])
        null_counts = {}
        for name in SILVER_NON_NULL_COLUMNS:
            if name in columns:
                null_counts[name] = int(frame.where(F.col(name).isNull()).count())
        invalid_ranges = {
            "negative_trip_distance": int(frame.where(F.col("trip_distance") < 0).count())
            if "trip_distance" in columns else None,
            "negative_trip_duration": int(frame.where(F.col("trip_duration_minutes") < 0).count())
            if "trip_duration_minutes" in columns else None,
            "negative_fare_amount": int(frame.where(F.col("fare_amount") < 0).count())
            if "fare_amount" in columns else None,
            "dropoff_before_pickup": int(frame.where(F.col("dropoff_datetime") < F.col("pickup_datetime")).count())
            if {"dropoff_datetime", "pickup_datetime"} <= set(columns) else None,
        }
        minmax = frame.agg(
            F.min("pickup_datetime").alias("min_pickup_datetime"),
            F.max("pickup_datetime").alias("max_pickup_datetime"),
        ).first().asDict()
        failed_reasons = []
        if missing_columns:
            failed_reasons.append(f"Missing Silver columns: {missing_columns}")
        if not coverage.passed:
            failed_reasons.append(f"Missing required months: {coverage.missing_months}")
        bad_nulls = {key: value for key, value in null_counts.items() if value}
        if bad_nulls:
            failed_reasons.append(f"Unexpected nulls: {bad_nulls}")
        bad_ranges = {key: value for key, value in invalid_ranges.items() if value}
        if bad_ranges:
            failed_reasons.append(f"Invalid range counts: {bad_ranges}")
        return {
            "status": "PASS" if not failed_reasons else "FAIL",
            "silver_path": args.silver_path,
            "total_rows": int(total_rows),
            "observed_months": coverage.observed_months,
            "required_months": coverage.required_months,
            "missing_months": coverage.missing_months,
            "monthly_rows": monthly_rows,
            "missing_columns": missing_columns,
            "null_counts": null_counts,
            "invalid_ranges": invalid_ranges,
            "min_pickup_datetime": str(minmax["min_pickup_datetime"]),
            "max_pickup_datetime": str(minmax["max_pickup_datetime"]),
            "failed_reasons": failed_reasons,
            "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    finally:
        spark.stop()


def render_markdown(summary: dict[str, Any]) -> str:
    monthly = "\n".join(
        f"| `{item['month']}` | `{item['rows']}` |"
        for item in summary["monthly_rows"]
    )
    return "\n".join(
        [
            "# ML Silver Audit",
            "",
            f"Date: {summary['checked_at']}",
            f"Status: `{summary['status']}`",
            f"Silver path: `{summary['silver_path']}`",
            "",
            "## Summary",
            "",
            f"- Total rows: `{summary['total_rows']}`",
            f"- Pickup datetime min: `{summary['min_pickup_datetime']}`",
            f"- Pickup datetime max: `{summary['max_pickup_datetime']}`",
            f"- Required months: `{', '.join(summary['required_months'])}`",
            f"- Observed months: `{', '.join(summary['observed_months'])}`",
            f"- Missing months: `{', '.join(summary['missing_months']) or 'none'}`",
            "",
            "## Monthly Rows",
            "",
            "| Month | Rows |",
            "|---|---:|",
            monthly,
            "",
            "## Failed Reasons",
            "",
            "```json",
            json.dumps(summary["failed_reasons"], indent=2),
            "```",
            "",
            "## Null Counts",
            "",
            "```json",
            json.dumps(summary["null_counts"], indent=2, sort_keys=True),
            "```",
            "",
            "## Invalid Range Counts",
            "",
            "```json",
            json.dumps(summary["invalid_ranges"], indent=2, sort_keys=True),
            "```",
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-path", default=DEFAULT_SILVER)
    parser.add_argument("--result-md", default=str(DEFAULT_RESULT_MD))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        summary = verify(args)
    except Exception as exc:  # noqa: BLE001 - CLI records audit failure evidence.
        summary = {
            "status": "FAIL",
            "silver_path": args.silver_path,
            "total_rows": None,
            "observed_months": [],
            "required_months": [],
            "missing_months": [],
            "monthly_rows": [],
            "missing_columns": [],
            "null_counts": {},
            "invalid_ranges": {},
            "min_pickup_datetime": None,
            "max_pickup_datetime": None,
            "failed_reasons": [str(exc).splitlines()[0]],
            "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    target = Path(args.result_md)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_markdown(summary) + "\n", encoding="utf-8")
    print(json.dumps(summary, sort_keys=True))
    print(f"ML DATA-GO = {summary['status']}")
    return 0 if summary["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
