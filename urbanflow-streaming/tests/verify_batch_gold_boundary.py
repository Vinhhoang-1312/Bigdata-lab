#!/usr/bin/env python3
"""Verify Batch Historical Gold can be consumed directly by Streaming."""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

STREAMING_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = STREAMING_ROOT.parent
if str(STREAMING_ROOT) not in sys.path:
    sys.path.insert(0, str(STREAMING_ROOT))

CONTRACT_PATH = "s3a://urbanflow/gold/historical_zone_baseline/"
DEFAULT_RESULT_MD = (
    STREAMING_ROOT / "docs" / "results" / "batch-gold-boundary.md"
)


def field_signature(schema) -> list[dict[str, Any]]:
    return [
        {
            "name": field.name,
            "type": field.dataType.simpleString(),
            "nullable": field.nullable,
        }
        for field in schema
    ]


def source_schema_issues(source_schema, expected_schema) -> list[str]:
    source = {field.name: field.dataType.simpleString() for field in source_schema}
    expected = {
        field.name: field.dataType.simpleString()
        for field in expected_schema
    }
    issues: list[str] = []
    missing = [name for name in expected if name not in source]
    extra = [name for name in source if name not in expected]
    if missing:
        issues.append(f"missing columns: {missing}")
    if extra:
        issues.append(f"extra columns: {extra}")
    wrong_types = [
        f"{name}: expected {expected[name]}, got {source[name]}"
        for name in expected
        if name in source and source[name] != expected[name]
    ]
    if wrong_types:
        issues.append(f"type mismatches: {wrong_types}")
    return issues


def null_violation_count(df, schema) -> int:
    from pyspark.sql import functions as F

    required = [field.name for field in schema if not field.nullable]
    if not required:
        return 0
    predicate = None
    for name in required:
        item = F.col(name).isNull()
        predicate = item if predicate is None else predicate | item
    return df.where(predicate).limit(1).count()


def nonnegative_violation_count(df) -> int:
    from pyspark.sql import functions as F

    return df.where(
        (F.col("historical_mean_trip_count") < 0)
        | (F.col("historical_std_trip_count") < 0)
    ).limit(1).count()


def duplicate_key_count(df, key: list[str], row_count: int) -> int:
    unique_key_count = df.select(*key).distinct().count()
    return row_count - unique_key_count


def current_window_for_key(row) -> tuple[datetime, datetime]:
    base_monday = datetime(2025, 1, 6, tzinfo=ZoneInfo("America/New_York"))
    start = base_monday + timedelta(
        days=int(row.day_of_week) - 1,
        hours=int(row.hour),
        minutes=int(row.five_minute_slot) * 5,
    )
    return start, start + timedelta(minutes=5)


def close_enough(left: float | None, right: float | None) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return math.isclose(float(left), float(right), rel_tol=1e-9, abs_tol=1e-9)


def verify_join_sample(spark, historical) -> dict[str, Any]:
    from pyspark.sql import types as T
    from spark.live_trip_metrics import join_historical

    sample = (
        historical.where(
            "historical_mean_trip_count > 0 AND historical_std_trip_count > 0"
        )
        .orderBy("pickup_zone_id", "day_of_week", "hour", "five_minute_slot")
        .limit(1)
        .collect()
    )
    if not sample:
        sample = (
            historical.orderBy(
                "pickup_zone_id",
                "day_of_week",
                "hour",
                "five_minute_slot",
            )
            .limit(1)
            .collect()
        )
    if not sample:
        raise ValueError("Historical baseline has zero rows")

    baseline = sample[0]
    window_start, window_end = current_window_for_key(baseline)
    current_count = max(
        1,
        int(round(
            baseline.historical_mean_trip_count
            + baseline.historical_std_trip_count
        )),
    )
    current_schema = T.StructType(
        [
            T.StructField("window_start", T.TimestampType(), False),
            T.StructField("window_end", T.TimestampType(), False),
            T.StructField("pickup_zone_id", T.IntegerType(), False),
            T.StructField("current_trip_count", T.LongType(), False),
        ]
    )
    current = spark.createDataFrame(
        [
            (
                window_start,
                window_end,
                int(baseline.pickup_zone_id),
                int(current_count),
            )
        ],
        current_schema,
    )
    joined = join_historical(current, historical).collect()[0]

    expected_ratio = None
    if baseline.historical_mean_trip_count > 0:
        expected_ratio = current_count / baseline.historical_mean_trip_count
    expected_z = None
    if baseline.historical_std_trip_count > 0:
        expected_z = (
            current_count - baseline.historical_mean_trip_count
        ) / baseline.historical_std_trip_count

    if joined.historical_mean_trip_count != baseline.historical_mean_trip_count:
        raise ValueError("join_historical returned the wrong baseline mean")
    if not close_enough(joined.demand_ratio, expected_ratio):
        raise ValueError("join_historical returned the wrong demand_ratio")
    if not close_enough(joined.z_score, expected_z):
        raise ValueError("join_historical returned the wrong z_score")

    return {
        "pickup_zone_id": int(joined.pickup_zone_id),
        "window_start": joined.window_start.isoformat(),
        "current_trip_count": int(joined.current_trip_count),
        "historical_mean_trip_count": float(joined.historical_mean_trip_count),
        "demand_ratio": joined.demand_ratio,
        "z_score": joined.z_score,
    }


def verify_boundary(spark, path: str, source_commit: str) -> dict[str, Any]:
    from spark.historical_lookup import HISTORICAL_SCHEMA, KEY, load_historical_baseline

    raw = spark.read.parquet(path)
    schema_issues = source_schema_issues(raw.schema, HISTORICAL_SCHEMA)
    if schema_issues:
        raise ValueError("; ".join(schema_issues))

    required_nulls = null_violation_count(raw, HISTORICAL_SCHEMA)
    if required_nulls:
        raise ValueError("Historical Gold has nulls in required fields")

    negative_values = nonnegative_violation_count(raw)
    if negative_values:
        raise ValueError("Historical Gold has negative mean or std values")

    historical = load_historical_baseline(spark, path)
    row_count = historical.count()
    duplicate_keys = duplicate_key_count(historical, KEY, row_count)
    if duplicate_keys:
        raise ValueError("Historical Gold contains duplicate lookup keys")

    unique_key_count = row_count - duplicate_keys
    return {
        "status": "PASS",
        "path": path,
        "source_commit": source_commit,
        "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "row_count": row_count,
        "schema": field_signature(raw.schema),
        "unique_key_count": unique_key_count,
        "duplicate_key_count": duplicate_keys,
        "sample_join": verify_join_sample(spark, historical),
    }


def is_missing_artifact_error(exc: BaseException) -> bool:
    text = str(exc)
    markers = [
        "Path does not exist",
        "FileNotFoundException",
        "No such file or directory",
        "Not Found",
        "404",
    ]
    return any(marker in text for marker in markers)


def schema_table(schema: list[dict[str, Any]]) -> str:
    if not schema:
        return "_No schema loaded._"
    lines = ["| name | type | nullable |", "|---|---|---|"]
    lines.extend(
        f"| `{field['name']}` | `{field['type']}` | `{field['nullable']}` |"
        for field in schema
    )
    return "\n".join(lines)


def render_markdown(summary: dict[str, Any]) -> str:
    sample = summary.get("sample_join")
    sample_text = json.dumps(sample, indent=2, sort_keys=True) if sample else "null"
    return "\n".join(
        [
            "# Batch Historical Gold Boundary Result",
            "",
            f"Date: {summary.get('checked_at', 'not-run')}",
            f"Status: `{summary['status']}`",
            f"Path: `{summary['path']}`",
            f"Batch source commit: `{summary.get('source_commit', 'unknown')}`",
            "",
            "## Summary",
            "",
            f"- Reason: {summary.get('reason', 'Historical Gold loaded successfully.')}",
            f"- Row count: `{summary.get('row_count')}`",
            f"- Unique key count: `{summary.get('unique_key_count')}`",
            f"- Duplicate key count: `{summary.get('duplicate_key_count')}`",
            "",
            "## Schema",
            "",
            schema_table(summary.get("schema", [])),
            "",
            "## Sample Join",
            "",
            "```json",
            sample_text,
            "```",
            "",
            "## Notes",
            "",
            "- This verifier reads the canonical S3A path directly.",
            "- It does not import Batch application source code.",
            "- It validates Streaming can consume Historical Gold without an adapter.",
        ]
    )


def exit_code(summary: dict[str, Any]) -> int:
    if summary["status"] == "PASS":
        return 0
    if summary["status"] == "BLOCKED_MISSING_ARTIFACT":
        return 2
    return 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", default=CONTRACT_PATH)
    parser.add_argument("--source-commit", default="unknown")
    parser.add_argument("--result-md", default=str(DEFAULT_RESULT_MD))
    return parser.parse_args()


def write_result(path: str, summary: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_markdown(summary) + "\n", encoding="utf-8")


def main() -> int:
    from pyspark.sql import SparkSession

    args = parse_args()
    spark = (
        SparkSession.builder.appName("UrbanFlow Batch Gold Boundary")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        try:
            summary = verify_boundary(spark, args.path, args.source_commit)
        except Exception as exc:  # noqa: BLE001 - CLI must record failures.
            status = "BLOCKED_MISSING_ARTIFACT"
            reason = "No Parquet files were found at the contract path."
            if not is_missing_artifact_error(exc):
                status = "FAIL"
                reason = str(exc).splitlines()[0]
            summary = {
                "status": status,
                "path": args.path,
                "source_commit": args.source_commit,
                "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "reason": reason,
                "row_count": None,
                "schema": [],
                "unique_key_count": None,
                "duplicate_key_count": None,
                "sample_join": None,
            }
        write_result(args.result_md, summary)
        print(json.dumps(summary, sort_keys=True))
        print(f"Batch Gold boundary = {summary['status']}")
        return exit_code(summary)
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
