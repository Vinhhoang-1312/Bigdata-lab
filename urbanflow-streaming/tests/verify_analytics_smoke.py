#!/usr/bin/env python3
"""Verify Analytics can read Gold and prepare the current UI view."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from urllib.request import urlopen
from pathlib import Path
from typing import Any

STREAMING_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = STREAMING_ROOT.parent
if str(STREAMING_ROOT) not in sys.path:
    sys.path.insert(0, str(STREAMING_ROOT))

DEFAULT_HISTORICAL = "s3a://urbanflow/gold/historical_zone_baseline/"
DEFAULT_LIVE = "s3a://urbanflow/gold/live_zone_metrics/task6/task6_20260914_003/"
DEFAULT_LOOKUP = REPO_ROOT / "urbanflow-foundation" / "fixtures" / "bronze" / "zone_lookup.csv"
DEFAULT_RESULT_MD = STREAMING_ROOT / "docs" / "results" / "analytics-smoke.md"


def verify(args: argparse.Namespace) -> dict[str, Any]:
    from analytics_app.data_access import GoldReader, chart_mode_for, prepare_live_view
    from analytics_app.zones import attach_demand_to_geojson, load_taxi_zone_geojson, load_zone_lookup

    reader = GoldReader(args.historical_path, args.live_path)
    live = reader.live()
    historical = reader.historical()
    lookup = load_zone_lookup(args.lookup_path)
    view = prepare_live_view(live, lookup)
    latest = view[view.window_start_local == view.window_start_local.max()] if not view.empty else view
    geo = attach_demand_to_geojson(load_taxi_zone_geojson(), latest)
    painted = [
        item for item in geo["features"]
        if item["properties"].get("current_trip_count") is not None
    ]
    missing_columns = sorted(
        {
            "zone_label",
            "window_label",
            "current_trip_count",
            "historical_mean_trip_count",
            "demand_ratio",
            "z_score",
            "is_high_demand",
        }
        - set(view.columns)
    )
    status = "PASS"
    reasons: list[str] = []
    if live.empty:
        status = "FAIL"
        reasons.append("Live Gold is empty")
    if historical.empty:
        status = "FAIL"
        reasons.append("Historical Gold is empty")
    if view.empty:
        status = "FAIL"
        reasons.append("Prepared analytics view is empty")
    if missing_columns:
        status = "FAIL"
        reasons.append(f"Missing UI columns: {missing_columns}")
    if not painted:
        status = "FAIL"
        reasons.append("No taxi-zone polygons received live demand values")
    streamlit_status = None
    if args.streamlit_url:
        try:
            with urlopen(args.streamlit_url, timeout=args.streamlit_timeout_seconds) as response:
                streamlit_status = response.status
            if streamlit_status != 200:
                status = "FAIL"
                reasons.append(f"Streamlit returned HTTP {streamlit_status}")
        except Exception as exc:  # noqa: BLE001 - smoke records UI reachability.
            status = "FAIL"
            reasons.append(f"Streamlit launch check failed: {exc}")

    return {
        "status": status,
        "reasons": reasons,
        "historical_path": args.historical_path,
        "live_path": args.live_path,
        "lookup_path": str(args.lookup_path),
        "s3_endpoint": os.getenv("URBANFLOW_S3_ENDPOINT") or os.getenv("AWS_ENDPOINT_URL"),
        "historical_rows": int(len(historical)),
        "live_rows": int(len(live)),
        "view_rows": int(len(view)),
        "latest_rows": int(len(latest)),
        "painted_zone_polygons": int(len(painted)),
        "high_demand_latest_zones": int(latest["is_high_demand"].sum()) if not latest.empty else 0,
        "chart_mode": chart_mode_for(view),
        "latest_window": None if latest.empty else str(latest.window_label.iloc[0]),
        "streamlit_url": args.streamlit_url,
        "streamlit_http_status": streamlit_status,
        "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "limitations": [
            "Smoke verifies Gold read and UI-ready data preparation.",
            "It does not inspect a browser screenshot.",
        ],
    }


def render_markdown(summary: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Analytics Smoke Result",
            "",
            f"Date: {summary['checked_at']}",
            f"Status: `{summary['status']}`",
            "",
            "## Summary",
            "",
            f"- Historical rows: `{summary['historical_rows']}`",
            f"- Live rows: `{summary['live_rows']}`",
            f"- Prepared view rows: `{summary['view_rows']}`",
            f"- Latest-window rows: `{summary['latest_rows']}`",
            f"- Painted zone polygons: `{summary['painted_zone_polygons']}`",
            f"- High-demand latest zones: `{summary['high_demand_latest_zones']}`",
            f"- Chart mode: `{summary['chart_mode']}`",
            f"- Latest window: `{summary['latest_window']}`",
            f"- Streamlit URL: `{summary.get('streamlit_url')}`",
            f"- Streamlit HTTP status: `{summary.get('streamlit_http_status')}`",
            f"- Historical path: `{summary['historical_path']}`",
            f"- Live path: `{summary['live_path']}`",
            f"- S3 endpoint: `{summary['s3_endpoint']}`",
            "",
            "## Reasons",
            "",
            "```json",
            json.dumps(summary["reasons"], indent=2),
            "```",
            "",
            "## Limitations",
            "",
            "\n".join(f"- {item}" for item in summary["limitations"]),
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--historical-path", default=DEFAULT_HISTORICAL)
    parser.add_argument("--live-path", default=DEFAULT_LIVE)
    parser.add_argument("--lookup-path", default=str(DEFAULT_LOOKUP))
    parser.add_argument("--result-md", default=str(DEFAULT_RESULT_MD))
    parser.add_argument("--streamlit-url")
    parser.add_argument("--streamlit-timeout-seconds", type=int, default=15)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        summary = verify(args)
    except Exception as exc:  # noqa: BLE001 - CLI records smoke failure evidence.
        summary = {
            "status": "FAIL",
            "reasons": [str(exc).splitlines()[0]],
            "historical_path": args.historical_path,
            "live_path": args.live_path,
            "lookup_path": args.lookup_path,
            "s3_endpoint": os.getenv("URBANFLOW_S3_ENDPOINT") or os.getenv("AWS_ENDPOINT_URL"),
            "historical_rows": None,
            "live_rows": None,
            "view_rows": None,
            "latest_rows": None,
            "painted_zone_polygons": None,
            "high_demand_latest_zones": None,
            "chart_mode": None,
            "latest_window": None,
            "streamlit_url": args.streamlit_url,
            "streamlit_http_status": None,
            "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "limitations": ["Verifier failed before completing analytics preparation."],
        }
    target = Path(args.result_md)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_markdown(summary) + "\n", encoding="utf-8")
    print(json.dumps(summary, sort_keys=True))
    print(f"ANALYTICS SMOKE = {summary['status']}")
    return 0 if summary["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
