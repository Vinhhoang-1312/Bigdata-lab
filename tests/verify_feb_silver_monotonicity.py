#!/usr/bin/env python3
"""Verify global chronological monotonicity of iter_integrated() on February 2023 Silver.

Tests:
1. iter_integrated() yields strictly non-decreasing pickup_datetime records.
2. Reports total rows, min/max timestamps, ordering violations count, and examples.
3. Measures memory boundedness and peak RSS.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "urbanflow-replay"))

from producer.config import load_config  # noqa: E402
from producer.input_reader import iter_integrated  # noqa: E402


def get_current_rss_mb() -> float:
    try:
        return int(os.popen(f"ps -o rss= -p {os.getpid()}").read().strip()) / 1024.0
    except Exception:
        return 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional limit on rows to scan (defaults to all 2,888,729 rows)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=8192,
        help="Scanner batch size (default: 8192)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = REPO_ROOT / "urbanflow-replay/config/settings.yaml"
    config = load_config(config_path)

    endpoint = os.environ.get("URBANFLOW_S3_ENDPOINT")
    if not endpoint:
        endpoint = "http://seaweedfs:8333" if os.path.exists("/.dockerenv") else "http://localhost:8333"

    print("==================================================================")
    print("Verifying Global Chronological Monotonicity of iter_integrated()")
    print(f"Replay Period: {config.replay_start_date} to {config.replay_end_date}")
    print(f"S3 URI:        {config.integrated_silver_uri}")
    print(f"S3 Endpoint:   {endpoint}")
    print(f"Batch Size:    {args.batch_size}")
    if args.max_rows:
        print(f"Row Limit:     {args.max_rows:,}")
    print("==================================================================")

    initial_rss = get_current_rss_mb()
    max_rss = initial_rss
    t0 = time.time()

    stream = iter_integrated(
        config.integrated_silver_uri,
        endpoint,
        config.s3_region,
        config.replay_start_date,
        config.replay_end_date,
        batch_size=args.batch_size,
    )

    rows = 0
    violations = 0
    prev_dt: datetime | None = None
    first_ts: str | None = None
    last_ts: str | None = None
    examples: list[tuple[int, str, str]] = []

    for r in stream:
        rows += 1
        ts_str = r["pickup_datetime"]
        cur_dt = datetime.fromisoformat(ts_str)

        if first_ts is None:
            first_ts = ts_str
        last_ts = ts_str

        if prev_dt is not None and cur_dt < prev_dt:
            violations += 1
            if len(examples) < 10:
                examples.append((rows, str(prev_dt), str(cur_dt)))

        prev_dt = cur_dt

        if rows % 250000 == 0:
            cur_rss = get_current_rss_mb()
            if cur_rss > max_rss:
                max_rss = cur_rss
            elapsed = time.time() - t0
            rate = rows / max(1e-6, elapsed)
            print(
                f"Progress: {rows:,} rows scanned ({rate:.0f} rows/s), "
                f"current timestamp: {ts_str}, RSS: {cur_rss:.1f} MB, violations: {violations}"
            )

        if args.max_rows is not None and rows >= args.max_rows:
            break

    elapsed = time.time() - t0
    final_rss = get_current_rss_mb()
    if final_rss > max_rss:
        max_rss = final_rss

    print("------------------------------------------------------------------")
    print(f"Total Rows Scanned:     {rows:,}")
    print(f"First Timestamp:        {first_ts}")
    print(f"Last Timestamp:         {last_ts}")
    print(f"Ordering Violations:    {violations}")
    print(f"Peak RSS Memory:        {max_rss:.2f} MB")
    print(f"Elapsed Time:           {elapsed:.2f} s")
    print("------------------------------------------------------------------")

    if violations > 0:
        print(f"FAIL: Found {violations} chronological ordering violations!")
        for ex in examples:
            print(f"  Row {ex[0]}: prev={ex[1]} > curr={ex[2]}")
        return 1

    print("GLOBAL CHRONOLOGICAL MONOTONICITY = PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
