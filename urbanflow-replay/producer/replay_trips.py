#!/usr/bin/env python3
"""CLI for fixture or integrated Silver replay to canonical Kafka."""

from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path

from producer.config import load_config
from producer.input_reader import (
    integrated_row_count,
    iter_fixture,
    iter_integrated,
    load_zone_ids,
)
from producer.replay import replay_records

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / "config" / "settings.yaml"
ZONE_FIXTURE = REPO_ROOT / "urbanflow-foundation/fixtures/bronze/zone_lookup.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--mode", choices=("fixture", "integrated"), default="fixture")
    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Fixed event rate (legacy mode)",
    )
    parser.add_argument(
        "--speedup",
        type=float,
        default=None,
        help="Proportional virtual clock acceleration factor (e.g. 60.0 for 1s wall = 1m virtual)",
    )
    parser.add_argument(
        "--max-rate",
        type=float,
        default=None,
        help="Burst limiter: maximum events per second (e.g. 500 eps)",
    )
    parser.add_argument("--max-events", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--print-events", type=int, default=0)
    parser.add_argument("--metrics-output")
    parser.add_argument(
        "--run-id",
        help="Event-ID suffix; defaults to a unique value for each CLI run",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_config(args.config)

    if args.mode == "fixture":
        records = list(iter_fixture(config.fixture_path))
        valid_zone_ids = load_zone_ids(ZONE_FIXTURE)
    else:
        count = integrated_row_count(
            config.integrated_silver_uri,
            config.s3_endpoint,
            config.s3_region,
            config.replay_start_date,
            config.replay_end_date,
        )
        print(f"integrated_replay_rows={count}")

        def records():
            return iter_integrated(
                config.integrated_silver_uri,
                config.s3_endpoint,
                config.s3_region,
                config.replay_start_date,
                config.replay_end_date,
            )

        valid_zone_ids = None

    run_id = args.run_id or uuid.uuid4().hex[:12]
    metrics, interrupted = replay_records(
        records,
        topic=config.topic,
        bootstrap_servers=config.bootstrap_servers,
        rate=args.rate,
        speedup=args.speedup,
        max_rate=args.max_rate,
        max_events=args.max_events,
        dry_run=args.dry_run,
        print_events=args.print_events,
        valid_zone_ids=valid_zone_ids,
        event_id_run_id=run_id,
    )
    result = metrics.as_dict()
    result.update(
        {
            "foundation_version": config.foundation_version,
            "mode": args.mode,
            "topic": config.topic,
            "interrupted": interrupted,
            "run_id": run_id,
            "speedup": args.speedup,
        }
    )
    rendered = json.dumps(result, sort_keys=True)
    print(rendered)
    if args.metrics_output:
        output = Path(args.metrics_output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")

    if interrupted:
        return 130
    if metrics.records_rejected or metrics.send_errors:
        return 1
    print("REPLAY = PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
