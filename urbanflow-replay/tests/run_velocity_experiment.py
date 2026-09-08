#!/usr/bin/env python3
"""Measure controlled Kafka replay at every planned demo rate."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from producer.config import load_config
from producer.input_reader import iter_integrated
from producer.replay import replay_records

REPLAY = Path(__file__).resolve().parents[1]
CONFIG = REPLAY / "config/settings.yaml"
OUTPUT = REPLAY / "output/velocity-results.json"
SECONDS_PER_RATE = 2
MINIMUM_RATE_FRACTION = 0.90
MAXIMUM_RATE_FRACTION = 1.10


def records(config):
    return iter_integrated(
        config.integrated_silver_uri,
        config.s3_endpoint,
        config.s3_region,
        config.replay_start_date,
        config.replay_end_date,
    )


def main() -> None:
    config = load_config(CONFIG)
    results = []
    for rate in config.planned_rates:
        count = rate * SECONDS_PER_RATE
        run_id = f"velocity-{rate}-{uuid.uuid4().hex[:8]}"
        metrics, interrupted = replay_records(
            records(config),
            topic=config.topic,
            bootstrap_servers=config.bootstrap_servers,
            rate=rate,
            max_events=count,
            event_id_run_id=run_id,
        )
        assert not interrupted
        assert metrics.records_read == count
        assert metrics.records_serialized == count
        assert metrics.records_rejected == 0
        assert metrics.records_sent == count
        assert metrics.send_errors == 0
        achieved = metrics.achieved_events_per_second
        assert achieved >= rate * MINIMUM_RATE_FRACTION, (rate, achieved)
        assert achieved <= rate * MAXIMUM_RATE_FRACTION, (rate, achieved)
        result = metrics.as_dict()
        result["run_id"] = run_id
        results.append(result)
        print(
            f"rate={rate} sent={count} achieved={achieved:.2f} "
            f"duration={metrics.send_duration_seconds:.3f}"
        )

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print("REQUESTED VS ACHIEVED RATE = PASS")
    print("REPLAY VELOCITY EXPERIMENT = PASS")


if __name__ == "__main__":
    main()
