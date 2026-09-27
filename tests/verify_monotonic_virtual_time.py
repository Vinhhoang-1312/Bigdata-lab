#!/usr/bin/env python3
"""Validate finite replay event-time preservation and virtual-clock pacing.

Verifies:
1. Historical event timestamps are emitted once and remain unchanged.
2. Proportional time acceleration and inter-arrival preservation.
3. Burst limiting (max-rate) behavior.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

# Add urbanflow-replay to path
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "urbanflow-replay"))

from producer.event_time import NYC  # noqa: E402
from producer.input_reader import iter_fixture  # noqa: E402
from producer.rate_controller import VirtualClockController  # noqa: E402
from producer.replay import record_stream  # noqa: E402

FIXTURE_PATH = REPO_ROOT / "urbanflow-foundation" / "fixtures" / "silver" / "silver_trips.jsonl"


def test_finite_event_time_preservation() -> None:
    print("=== 1. Validating finite replay event-time preservation ===")
    records = list(iter_fixture(FIXTURE_PATH))
    emitted = list(record_stream(records))
    expected = sorted(
        records,
        key=lambda record: datetime.fromisoformat(record["pickup_datetime"]),
    )

    assert len(emitted) == len(expected)
    previous_event_time: datetime | None = None
    for source, (record, event_time, original_time) in zip(
        expected, emitted, strict=True
    ):
        assert record == source
        assert original_time == source["pickup_datetime"]
        assert event_time.isoformat() == source["pickup_datetime"]
        if previous_event_time is not None:
            assert event_time >= previous_event_time
        previous_event_time = event_time

    print(f"  Emitted {len(emitted)} source events exactly once.")
    print("  Historical event times preserved: PASS\n")


def test_proportional_time_acceleration() -> None:
    print("=== 2. Validating Proportional Virtual Acceleration ===")
    speedups = [10.0, 60.0, 300.0]

    for speedup in speedups:
        slept_intervals: list[float] = []

        class FakeClock:
            def __init__(self) -> None:
                self.now = 0.0

            def clock(self) -> float:
                return self.now

            def sleep(self, dur: float) -> None:
                slept_intervals.append(dur)
                self.now += dur

        clock = FakeClock()
        controller = VirtualClockController(speedup=speedup, clock=clock.clock, sleeper=clock.sleep)

        # Event times 5 minutes (300s) apart
        t0 = datetime(2023, 2, 1, 12, 0, 0, tzinfo=NYC)
        t1 = datetime(2023, 2, 1, 12, 5, 0, tzinfo=NYC)

        controller.wait_for_event(t0)
        assert len(slept_intervals) == 0  # First event does not sleep

        controller.wait_for_event(t1)
        expected_wall_sleep = 300.0 / speedup
        assert len(slept_intervals) == 1
        assert abs(slept_intervals[0] - expected_wall_sleep) < 1e-6, (
            f"Expected {expected_wall_sleep}s sleep at {speedup}x, got {slept_intervals[0]}s"
        )
        print(f"  Speedup {speedup:5.1f}x: 5 min event time -> {slept_intervals[0]:.2f}s wall time (PASS)")

    print("  Proportional Acceleration: PASS\n")


def test_burst_limiter() -> None:
    print("=== 3. Validating Burst Limiter (max-rate) ===")
    max_rate = 500.0  # min interval = 2ms
    min_interval = 1.0 / max_rate

    slept: list[float] = []

    class FakeClock:
        def __init__(self) -> None:
            self.now = 0.0

        def clock(self) -> float:
            return self.now

        def sleep(self, dur: float) -> None:
            slept.append(dur)
            self.now += dur

    clock = FakeClock()
    controller = VirtualClockController(
        speedup=60.0, max_rate=max_rate, clock=clock.clock, sleeper=clock.sleep
    )

    t0 = datetime(2023, 2, 1, 12, 0, 0, tzinfo=NYC)
    # Burst of 10 events with identical timestamp
    controller.wait_for_event(t0)
    for _ in range(10):
        controller.wait_for_event(t0)

    assert len(slept) == 10
    for i, s in enumerate(slept):
        assert abs(s - min_interval) < 1e-6, f"Burst event {i} sleep {s} != {min_interval}"

    print(f"  Max rate {max_rate:.0f} eps capped {len(slept)} burst events to {min_interval*1000:.1f}ms each")
    print("  Burst Limiter: PASS\n")


def main() -> int:
    try:
        test_finite_event_time_preservation()
        test_proportional_time_acceleration()
        test_burst_limiter()
        print("==================================================")
        print("FINITE VIRTUAL TIME VALIDATION = PASS")
        print("==================================================")
        return 0
    except Exception as exc:
        print(f"VALIDATION FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
