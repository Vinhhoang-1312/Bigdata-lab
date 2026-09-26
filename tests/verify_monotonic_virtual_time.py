#!/usr/bin/env python3
"""Validates continuous virtual time replay across multiple loop cycles.

Verifies:
1. Proportional time acceleration and inter-arrival preservation.
2. Burst limiting (max-rate) behavior.
3. Monotonic timestamp rebasing across 3+ loop cycles.
4. Spark Structured Streaming watermark compatibility (no events dropped).
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add urbanflow-replay to path
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "urbanflow-replay"))

from producer.event_time import NYC
from producer.input_reader import iter_fixture
from producer.rate_controller import VirtualClockController
from producer.replay import rebased_record_stream

FIXTURE_PATH = REPO_ROOT / "urbanflow-foundation" / "fixtures" / "silver" / "silver_trips.jsonl"


def test_monotonic_rebasing_across_cycles(num_cycles: int = 5) -> None:
    print(f"=== 1. Validating Monotonic Rebasing across {num_cycles} cycles ===")
    records = list(iter_fixture(FIXTURE_PATH))
    records_per_cycle = len(records)
    total_events_to_read = records_per_cycle * num_cycles

    stream = rebased_record_stream(records, loop=True)

    previous_virtual_dt: datetime | None = None
    watermark_delay = timedelta(minutes=10)
    current_watermark: datetime | None = None

    cycle_counts: dict[int, int] = {}
    max_event_time_seen: datetime | None = None

    dropped_by_watermark_count = 0

    for i in range(total_events_to_read):
        cycle, record, virtual_dt, orig_str = next(stream)
        cycle_counts[cycle] = cycle_counts.get(cycle, 0) + 1

        # Check watermark
        if max_event_time_seen is None or virtual_dt > max_event_time_seen:
            max_event_time_seen = virtual_dt
            current_watermark = max_event_time_seen - watermark_delay

        if current_watermark is not None and virtual_dt < current_watermark:
            dropped_by_watermark_count += 1
            print(
                f"ERROR: Event {i} (cycle {cycle}) at {virtual_dt} dropped by watermark {current_watermark}!",
                file=sys.stderr,
            )

        # Monotonicity check
        if previous_virtual_dt is not None:
            assert virtual_dt >= previous_virtual_dt, (
                f"Monotonicity violation at event {i}: {virtual_dt} < {previous_virtual_dt}"
            )
            # If across cycle boundary, it must be strictly greater
            if cycle > 0 and (i % records_per_cycle) == 0:
                assert virtual_dt > previous_virtual_dt, (
                    f"Cycle boundary not strictly advancing: cycle {cycle} start {virtual_dt} "
                    f"<= cycle {cycle - 1} end {previous_virtual_dt}"
                )

        previous_virtual_dt = virtual_dt

    print(f"  Processed {total_events_to_read} events across {len(cycle_counts)} cycles.")
    for c, cnt in sorted(cycle_counts.items()):
        print(f"  - Cycle {c}: {cnt} events")
    print(f"  Events dropped by Spark watermark (10m): {dropped_by_watermark_count}")
    assert dropped_by_watermark_count == 0, "Watermark dropped events!"
    print("  Monotonic Rebasing: PASS\n")


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
        test_monotonic_rebasing_across_cycles(num_cycles=5)
        test_proportional_time_acceleration()
        test_burst_limiter()
        print("==================================================")
        print("MONOTONIC VIRTUAL TIME VALIDATION = PASS")
        print("==================================================")
        return 0
    except Exception as exc:
        print(f"VALIDATION FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
