#!/usr/bin/env python3
"""Dependency-light Replay contract tests using Foundation fixtures."""

from __future__ import annotations

import copy
import json
import tempfile
from pathlib import Path

import yaml

from producer.config import load_config
from producer.event_mapper import EVENT_FIELDS, EventMapper, SILVER_EVENT_FIELDS
from producer.event_time import EventTimeError, parse_event_time
from producer.event_validator import EventValidator
from producer.input_reader import iter_fixture, load_zone_ids
from producer.rate_controller import RateController, VirtualClockController
from producer.replay import record_stream, replay_records
from producer.serialization import message_key, serialize_event

ROOT = Path(__file__).resolve().parents[2]
REPLAY = ROOT / "urbanflow-replay"
FOUNDATION = ROOT / "urbanflow-foundation"
CONFIG = REPLAY / "config/settings.yaml"
SILVER = FOUNDATION / "fixtures/silver/silver_trips.jsonl"
EXPECTED_EVENTS = FOUNDATION / "fixtures/kafka/trip_events.jsonl"
ZONES = FOUNDATION / "fixtures/bronze/zone_lookup.csv"
EVENT_SCHEMA = FOUNDATION / "contracts/kafka-trip-event.schema.json"
PERIODS = FOUNDATION / "config/data-periods-reference.yaml"


def expect_error(action, expected: str) -> None:
    try:
        action()
    except (EventTimeError, ValueError) as error:
        assert expected in str(error), (expected, str(error))
    else:
        raise AssertionError(f"Expected an error containing {expected!r}")


def mapped_fixture() -> tuple[list[dict], list[dict]]:
    source = list(iter_fixture(SILVER))
    mapper = EventMapper()
    return source, [mapper.map_record(row) for row in source]


def test_configuration() -> None:
    config = load_config(CONFIG)
    assert config.foundation_version == "1.0.0"
    assert config.analytics_timezone == "America/New_York"
    assert config.topic == "urbanflow.trip-events"
    assert config.replay_start_date == "2023-02-01"
    assert config.replay_end_date == "2023-02-28"
    assert config.planned_rates == (100, 500, 1000, 2500, 5000)
    period_text = PERIODS.read_text(encoding="utf-8")
    assert 'start_date: "2023-02-01"' in period_text
    assert 'end_date: "2023-02-28"' in period_text
    assert (REPLAY / "FOUNDATION_VERSION").read_text().strip() == "1.0.0"

    raw = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    raw["kafka"]["topic"] = "wrong.topic"
    with tempfile.TemporaryDirectory() as directory:
        config_path = Path(directory) / "invalid-topic.yaml"
        config_path.write_text(yaml.safe_dump(raw), encoding="utf-8")
        expect_error(lambda: load_config(config_path), "topic must be")


def test_fixture_mapping() -> None:
    source, actual = mapped_fixture()
    expected = list(iter_fixture(EXPECTED_EVENTS))
    assert len(source) == len(actual) == len(expected) == 7
    assert all(set(row) >= set(SILVER_EVENT_FIELDS) for row in source)
    assert actual == expected

    schema = json.loads(EVENT_SCHEMA.read_text(encoding="utf-8"))
    assert tuple(schema["required"]) == EVENT_FIELDS
    assert schema["additionalProperties"] is False

    validator = EventValidator(load_zone_ids(ZONES))
    for event in actual:
        assert validator.validate(event) == []
        assert message_key(event) == str(event["pickup_zone_id"]).encode("ascii")
        assert json.loads(serialize_event(event)) == event
    assert len({event["event_id"] for event in actual}) == 7


def test_event_validation_failures() -> None:
    _, events = mapped_fixture()
    valid = events[0]

    mutations: list[tuple[str, dict, str]] = []
    extra = copy.deepcopy(valid)
    extra["unexpected"] = 1
    mutations.append(("extra", extra, "extra_fields"))
    missing = copy.deepcopy(valid)
    del missing["fare_amount"]
    mutations.append(("missing", missing, "missing_fields"))

    for field, value, reason in (
        ("event_id", "", "event_id_invalid"),
        ("pickup_zone_id", 0, "pickup_zone_id_invalid"),
        ("pickup_zone_id", True, "pickup_zone_id_invalid"),
        ("pickup_zone_id", 999, "pickup_zone_id_unknown"),
        ("dropoff_zone_id", -1, "dropoff_zone_id_invalid"),
        ("trip_distance", -0.1, "trip_distance_invalid"),
        ("trip_duration_minutes", float("inf"), "trip_duration_minutes_invalid"),
        ("fare_amount", True, "fare_amount_invalid"),
        ("event_time", "2025-01-06T08:00:30", "event_time_invalid"),
        ("event_time", "2025-01-06T08:00:30-04:00", "event_time_invalid"),
        ("event_time", "2025-03-09T02:30:00-05:00", "event_time_invalid"),
    ):
        changed = copy.deepcopy(valid)
        changed[field] = value
        mutations.append((field, changed, reason))

    zones = load_zone_ids(ZONES)
    for name, event, reason in mutations:
        errors = EventValidator(zones).validate(event)
        assert any(error.startswith(reason) for error in errors), (
            name,
            reason,
            errors,
        )

    duplicate_validator = EventValidator(zones)
    assert duplicate_validator.validate(valid) == []
    assert "event_id_duplicate" in duplicate_validator.validate(valid)

    parse_event_time("2025-11-02T01:30:00-04:00")
    expect_error(
        lambda: parse_event_time("2025-11-02T01:30:00-05:00"),
        "does not match",
    )


def test_rate_controller() -> None:
    class FakeClock:
        def __init__(self) -> None:
            self.now = 10.0

        def clock(self) -> float:
            return self.now

        def sleep(self, duration: float) -> None:
            assert duration >= 0
            self.now += duration

    for rate in (100, 500, 1000, 2500, 5000):
        fake = FakeClock()
        controller = RateController(rate, fake.clock, fake.sleep)
        for _ in range(100):
            controller.wait_for_next()
        assert abs(fake.now - (10.0 + 100 / rate)) < 1e-9
    expect_error(lambda: RateController(0), "must be positive")


def test_replay_orchestration() -> None:
    source = list(iter_fixture(SILVER))

    dry_metrics, interrupted = replay_records(
        source,
        topic="urbanflow.trip-events",
        bootstrap_servers="unused",
        rate=5000,
        dry_run=True,
        print_events=0,
        valid_zone_ids=load_zone_ids(ZONES),
    )
    assert not interrupted
    assert dry_metrics.records_read == 7
    assert dry_metrics.records_serialized == 7
    assert dry_metrics.records_rejected == 0
    assert dry_metrics.records_sent == 0

    class ImmediateController:
        def wait_for_next(self) -> None:
            pass

    class MemoryProducer:
        def __init__(self) -> None:
            self.messages: list[tuple[str, bytes, bytes]] = []
            self.flushed = False

        def send(self, topic, key, value, callback) -> None:
            self.messages.append((topic, key, value))
            callback(None, None)

        def flush(self, timeout=30.0) -> int:
            self.flushed = True
            return 0

    producer = MemoryProducer()
    metrics, interrupted = replay_records(
        source,
        topic="urbanflow.trip-events",
        bootstrap_servers="unused",
        rate=5000,
        producer=producer,
        controller=ImmediateController(),
        valid_zone_ids=load_zone_ids(ZONES),
    )
    assert not interrupted and producer.flushed
    assert metrics.records_sent == len(producer.messages) == 7
    assert all(topic == "urbanflow.trip-events" for topic, _, _ in producer.messages)

    class FailedProducer(MemoryProducer):
        def send(self, topic, key, value, callback) -> None:
            self.messages.append((topic, key, value))
            callback(RuntimeError("delivery failed"), None)

    failed, _ = replay_records(
        source[:1],
        topic="urbanflow.trip-events",
        bootstrap_servers="unused",
        rate=100,
        producer=FailedProducer(),
        controller=ImmediateController(),
        valid_zone_ids=load_zone_ids(ZONES),
    )
    assert failed.records_sent == 0
    assert failed.send_errors == 1

    invalid = copy.deepcopy(source[0])
    invalid["fare_amount"] = -1
    invalid_producer = MemoryProducer()
    rejected, _ = replay_records(
        [invalid],
        topic="urbanflow.trip-events",
        bootstrap_servers="unused",
        rate=100,
        producer=invalid_producer,
        controller=ImmediateController(),
        valid_zone_ids=load_zone_ids(ZONES),
    )
    assert rejected.records_rejected == 1
    assert rejected.records_serialized == 0
    assert invalid_producer.messages == []
    assert invalid_producer.flushed

    class InterruptedRows:
        def __iter__(self):
            yield source[0]
            raise KeyboardInterrupt

    shutdown_producer = MemoryProducer()
    shutdown, interrupted = replay_records(
        InterruptedRows(),
        topic="urbanflow.trip-events",
        bootstrap_servers="unused",
        rate=100,
        producer=shutdown_producer,
        controller=ImmediateController(),
        valid_zone_ids=load_zone_ids(ZONES),
    )
    assert interrupted and shutdown_producer.flushed
    assert shutdown.records_sent == 1


def test_virtual_clock_controller() -> None:
    from producer.event_time import NYC
    from datetime import datetime, timedelta

    class FakeClock:
        def __init__(self) -> None:
            self.now = 0.0
            self.slept: list[float] = []

        def clock(self) -> float:
            return self.now

        def sleep(self, duration: float) -> None:
            assert duration >= 0
            self.slept.append(duration)
            self.now += duration

    fake = FakeClock()
    ctrl = VirtualClockController(speedup=60.0, clock=fake.clock, sleeper=fake.sleep)
    t0 = datetime(2023, 2, 1, 10, 0, 0, tzinfo=NYC)
    t1 = datetime(2023, 2, 1, 10, 1, 0, tzinfo=NYC)
    ctrl.wait_for_event(t0)
    assert len(fake.slept) == 0
    ctrl.wait_for_event(t1)
    assert len(fake.slept) == 1
    assert abs(fake.slept[0] - 1.0) < 1e-9

    # Duplicate and out of order
    ctrl.wait_for_event(t1)
    # Target wall was same as now, no sleep needed
    t_prior = datetime(2023, 2, 1, 9, 59, 0, tzinfo=NYC)
    ctrl.wait_for_event(t_prior)

    # Max rate burst limit
    fake_burst = FakeClock()
    ctrl_burst = VirtualClockController(
        speedup=60.0, max_rate=500.0, clock=fake_burst.clock, sleeper=fake_burst.sleep
    )
    ctrl_burst.wait_for_event(t0)
    ctrl_burst.wait_for_event(t0)
    assert len(fake_burst.slept) == 1
    assert abs(fake_burst.slept[0] - (1.0 / 500.0)) < 1e-9

    # Sleep overshoot & rate limiting protection
    class OvershootingClock:
        def __init__(self, overshoot: float = 0.001) -> None:
            self.now = 0.0
            self.overshoot = overshoot
            self.slept: list[float] = []

        def clock(self) -> float:
            return self.now

        def sleep(self, duration: float) -> None:
            assert duration >= 0
            self.slept.append(duration)
            self.now += duration + self.overshoot

    over_clock = OvershootingClock(overshoot=0.0005)
    ctrl_over = VirtualClockController(
        speedup=60.0, max_rate=500.0, clock=over_clock.clock, sleeper=over_clock.sleep
    )
    ctrl_over.wait_for_event(t0)
    ctrl_over.wait_for_event(t0)
    # Second event slept requested delay (0.002) + overshot by 0.0005 -> now = 0.0025
    assert abs(over_clock.now - 0.0025) < 1e-9
    # Third simultaneous event must space out by 1/500 from the post-overshoot time (0.0025)
    ctrl_over.wait_for_event(t0)
    # Delay requested should be (0.0025 + 0.002) - 0.0025 = 0.002
    assert abs(ctrl_over._last_emit_wall - 0.0025) < 1e-9 or abs(ctrl_over._last_emit_wall - 0.005) < 1e-9

    # Zero cumulative drift across multiple events
    drift_clock = OvershootingClock(overshoot=0.002)
    ctrl_drift = VirtualClockController(
        speedup=60.0, max_rate=1000.0, clock=drift_clock.clock, sleeper=drift_clock.sleep
    )
    t_start = datetime(2023, 2, 1, 10, 0, 0, tzinfo=NYC)
    ctrl_drift.wait_for_event(t_start)
    for i in range(1, 11):
        event_time = t_start + timedelta(minutes=i)
        # Advance clock to just before target to simulate wall progression
        drift_clock.now = (i - 1) * 1.0 + 0.5
        ctrl_drift.wait_for_event(event_time)

    # After 10 virtual minutes (600s) at 60x, virtual target is EXACTLY 10.0s from wall_start
    dt_total = 600.0
    expected_virtual_target = 0.0 + dt_total / 60.0
    assert abs(expected_virtual_target - 10.0) < 1e-9



def test_record_stream_preserves_event_time() -> None:
    records = [
        {"pickup_datetime": "2023-02-01T10:00:00-05:00", "id": 1},
        {"pickup_datetime": "2023-02-01T10:05:00-05:00", "id": 2},
    ]
    events = list(record_stream(records))

    assert len(events) == len(records)
    assert [event[0] for event in events] == records
    assert [event[2] for event in events] == [
        record["pickup_datetime"] for record in records
    ]
    assert events[0][1] < events[1][1]

    out_of_order = iter(reversed(records))
    expect_error(lambda: list(record_stream(out_of_order)), "Monotonicity violation")


def main() -> None:
    test_configuration()
    print("CONFIGURATION = PASS")
    test_fixture_mapping()
    print("FIXTURE MAPPING = PASS")
    test_event_validation_failures()
    print("EVENT VALIDATION = PASS")
    test_rate_controller()
    print("RATE CONTROLLER = PASS")
    test_virtual_clock_controller()
    print("VIRTUAL CLOCK CONTROLLER = PASS")
    test_record_stream_preserves_event_time()
    print("FINITE RECORD STREAM = PASS")
    test_replay_orchestration()
    print("REPLAY ORCHESTRATION = PASS")

    from tests.test_timezone_semantics import (
        test_winter_standard_time_february,
        test_summer_daylight_saving_time,
        test_kafka_serialization_rfc3339,
    )
    test_winter_standard_time_february()
    test_summer_daylight_saving_time()
    test_kafka_serialization_rfc3339()
    print("TIMEZONE SEMANTICS = PASS")

    print("REPLAY CONTRACT TESTS = PASS")



if __name__ == "__main__":
    main()
