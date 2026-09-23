"""Replay orchestration with proportional virtual clock acceleration and monotonic looping."""

from __future__ import annotations

import json
import sys
import time
from collections.abc import Callable, Iterable, Iterator
from datetime import datetime, timedelta
from typing import Any, Protocol

from producer.event_mapper import EventMapper
from producer.event_time import NYC, parse_event_time
from producer.event_validator import EventValidator
from producer.kafka_producer import KafkaEventProducer
from producer.producer_metrics import ProducerMetrics
from producer.rate_controller import RateController, VirtualClockController
from producer.serialization import message_key, serialize_event


class ProducerLike(Protocol):
    def send(self, topic: str, key: bytes, value: bytes, callback: Any) -> None: ...

    def flush(self, timeout: float = 30.0) -> int: ...


def rebased_record_stream(
    records: Iterable[dict[str, Any]] | Callable[[], Iterable[dict[str, Any]]],
    loop: bool = False,
) -> Iterator[tuple[int, dict[str, Any], datetime, str]]:
    """Yields (cycle, record_dict, virtual_dt, orig_time_str) with strictly monotonic rebasing.

    Supports re-scanning factories or in-memory sequences to maintain bounded-memory
    batch streaming during continuous looping across millions of events without
    materializing the full dataset in Python memory.
    """
    min_dt: datetime | None = None
    max_dt: datetime | None = None

    if callable(records):
        cycle0_iter = records()
        get_next_cycle: Callable[[], Iterable[dict[str, Any]]] | None = records
        fallback_cache = None
    elif isinstance(records, (list, tuple)):
        sorted_list = sorted(records, key=lambda r: str(r.get("pickup_datetime", "")))
        cycle0_iter = sorted_list
        get_next_cycle = lambda: sorted_list
        fallback_cache = None
    else:
        cycle0_iter = records
        fallback_cache = [] if loop else None
        get_next_cycle = None

    for r in cycle0_iter:
        orig_str = r["pickup_datetime"]
        orig_dt = parse_event_time(orig_str)
        if min_dt is None or orig_dt < min_dt:
            min_dt = orig_dt
        if max_dt is None or orig_dt > max_dt:
            max_dt = orig_dt
        if fallback_cache is not None:
            fallback_cache.append(r)
        yield 0, r, orig_dt, orig_str

    if not loop or min_dt is None or max_dt is None:
        return

    span = max(timedelta(seconds=1), max_dt - min_dt)
    boundary = timedelta(minutes=1) if span >= timedelta(minutes=1) else timedelta(seconds=1)
    cycle_span = span + boundary

    if get_next_cycle is None:
        assert fallback_cache is not None
        get_next_cycle = lambda: fallback_cache

    cycle = 1
    while True:
        offset = cycle * cycle_span
        for orig_r in get_next_cycle():
            orig_str = orig_r["pickup_datetime"]
            orig_dt = parse_event_time(orig_str)
            virtual_dt = (orig_dt + offset).astimezone(NYC)
            new_r = dict(orig_r)
            new_r["pickup_datetime"] = virtual_dt.isoformat()
            yield cycle, new_r, virtual_dt, orig_str
        cycle += 1


def replay_records(
    records: Iterable[dict[str, Any]] | Callable[[], Iterable[dict[str, Any]]],
    *,
    topic: str,
    bootstrap_servers: str,
    rate: float | None = None,
    speedup: float | None = None,
    max_rate: float | None = None,
    loop: bool = False,
    max_events: int | None = None,
    dry_run: bool = False,
    print_events: int = 0,
    valid_zone_ids: set[int] | None = None,
    event_id_run_id: str | None = None,
    producer: ProducerLike | None = None,
    controller: Any | None = None,
    log_interval_seconds: float = 5.0,
) -> tuple[ProducerMetrics, bool]:
    if max_events is not None and max_events <= 0:
        raise ValueError("max_events must be positive")

    effective_rate = rate if rate is not None else 0.0
    metrics = ProducerMetrics(requested_events_per_second=effective_rate)

    if controller is not None:
        pace = controller
    elif rate is not None and speedup is None:
        pace = RateController(rate)
    elif speedup is not None:
        effective_max = max_rate if max_rate is not None else max(500.0, 4.0 * speedup)
        pace = VirtualClockController(speedup=speedup, max_rate=effective_max)
    elif rate is not None:
        pace = RateController(rate)
    else:
        speedup = 60.0
        effective_max = max_rate if max_rate is not None else max(500.0, 4.0 * speedup)
        pace = VirtualClockController(speedup=speedup, max_rate=effective_max)


    mapper = EventMapper(event_id_run_id)
    validator = EventValidator(valid_zone_ids, track_duplicates=False)
    client = producer
    if not dry_run and client is None:
        client = KafkaEventProducer(bootstrap_servers)
    interrupted = False

    def delivered(error: object, _message: object) -> None:
        if error is None:
            metrics.records_sent += 1
        else:
            metrics.send_errors += 1

    last_log_wall = time.perf_counter()

    try:
        for cycle, record, virtual_dt, orig_time_str in rebased_record_stream(records, loop=loop):
            metrics.records_read += 1
            try:
                event = mapper.map_record(record)
                errors = validator.validate(event)
                if errors:
                    raise ValueError(",".join(errors))
                payload = serialize_event(event)
                key = message_key(event)
            except (KeyError, TypeError, ValueError) as error:
                metrics.records_rejected += 1
                print(
                    f"rejected_record={metrics.records_read} reason={error}",
                    file=sys.stderr,
                )
                continue

            metrics.records_serialized += 1
            if metrics.records_serialized <= print_events:
                print(
                    json.dumps(
                        {"key": key.decode("ascii"), "value": event},
                        separators=(",", ":"),
                    )
                )

            if not dry_run:
                if hasattr(pace, "wait_for_event"):
                    pace.wait_for_event(virtual_dt)
                elif hasattr(pace, "wait_for_next"):
                    pace.wait_for_next()
                assert client is not None
                client.send(topic, key, payload, delivered)

            now_wall = time.perf_counter()
            if now_wall - last_log_wall >= log_interval_seconds:
                elapsed = max(1e-6, now_wall - metrics._start_clock)
                effective_rate = metrics.records_sent / elapsed
                speedup_label = f"{speedup:.1f}x" if speedup is not None else "fixed"
                print(
                    f"[REPLAY] cycle={cycle} orig_time={orig_time_str} "
                    f"virtual_time={event['event_time']} speedup={speedup_label} "
                    f"rate={effective_rate:.1f}eps total_sent={metrics.records_sent}",
                    flush=True,
                )
                last_log_wall = now_wall

            if max_events is not None and metrics.records_serialized >= max_events:
                break
    except KeyboardInterrupt:
        interrupted = True
    finally:
        if client is not None:
            remaining = client.flush(30.0)
            metrics.send_errors += remaining
        metrics.finish()

    return metrics, interrupted
