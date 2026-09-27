"""Finite replay orchestration with proportional virtual-clock acceleration."""

from __future__ import annotations

import json
import sys
import time
from collections.abc import Callable, Iterable, Iterator
from datetime import datetime
from typing import Any, Protocol

from producer.event_mapper import EventMapper
from producer.event_time import parse_event_time
from producer.event_validator import EventValidator
from producer.kafka_producer import KafkaEventProducer
from producer.producer_metrics import ProducerMetrics
from producer.rate_controller import RateController, VirtualClockController
from producer.serialization import message_key, serialize_event


class ProducerLike(Protocol):
    def send(self, topic: str, key: bytes, value: bytes, callback: Any) -> None: ...

    def flush(self, timeout: float = 30.0) -> int: ...


def record_stream(
    records: Iterable[dict[str, Any]] | Callable[[], Iterable[dict[str, Any]]],
) -> Iterator[tuple[dict[str, Any], datetime, str]]:
    """Yield each source record once without changing its historical event time.

    Replay requires chronological input for proportional pacing and fails visibly
    rather than silently reordering or rebasing canonical Silver records.
    """
    if callable(records):
        source = records()
    elif isinstance(records, (list, tuple)):
        source = sorted(
            records,
            key=lambda record: parse_event_time(record["pickup_datetime"]),
        )
    else:
        source = records
    previous_event_time: datetime | None = None
    for row_number, record in enumerate(source, start=1):
        original_time = record["pickup_datetime"]
        event_time = parse_event_time(original_time)
        if previous_event_time is not None and event_time < previous_event_time:
            raise ValueError(
                f"Monotonicity violation at row {row_number}: event time "
                f"{original_time} precedes {previous_event_time.isoformat()}"
            )
        previous_event_time = event_time
        yield record, event_time, original_time


def replay_records(
    records: Iterable[dict[str, Any]] | Callable[[], Iterable[dict[str, Any]]],
    *,
    topic: str,
    bootstrap_servers: str,
    rate: float | None = None,
    speedup: float | None = None,
    max_rate: float | None = None,
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
        for record, virtual_dt, orig_time_str in record_stream(records):
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
                    f"[REPLAY] event_time={orig_time_str} "
                    f"speedup={speedup_label} "
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
