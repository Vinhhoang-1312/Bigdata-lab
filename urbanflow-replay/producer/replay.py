"""Replay orchestration independent of CLI and source format."""

from __future__ import annotations

import json
import sys
from collections.abc import Iterable
from typing import Any, Protocol

from producer.event_mapper import EventMapper
from producer.event_validator import EventValidator
from producer.kafka_producer import KafkaEventProducer
from producer.producer_metrics import ProducerMetrics
from producer.rate_controller import RateController
from producer.serialization import message_key, serialize_event


class ProducerLike(Protocol):
    def send(self, topic: str, key: bytes, value: bytes, callback: Any) -> None: ...

    def flush(self, timeout: float = 30.0) -> int: ...


def replay_records(
    records: Iterable[dict[str, Any]],
    *,
    topic: str,
    bootstrap_servers: str,
    rate: float,
    max_events: int | None = None,
    dry_run: bool = False,
    print_events: int = 0,
    valid_zone_ids: set[int] | None = None,
    event_id_run_id: str | None = None,
    producer: ProducerLike | None = None,
    controller: RateController | None = None,
) -> tuple[ProducerMetrics, bool]:
    if max_events is not None and max_events <= 0:
        raise ValueError("max_events must be positive")
    metrics = ProducerMetrics(requested_events_per_second=rate)
    # EventMapper's per-zone monotonic sequence guarantees run-level uniqueness
    # without retaining millions of IDs during a full replay.
    mapper = EventMapper(event_id_run_id)
    validator = EventValidator(valid_zone_ids, track_duplicates=False)
    pace = controller or RateController(rate)
    client = producer
    if not dry_run and client is None:
        client = KafkaEventProducer(bootstrap_servers)
    interrupted = False

    def delivered(error: object, _message: object) -> None:
        if error is None:
            metrics.records_sent += 1
        else:
            metrics.send_errors += 1

    try:
        for record in records:
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
                pace.wait_for_next()
                assert client is not None
                client.send(topic, key, payload, delivered)

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
