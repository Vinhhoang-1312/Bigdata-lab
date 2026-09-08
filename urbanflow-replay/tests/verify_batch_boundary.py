#!/usr/bin/env python3
"""Verify direct Batch Silver → Replay → Kafka integration."""

from __future__ import annotations

import itertools
import json
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from producer.config import load_config
from producer.event_mapper import EVENT_FIELDS, EventMapper
from producer.event_validator import EventValidator
from producer.input_reader import integrated_row_count, iter_integrated
from producer.replay import replay_records

REPLAY = Path(__file__).resolve().parents[1]
CONFIG = REPLAY / "config/settings.yaml"
EXPECTED_REPLAY_ROWS = 2_888_729
SAMPLE_ROWS = 1_000
PUBLISHED_ROWS = 100


def consumer_at_end(bootstrap_servers: str, topic: str) -> Consumer:
    metadata = AdminClient({"bootstrap.servers": bootstrap_servers}).list_topics(
        topic=topic, timeout=10
    )
    partitions = metadata.topics[topic].partitions
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"urbanflow-batch-boundary-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    positions = []
    for partition_id in sorted(partitions):
        partition = TopicPartition(topic, partition_id)
        _, high = consumer.get_watermark_offsets(partition, timeout=10)
        positions.append(TopicPartition(topic, partition_id, high))
    consumer.assign(positions)
    return consumer


def integrated_records(config):
    return iter_integrated(
        config.integrated_silver_uri,
        config.s3_endpoint,
        config.s3_region,
        config.replay_start_date,
        config.replay_end_date,
    )


def main() -> None:
    config = load_config(CONFIG)
    row_count = integrated_row_count(
        config.integrated_silver_uri,
        config.s3_endpoint,
        config.s3_region,
        config.replay_start_date,
        config.replay_end_date,
    )
    assert row_count == EXPECTED_REPLAY_ROWS
    print(f"batch_replay_rows={row_count}")

    sample = list(itertools.islice(integrated_records(config), SAMPLE_ROWS))
    assert len(sample) == SAMPLE_ROWS
    mapper = EventMapper()
    validator = EventValidator()
    expected_events = []
    for row in sample:
        event = mapper.map_record(row)
        assert validator.validate(event) == []
        assert tuple(event) == EVENT_FIELDS
        assert event["event_time"].startswith("2023-02-")
        assert event["event_time"].endswith("-05:00")
        expected_events.append(event)
    assert len({event["event_id"] for event in expected_events}) == SAMPLE_ROWS
    print("DIRECT SILVER PARQUET = PASS")
    print("INTEGRATED EVENT CONTRACT = PASS")

    consumer = consumer_at_end(config.bootstrap_servers, config.topic)
    metrics, interrupted = replay_records(
        sample[:PUBLISHED_ROWS],
        topic=config.topic,
        bootstrap_servers=config.bootstrap_servers,
        rate=1000,
        max_events=PUBLISHED_ROWS,
    )
    assert not interrupted
    assert metrics.records_sent == PUBLISHED_ROWS
    assert metrics.records_rejected == 0
    assert metrics.send_errors == 0

    received: list[tuple[str, dict]] = []
    deadline = time.monotonic() + 20
    try:
        while len(received) < PUBLISHED_ROWS and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())
            received.append(
                (message.key().decode("ascii"), json.loads(message.value()))
            )
    finally:
        consumer.close()

    assert len(received) == PUBLISHED_ROWS
    expected = {event["event_id"]: event for event in expected_events[:PUBLISHED_ROWS]}
    assert {event["event_id"]: event for _, event in received} == expected
    assert all(key == str(event["pickup_zone_id"]) for key, event in received)

    print(f"published_batch_rows={metrics.records_sent}")
    print("BATCH SILVER TO KAFKA = PASS")
    print("BATCH TO REPLAY INTEGRATION = PASS")


if __name__ == "__main__":
    main()
