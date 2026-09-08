#!/usr/bin/env python3
"""Publish fixture-derived events and verify them with a generic Kafka consumer."""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from producer.config import load_config
from producer.event_validator import EventValidator
from producer.input_reader import iter_fixture, load_zone_ids
from producer.replay import replay_records

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "urbanflow-replay/config/settings.yaml"
EXPECTED = ROOT / "urbanflow-foundation/fixtures/kafka/trip_events.jsonl"
ZONES = ROOT / "urbanflow-foundation/fixtures/bronze/zone_lookup.csv"


def main() -> None:
    config = load_config(CONFIG)
    metadata = AdminClient({"bootstrap.servers": config.bootstrap_servers}).list_topics(
        topic=config.topic, timeout=10
    )
    topic_metadata = metadata.topics[config.topic]
    if topic_metadata.error is not None:
        raise RuntimeError(topic_metadata.error)
    assert len(topic_metadata.partitions) == 4
    assert all(
        len(partition.replicas) == 1 for partition in topic_metadata.partitions.values()
    )
    print("KAFKA TOPIC CONTRACT = PASS")

    consumer = Consumer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "group.id": f"urbanflow-replay-test-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }
    )
    starts: list[TopicPartition] = []
    for partition_id in sorted(topic_metadata.partitions):
        partition = TopicPartition(config.topic, partition_id)
        _, high = consumer.get_watermark_offsets(partition, timeout=10)
        starts.append(TopicPartition(config.topic, partition_id, high))
    consumer.assign(starts)

    source = iter_fixture(config.fixture_path)
    metrics, interrupted = replay_records(
        source,
        topic=config.topic,
        bootstrap_servers=config.bootstrap_servers,
        rate=1000,
        max_events=7,
        valid_zone_ids=load_zone_ids(ZONES),
    )
    assert not interrupted
    assert metrics.records_sent == 7
    assert metrics.send_errors == 0

    received: list[tuple[str, dict, int]] = []
    deadline = time.monotonic() + 20
    try:
        while len(received) < 7 and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())
            received.append(
                (
                    message.key().decode("ascii"),
                    json.loads(message.value()),
                    message.partition(),
                )
            )
    finally:
        consumer.close()

    assert len(received) == 7, f"Expected 7 events, received {len(received)}"
    expected = list(iter_fixture(EXPECTED))
    assert {event["event_id"]: event for _, event, _ in received} == {
        event["event_id"]: event for event in expected
    }

    validator = EventValidator(load_zone_ids(ZONES))
    partitions_by_key: dict[str, set[int]] = {}
    for key, event, partition in received:
        assert key == str(event["pickup_zone_id"])
        assert validator.validate(event) == []
        partitions_by_key.setdefault(key, set()).add(partition)
    assert all(len(partitions) == 1 for partitions in partitions_by_key.values())

    print("GENERIC CONSUMER = PASS")
    print("MESSAGE KEYS = PASS")
    print("FIXTURE KAFKA EVENTS = PASS")
    print("REPLAY KAFKA INTEGRATION = PASS")


if __name__ == "__main__":
    main()
