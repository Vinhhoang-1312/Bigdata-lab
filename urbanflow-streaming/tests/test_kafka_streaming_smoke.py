import importlib.util
from pathlib import Path


def load_smoke_module():
    path = Path(__file__).with_name("run_kafka_streaming_smoke.py")
    spec = importlib.util.spec_from_file_location("run_kafka_streaming_smoke", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_fixture_messages_use_pickup_zone_keys_and_unique_run_marker(foundation):
    smoke = load_smoke_module()

    messages = smoke.load_fixture_messages(
        foundation / "fixtures/kafka/trip_events.jsonl",
        "run-abc",
    )

    assert len(messages) == 7
    assert {message.key for message in messages} == {"161", "236"}
    assert messages[0].event["event_id"] == "trip-20250106-080030-161-001::run-abc"
    assert messages[0].event["event_time"] == "2025-01-06T08:00:30-05:00"
    assert messages[0].value.endswith('"fare_amount":18.5}')


def test_malformed_message_keeps_run_marker_visible():
    smoke = load_smoke_module()

    message = smoke.malformed_message("run-bad")

    assert message.key == "malformed"
    assert "run-bad" in message.value
    assert message.value.startswith('{"event_id":""')
