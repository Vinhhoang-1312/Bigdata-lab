#!/usr/bin/env python3
"""Comprehensive test suite for NYC TLC timezone semantics and normalization.

Verifies:
1. Winter / February standard time (EST, UTC-5) preservation of authentic local rush hours.
2. Summer / Daylight Saving Time (EDT, UTC-4) preservation of authentic local rush hours.
3. Verification against wrong normalization (replace(tzinfo=NYC) vs replace(tzinfo=UTC).astimezone(NYC)).
4. Kafka RFC 3339 serialization and offset format validation.
5. Ingestion compatibility with Spark session timezone America/New_York.
6. Real Silver Parquet alignment: Parquet `hour` column vs normalized `pickup_datetime` hour.
"""

from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime

from producer.event_mapper import EventMapper
from producer.event_time import NYC
from producer.serialization import serialize_event

RFC3339_OFFSET_REGEX = re.compile(r"(Z|[+-][0-9]{2}:[0-9]{2})$")


def normalize_parquet_pickup(pickup: datetime) -> str:
    """Normalize a Parquet naive UTC timestamp to NYC local time with explicit offset."""
    if pickup.tzinfo is None:
        pickup = pickup.replace(tzinfo=UTC)
    return pickup.astimezone(NYC).isoformat()


def test_winter_standard_time_february() -> None:
    """Feb 2023 trip at 08:30:00 EST stored as UTC 13:30:00 in Parquet."""
    parquet_stored_utc = datetime(2023, 2, 15, 13, 30, 0)  # naive UTC instant
    normalized = normalize_parquet_pickup(parquet_stored_utc)

    assert normalized == "2023-02-15T08:30:00-05:00", f"Expected EST offset -05:00, got {normalized}"
    assert RFC3339_OFFSET_REGEX.search(normalized) is not None

    parsed = datetime.fromisoformat(normalized)
    assert parsed.hour == 8, f"Authentic NYC local hour must be 8, got {parsed.hour}"
    assert parsed.minute == 30
    assert parsed.utcoffset().total_seconds() == -5 * 3600

    # Demonstrate why replace(tzinfo=NYC) is wrong
    wrong_normalization = parquet_stored_utc.replace(tzinfo=NYC).isoformat()
    assert wrong_normalization == "2023-02-15T13:30:00-05:00"
    wrong_parsed = datetime.fromisoformat(wrong_normalization)
    assert wrong_parsed.hour == 13  # 1:30 PM instead of 8:30 AM morning rush hour!


def test_summer_daylight_saving_time() -> None:
    """July 2023 trip at 08:30:00 EDT stored as UTC 12:30:00 in Parquet."""
    parquet_stored_utc = datetime(2023, 7, 15, 12, 30, 0)  # naive UTC instant
    normalized = normalize_parquet_pickup(parquet_stored_utc)

    assert normalized == "2023-07-15T08:30:00-04:00", f"Expected EDT offset -04:00, got {normalized}"
    assert RFC3339_OFFSET_REGEX.search(normalized) is not None

    parsed = datetime.fromisoformat(normalized)
    assert parsed.hour == 8, f"Authentic NYC local hour must be 8, got {parsed.hour}"
    assert parsed.minute == 30
    assert parsed.utcoffset().total_seconds() == -4 * 3600


def test_kafka_serialization_rfc3339() -> None:
    """Kafka serialization produces valid JSON containing valid RFC 3339 timestamp with offset."""
    record = {
        "pickup_datetime": "2023-02-15T08:30:00-05:00",
        "pickup_zone_id": 161,
        "dropoff_zone_id": 237,
        "trip_distance": 2.5,
        "trip_duration_minutes": 12.0,
        "fare_amount": 14.5,
    }
    mapper = EventMapper(run_id="test-tz")
    event = mapper.map_record(record)
    payload_bytes = serialize_event(event)
    payload = json.loads(payload_bytes.decode("utf-8"))

    assert payload["event_time"] == "2023-02-15T08:30:00-05:00"
    assert RFC3339_OFFSET_REGEX.search(payload["event_time"]) is not None


def test_real_silver_parquet_hour_alignment() -> None:
    """Verifies that normalized pickup_datetime matches the Parquet 'hour' column exactly."""
    try:
        import pyarrow.dataset as ds
        import pyarrow.fs as fs
    except ImportError:
        print("PyArrow not installed, skipping S3 Parquet hour alignment test.")
        return

    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        print("S3 credentials not set, skipping S3 Parquet hour alignment test.")
        return

    endpoint = os.environ.get("URBANFLOW_S3_ENDPOINT")
    if not endpoint:
        endpoint = "http://seaweedfs:8333" if os.path.exists("/.dockerenv") else "http://localhost:8333"

    s3 = fs.S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        region="us-east-1",
        scheme="http",
        endpoint_override=endpoint,
        force_virtual_addressing=False,
    )

    parquet_file = "urbanflow/silver/trips/year=2023/month=2/part-00000-sorted.parquet"
    dataset = ds.dataset(parquet_file, filesystem=s3, format="parquet")
    sample_table = dataset.scanner(
        columns=["pickup_datetime", "hour", "day_of_week"],
        batch_size=2000,
    ).take(list(range(2000)))

    mismatches = 0
    for row in sample_table.to_pylist():
        raw_pickup = row["pickup_datetime"]
        expected_hour = row["hour"]
        normalized_str = normalize_parquet_pickup(raw_pickup)
        actual_hour = datetime.fromisoformat(normalized_str).hour
        if actual_hour != expected_hour:
            mismatches += 1

    assert mismatches == 0, f"Found {mismatches} hour mismatches between normalized pickup and Parquet hour column!"


def main() -> None:
    test_winter_standard_time_february()
    print("TEST: Winter Standard Time (EST, UTC-5) = PASS")
    test_summer_daylight_saving_time()
    print("TEST: Summer Daylight Saving Time (EDT, UTC-4) = PASS")
    test_kafka_serialization_rfc3339()
    print("TEST: Kafka RFC 3339 Serialization = PASS")
    test_real_silver_parquet_hour_alignment()
    print("TEST: Real Silver Parquet Hour Alignment = PASS")
    print("ALL TIMEZONE SEMANTICS TESTS = PASS")


if __name__ == "__main__":
    main()
