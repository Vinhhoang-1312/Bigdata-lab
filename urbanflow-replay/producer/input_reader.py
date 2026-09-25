"""Fixture JSONL and canonical S3 Parquet Silver source adapters."""

from __future__ import annotations

import csv
import json
import os
from collections.abc import Iterator
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from producer.event_mapper import SILVER_EVENT_FIELDS
from producer.event_time import NYC


def iter_fixture(path: str | Path) -> Iterator[dict[str, Any]]:
    with Path(path).open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"Invalid JSON at {path}:{line_number}") from error
            if not isinstance(row, dict):
                raise ValueError(f"Expected an object at {path}:{line_number}")
            yield row


def load_zone_ids(path: str | Path) -> set[int]:
    with Path(path).open(encoding="utf-8", newline="") as handle:
        return {int(row["LocationID"]) for row in csv.DictReader(handle)}


def _s3_filesystem(endpoint: str, region: str):
    try:
        import pyarrow.fs as fs
    except ImportError as error:
        raise RuntimeError("Integrated mode requires pyarrow") from error

    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise RuntimeError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required")
    return fs.S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        region=region,
        scheme="http",
        endpoint_override=endpoint,
        force_virtual_addressing=False,
    )


def _s3_dataset(uri: str, endpoint: str, region: str):
    try:
        import pyarrow.dataset as dataset
    except ImportError as error:
        raise RuntimeError("Integrated mode requires pyarrow") from error

    parsed = urlparse(uri)
    if parsed.scheme not in {"s3", "s3a"} or not parsed.netloc:
        raise ValueError(f"Expected an s3:// or s3a:// URI, got {uri!r}")
    path = f"{parsed.netloc}/{parsed.path.strip('/')}"
    return dataset.dataset(
        path,
        filesystem=_s3_filesystem(endpoint, region),
        format="parquet",
        partitioning="hive",
    )


def _partition_filter(start_date: str, end_date: str):
    import pyarrow.dataset as dataset

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if start > end:
        raise ValueError("Replay start date must not exceed end date")
    if (start.year, start.month) != (end.year, end.month):
        raise ValueError("Current integrated adapter expects one frozen replay month")
    end_exclusive = end + timedelta(days=1)
    start_utc = (
        datetime.combine(start, time.min, NYC).astimezone(UTC).replace(tzinfo=None)
    )
    end_utc = (
        datetime.combine(end_exclusive, time.min, NYC)
        .astimezone(UTC)
        .replace(tzinfo=None)
    )
    return (
        (dataset.field("year") == start.year)
        & (dataset.field("month") == start.month)
        & (dataset.field("pickup_datetime") >= start_utc)
        & (dataset.field("pickup_datetime") < end_utc)
    )


def integrated_row_count(
    uri: str,
    endpoint: str,
    region: str,
    start_date: str,
    end_date: str,
) -> int:
    source = _s3_dataset(uri, endpoint, region)
    return source.count_rows(filter=_partition_filter(start_date, end_date))


def iter_integrated(
    uri: str,
    endpoint: str,
    region: str,
    start_date: str,
    end_date: str,
    batch_size: int = 8192,
    batch_readahead: int = 4,
    fragment_readahead: int = 2,
) -> Iterator[dict[str, Any]]:
    """Streams Silver records using bounded-memory batch streaming.

    Uses PyArrow Dataset Scanner to yield records batch-by-batch without materializing
    the entire dataset in Python memory. Parquet timestamps (stored as UTC instants)
    are normalized to authentic New York local wall-clock time with explicit RFC 3339
    UTC offsets (e.g. -05:00 for EST, -04:00 for EDT).
    """
    source = _s3_dataset(uri, endpoint, region)
    scanner = source.scanner(
        columns=list(SILVER_EVENT_FIELDS),
        filter=_partition_filter(start_date, end_date),
        batch_size=batch_size,
        batch_readahead=batch_readahead,
        fragment_readahead=fragment_readahead,
    )
    last_pickup: datetime | None = None
    row_count = 0
    for batch in scanner.to_batches():
        for row in batch.to_pylist():
            row_count += 1
            pickup = row["pickup_datetime"]
            if not isinstance(pickup, datetime):
                raise ValueError(
                    "Integrated pickup_datetime is not a Parquet timestamp"
                )
            if pickup.tzinfo is None:
                pickup = pickup.replace(tzinfo=UTC)
            if last_pickup is not None and pickup < last_pickup:
                raise ValueError(
                    f"Monotonicity violation at row {row_count}: event time {pickup.isoformat()} "
                    f"is earlier than preceding event {last_pickup.isoformat()}. "
                    "Silver Parquet dataset is not globally sorted."
                )
            last_pickup = pickup
            row["pickup_datetime"] = pickup.astimezone(NYC).isoformat()
            yield row

