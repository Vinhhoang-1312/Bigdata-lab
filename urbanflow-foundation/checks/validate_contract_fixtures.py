#!/usr/bin/env python3
"""
UrbanFlow Foundation — Contract Fixture Validator
Foundation version: 1.0.0

Validates the supplied JSONL/CSV fixtures and core cross-contract
invariants using only the Python standard library.

Usage:
    python checks/validate_contract_fixtures.py

Expected result:
    FOUNDATION FIXTURES = PASS

Run from the urbanflow-foundation root directory.
"""

import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FOUNDATION_VERSION = "1.0.0"

# Resolve paths relative to the script's parent directory (urbanflow-foundation/)
BASE_DIR = Path(__file__).resolve().parent.parent

FIXTURE_PATHS = {
    "bronze_source":      BASE_DIR / "fixtures" / "bronze" / "source_trips.csv",
    "silver_trips":       BASE_DIR / "fixtures" / "silver" / "silver_trips.jsonl",
    "kafka_events":       BASE_DIR / "fixtures" / "kafka"  / "trip_events.jsonl",
    "historical_baseline": BASE_DIR / "fixtures" / "gold"  / "historical_zone_baseline.jsonl",
    "expected_live":      BASE_DIR / "fixtures" / "gold"   / "expected_live_zone_metrics.jsonl",
}

VERSION_FILE = BASE_DIR / "VERSION"

# ---------------------------------------------------------------------------
# Silver contract fields
# ---------------------------------------------------------------------------

SILVER_REQUIRED_FIELDS = {
    "pickup_datetime", "dropoff_datetime",
    "pickup_zone_id", "dropoff_zone_id",
    "trip_distance", "fare_amount", "trip_duration_minutes",
    "year", "month", "day_of_week", "hour", "five_minute_slot",
}

SILVER_OPTIONAL_FIELDS = {"passenger_count", "total_amount"}

SILVER_ALL_FIELDS = SILVER_REQUIRED_FIELDS | SILVER_OPTIONAL_FIELDS

# ---------------------------------------------------------------------------
# Kafka event contract fields
# ---------------------------------------------------------------------------

KAFKA_REQUIRED_FIELDS = {
    "event_id", "event_time",
    "pickup_zone_id", "dropoff_zone_id",
    "trip_distance", "trip_duration_minutes", "fare_amount",
}

# ---------------------------------------------------------------------------
# Historical baseline contract fields
# ---------------------------------------------------------------------------

BASELINE_REQUIRED_FIELDS = {
    "pickup_zone_id", "day_of_week", "hour", "five_minute_slot",
    "historical_mean_trip_count", "historical_std_trip_count",
}

BASELINE_OPTIONAL_FIELDS = {"historical_mean_distance", "historical_mean_duration"}

# ---------------------------------------------------------------------------
# Live metrics contract fields
# ---------------------------------------------------------------------------

LIVE_REQUIRED_FIELDS = {
    "window_start", "window_end", "pickup_zone_id", "current_trip_count",
}

LIVE_NULLABLE_FIELDS = {
    "historical_mean_trip_count", "demand_ratio", "z_score",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class ValidationError(Exception):
    """Raised when a fixture fails a contract check."""


def load_jsonl(path: Path) -> list[dict]:
    """Load a JSONL file and return a list of dicts."""
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise ValidationError(
                    f"{path.name}:{lineno}: invalid JSON: {e}"
                ) from e
    return rows


def load_csv(path: Path) -> list[dict]:
    """Load a CSV file and return a list of dicts."""
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def check_required_fields(row: dict, required: set, source: str, lineno: int):
    """Verify all required fields are present and non-null."""
    missing = required - set(row.keys())
    if missing:
        raise ValidationError(
            f"{source}:{lineno}: missing required fields: {sorted(missing)}"
        )
    for field in required:
        if row[field] is None:
            raise ValidationError(
                f"{source}:{lineno}: required field '{field}' is null"
            )


def check_non_negative(row: dict, fields: list[str], source: str, lineno: int):
    """Verify numeric fields are >= 0."""
    for field in fields:
        val = row.get(field)
        if val is not None:
            try:
                if float(val) < 0:
                    raise ValidationError(
                        f"{source}:{lineno}: field '{field}' is negative: {val}"
                    )
            except (ValueError, TypeError):
                raise ValidationError(
                    f"{source}:{lineno}: field '{field}' is not numeric: {val}"
                )


def check_integer(row: dict, fields: list[str], source: str, lineno: int):
    """Verify fields are integers (or integer-valued)."""
    for field in fields:
        val = row.get(field)
        if val is not None:
            try:
                if int(val) != float(val):
                    raise ValidationError(
                        f"{source}:{lineno}: field '{field}' is not integer: {val}"
                    )
            except (ValueError, TypeError):
                raise ValidationError(
                    f"{source}:{lineno}: field '{field}' is not numeric: {val}"
                )


def check_range(row: dict, field: str, lo: int, hi: int, source: str, lineno: int):
    """Verify an integer field is within [lo, hi]."""
    val = row.get(field)
    if val is not None:
        ival = int(val)
        if ival < lo or ival > hi:
            raise ValidationError(
                f"{source}:{lineno}: field '{field}' = {ival}, expected [{lo}..{hi}]"
            )


def parse_rfc3339(s: str) -> datetime:
    """Parse an RFC 3339 datetime string with explicit offset."""
    # Python 3.11+ fromisoformat handles offsets
    return datetime.fromisoformat(s)


# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------

def validate_version():
    """Check the VERSION file exists and matches."""
    print("  Checking VERSION file ... ", end="")
    if not VERSION_FILE.exists():
        raise ValidationError(f"VERSION file not found: {VERSION_FILE}")
    version = VERSION_FILE.read_text().strip()
    if version != FOUNDATION_VERSION:
        raise ValidationError(
            f"VERSION is '{version}', expected '{FOUNDATION_VERSION}'"
        )
    print(f"OK ({version})")


def validate_file_existence():
    """Check all fixture files exist."""
    print("  Checking fixture file existence ... ", end="")
    missing = []
    for name, path in FIXTURE_PATHS.items():
        if not path.exists():
            missing.append(f"{name}: {path}")
    if missing:
        raise ValidationError(
            "Missing fixture files:\n    " + "\n    ".join(missing)
        )
    print(f"OK ({len(FIXTURE_PATHS)} files)")


def validate_bronze_fixture() -> list[dict]:
    """Validate the Bronze source_trips.csv fixture."""
    print("  Validating bronze/source_trips.csv ... ", end="")
    path = FIXTURE_PATHS["bronze_source"]
    rows = load_csv(path)
    if len(rows) == 0:
        raise ValidationError("bronze fixture is empty")

    required_csv_cols = {
        "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "PULocationID", "DOLocationID",
        "trip_distance", "fare_amount",
    }
    for i, row in enumerate(rows, start=1):
        missing = required_csv_cols - set(row.keys())
        if missing:
            raise ValidationError(
                f"source_trips.csv:{i}: missing columns: {sorted(missing)}"
            )
    print(f"OK ({len(rows)} rows)")
    return rows


def validate_silver_fixture() -> list[dict]:
    """Validate the Silver silver_trips.jsonl fixture."""
    print("  Validating silver/silver_trips.jsonl ... ", end="")
    path = FIXTURE_PATHS["silver_trips"]
    rows = load_jsonl(path)
    if len(rows) == 0:
        raise ValidationError("silver fixture is empty")

    for i, row in enumerate(rows, start=1):
        check_required_fields(row, SILVER_REQUIRED_FIELDS, "silver_trips.jsonl", i)

        # Only recognized fields
        unknown = set(row.keys()) - SILVER_ALL_FIELDS
        if unknown:
            raise ValidationError(
                f"silver_trips.jsonl:{i}: unknown fields: {sorted(unknown)}"
            )

        # Non-negative numerics
        check_non_negative(row, [
            "trip_distance", "fare_amount", "trip_duration_minutes",
        ], "silver_trips.jsonl", i)

        if row.get("passenger_count") is not None:
            check_non_negative(row, ["passenger_count"], "silver_trips.jsonl", i)
        if row.get("total_amount") is not None:
            check_non_negative(row, ["total_amount"], "silver_trips.jsonl", i)

        # Integer fields
        check_integer(row, [
            "pickup_zone_id", "dropoff_zone_id",
            "year", "month", "day_of_week", "hour", "five_minute_slot",
        ], "silver_trips.jsonl", i)

        # Ranges
        check_range(row, "month", 1, 12, "silver_trips.jsonl", i)
        check_range(row, "day_of_week", 1, 7, "silver_trips.jsonl", i)
        check_range(row, "hour", 0, 23, "silver_trips.jsonl", i)
        check_range(row, "five_minute_slot", 0, 11, "silver_trips.jsonl", i)

        # Positive zone IDs
        if int(row["pickup_zone_id"]) < 1:
            raise ValidationError(
                f"silver_trips.jsonl:{i}: pickup_zone_id must be >= 1"
            )
        if int(row["dropoff_zone_id"]) < 1:
            raise ValidationError(
                f"silver_trips.jsonl:{i}: dropoff_zone_id must be >= 1"
            )

    print(f"OK ({len(rows)} rows)")
    return rows


def validate_kafka_fixture() -> list[dict]:
    """Validate the Kafka trip_events.jsonl fixture."""
    print("  Validating kafka/trip_events.jsonl ... ", end="")
    path = FIXTURE_PATHS["kafka_events"]
    rows = load_jsonl(path)
    if len(rows) == 0:
        raise ValidationError("kafka fixture is empty")

    event_ids = set()
    for i, row in enumerate(rows, start=1):
        check_required_fields(row, KAFKA_REQUIRED_FIELDS, "trip_events.jsonl", i)

        # No additional properties
        unknown = set(row.keys()) - KAFKA_REQUIRED_FIELDS
        if unknown:
            raise ValidationError(
                f"trip_events.jsonl:{i}: unexpected fields: {sorted(unknown)}"
            )

        # event_id non-empty
        if not row["event_id"] or not str(row["event_id"]).strip():
            raise ValidationError(
                f"trip_events.jsonl:{i}: event_id is empty"
            )

        # event_id uniqueness
        eid = row["event_id"]
        if eid in event_ids:
            raise ValidationError(
                f"trip_events.jsonl:{i}: duplicate event_id: {eid}"
            )
        event_ids.add(eid)

        # event_time RFC 3339 with offset
        try:
            dt = parse_rfc3339(row["event_time"])
            if dt.tzinfo is None:
                raise ValidationError(
                    f"trip_events.jsonl:{i}: event_time has no UTC offset"
                )
        except (ValueError, TypeError) as e:
            raise ValidationError(
                f"trip_events.jsonl:{i}: event_time is not valid RFC 3339: {e}"
            )

        # Positive integers for zone IDs
        check_integer(row, ["pickup_zone_id", "dropoff_zone_id"], "trip_events.jsonl", i)
        if int(row["pickup_zone_id"]) < 1:
            raise ValidationError(
                f"trip_events.jsonl:{i}: pickup_zone_id must be >= 1"
            )
        if int(row["dropoff_zone_id"]) < 1:
            raise ValidationError(
                f"trip_events.jsonl:{i}: dropoff_zone_id must be >= 1"
            )

        # Non-negative numerics
        check_non_negative(row, [
            "trip_distance", "trip_duration_minutes", "fare_amount",
        ], "trip_events.jsonl", i)

    print(f"OK ({len(rows)} rows, {len(event_ids)} unique event_ids)")
    return rows


def validate_baseline_fixture() -> list[dict]:
    """Validate the historical_zone_baseline.jsonl fixture."""
    print("  Validating gold/historical_zone_baseline.jsonl ... ", end="")
    path = FIXTURE_PATHS["historical_baseline"]
    rows = load_jsonl(path)
    if len(rows) == 0:
        raise ValidationError("baseline fixture is empty")

    keys_seen = set()
    for i, row in enumerate(rows, start=1):
        check_required_fields(row, BASELINE_REQUIRED_FIELDS, "historical_zone_baseline.jsonl", i)

        # Only recognized fields
        unknown = set(row.keys()) - (BASELINE_REQUIRED_FIELDS | BASELINE_OPTIONAL_FIELDS)
        if unknown:
            raise ValidationError(
                f"historical_zone_baseline.jsonl:{i}: unknown fields: {sorted(unknown)}"
            )

        # Integer fields
        check_integer(row, [
            "pickup_zone_id", "day_of_week", "hour", "five_minute_slot",
        ], "historical_zone_baseline.jsonl", i)

        # Ranges
        check_range(row, "day_of_week", 1, 7, "historical_zone_baseline.jsonl", i)
        check_range(row, "hour", 0, 23, "historical_zone_baseline.jsonl", i)
        check_range(row, "five_minute_slot", 0, 11, "historical_zone_baseline.jsonl", i)

        # Non-negative
        check_non_negative(row, [
            "historical_mean_trip_count", "historical_std_trip_count",
        ], "historical_zone_baseline.jsonl", i)

        # Grain uniqueness
        key = (
            int(row["pickup_zone_id"]),
            int(row["day_of_week"]),
            int(row["hour"]),
            int(row["five_minute_slot"]),
        )
        if key in keys_seen:
            raise ValidationError(
                f"historical_zone_baseline.jsonl:{i}: duplicate grain key: {key}"
            )
        keys_seen.add(key)

    print(f"OK ({len(rows)} rows)")
    return rows


def validate_expected_live_fixture() -> list[dict]:
    """Validate the expected_live_zone_metrics.jsonl fixture."""
    print("  Validating gold/expected_live_zone_metrics.jsonl ... ", end="")
    path = FIXTURE_PATHS["expected_live"]
    rows = load_jsonl(path)
    if len(rows) == 0:
        raise ValidationError("expected live fixture is empty")

    for i, row in enumerate(rows, start=1):
        check_required_fields(row, LIVE_REQUIRED_FIELDS, "expected_live_zone_metrics.jsonl", i)

        # Check all expected fields present
        all_live_fields = LIVE_REQUIRED_FIELDS | LIVE_NULLABLE_FIELDS
        unknown = set(row.keys()) - all_live_fields
        if unknown:
            raise ValidationError(
                f"expected_live_zone_metrics.jsonl:{i}: unknown fields: {sorted(unknown)}"
            )

        # Integer fields
        check_integer(row, [
            "pickup_zone_id", "current_trip_count",
        ], "expected_live_zone_metrics.jsonl", i)

        # current_trip_count >= 0
        if int(row["current_trip_count"]) < 0:
            raise ValidationError(
                f"expected_live_zone_metrics.jsonl:{i}: current_trip_count is negative"
            )

    print(f"OK ({len(rows)} rows)")
    return rows


# ---------------------------------------------------------------------------
# Cross-contract invariants
# ---------------------------------------------------------------------------

def validate_cross_contract_invariants(
    kafka_events: list[dict],
    baseline_rows: list[dict],
    expected_live: list[dict],
):
    """
    Verify that the expected live metrics are the correct computation
    from the Kafka events and historical baseline.
    """
    print("  Validating cross-contract invariants ... ", end="")

    # --- Count Kafka events per zone in each 5-minute window ---
    # Parse event times, derive New York day_of_week / hour / slot
    # (all fixture events use -05:00 offset = EST)
    zone_counts: dict[int, int] = {}
    for event in kafka_events:
        zone = int(event["pickup_zone_id"])
        zone_counts[zone] = zone_counts.get(zone, 0) + 1

    # --- Build baseline lookup ---
    baseline_lookup: dict[tuple, dict] = {}
    for row in baseline_rows:
        key = (
            int(row["pickup_zone_id"]),
            int(row["day_of_week"]),
            int(row["hour"]),
            int(row["five_minute_slot"]),
        )
        baseline_lookup[key] = row

    # --- Parse the first Kafka event to determine the window key ---
    first_event = kafka_events[0]
    dt = parse_rfc3339(first_event["event_time"])
    # Derive day_of_week (Monday=1, Sunday=7 in ISO)
    dow = dt.isoweekday()
    hour = dt.hour
    slot = dt.minute // 5

    # --- Verify each expected live metric row ---
    for i, expected in enumerate(expected_live, start=1):
        zone = int(expected["pickup_zone_id"])
        expected_count = int(expected["current_trip_count"])

        # Check event count matches
        actual_count = zone_counts.get(zone, 0)
        if actual_count != expected_count:
            raise ValidationError(
                f"Cross-contract: zone {zone}: expected current_trip_count="
                f"{expected_count}, but Kafka events give {actual_count}"
            )

        # Check baseline lookup
        baseline_key = (zone, dow, hour, slot)
        baseline = baseline_lookup.get(baseline_key)

        if baseline is None:
            # historical_mean should be null in expected
            if expected.get("historical_mean_trip_count") is not None:
                raise ValidationError(
                    f"Cross-contract: zone {zone}: no baseline row, but "
                    f"expected historical_mean_trip_count is not null"
                )
            if expected.get("demand_ratio") is not None:
                raise ValidationError(
                    f"Cross-contract: zone {zone}: no baseline row, but "
                    f"expected demand_ratio is not null"
                )
            if expected.get("z_score") is not None:
                raise ValidationError(
                    f"Cross-contract: zone {zone}: no baseline row, but "
                    f"expected z_score is not null"
                )
            continue

        hist_mean = float(baseline["historical_mean_trip_count"])
        hist_std = float(baseline["historical_std_trip_count"])

        # Check historical_mean_trip_count matches
        expected_hist_mean = expected.get("historical_mean_trip_count")
        if expected_hist_mean is not None:
            if abs(float(expected_hist_mean) - hist_mean) > 1e-6:
                raise ValidationError(
                    f"Cross-contract: zone {zone}: expected historical_mean="
                    f"{expected_hist_mean}, baseline has {hist_mean}"
                )

        # Check demand_ratio
        expected_ratio = expected.get("demand_ratio")
        if hist_mean > 0:
            computed_ratio = actual_count / hist_mean
            if expected_ratio is None:
                raise ValidationError(
                    f"Cross-contract: zone {zone}: demand_ratio should be "
                    f"{computed_ratio:.6f}, but is null"
                )
            if abs(float(expected_ratio) - computed_ratio) > 1e-6:
                raise ValidationError(
                    f"Cross-contract: zone {zone}: expected demand_ratio="
                    f"{expected_ratio}, computed={computed_ratio:.6f}"
                )
        else:
            if expected_ratio is not None:
                raise ValidationError(
                    f"Cross-contract: zone {zone}: historical_mean <= 0, "
                    f"demand_ratio should be null but is {expected_ratio}"
                )

        # Check z_score
        expected_z = expected.get("z_score")
        if hist_std > 0:
            computed_z = (actual_count - hist_mean) / hist_std
            if expected_z is None:
                raise ValidationError(
                    f"Cross-contract: zone {zone}: z_score should be "
                    f"{computed_z:.6f}, but is null"
                )
            if abs(float(expected_z) - computed_z) > 1e-6:
                raise ValidationError(
                    f"Cross-contract: zone {zone}: expected z_score="
                    f"{expected_z}, computed={computed_z:.6f}"
                )
        else:
            if expected_z is not None:
                raise ValidationError(
                    f"Cross-contract: zone {zone}: historical_std <= 0, "
                    f"z_score should be null but is {expected_z}"
                )

    print("OK")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"UrbanFlow Foundation Fixture Validator — v{FOUNDATION_VERSION}")
    print(f"Base directory: {BASE_DIR}")
    print()

    errors: list[str] = []

    # 1. Version check
    try:
        validate_version()
    except ValidationError as e:
        errors.append(str(e))

    # 2. File existence
    try:
        validate_file_existence()
    except ValidationError as e:
        errors.append(str(e))
        # Can't continue if files are missing
        print()
        print(f"FOUNDATION FIXTURES = FAIL ({len(errors)} error(s))")
        for err in errors:
            print(f"  ERROR: {err}")
        sys.exit(1)

    # 3. Individual fixture validation
    bronze_rows = None
    silver_rows = None
    kafka_events = None
    baseline_rows = None
    expected_live = None

    try:
        bronze_rows = validate_bronze_fixture()
    except ValidationError as e:
        errors.append(str(e))

    try:
        silver_rows = validate_silver_fixture()
    except ValidationError as e:
        errors.append(str(e))

    try:
        kafka_events = validate_kafka_fixture()
    except ValidationError as e:
        errors.append(str(e))

    try:
        baseline_rows = validate_baseline_fixture()
    except ValidationError as e:
        errors.append(str(e))

    try:
        expected_live = validate_expected_live_fixture()
    except ValidationError as e:
        errors.append(str(e))

    # 4. Cross-contract invariants
    if kafka_events and baseline_rows and expected_live:
        try:
            validate_cross_contract_invariants(
                kafka_events, baseline_rows, expected_live
            )
        except ValidationError as e:
            errors.append(str(e))
    elif not errors:
        errors.append("Cannot validate cross-contract invariants: fixture loading failed")

    # 5. Result
    print()
    if errors:
        print(f"FOUNDATION FIXTURES = FAIL ({len(errors)} error(s))")
        for err in errors:
            print(f"  ERROR: {err}")
        sys.exit(1)
    else:
        print("FOUNDATION FIXTURES = PASS")
        sys.exit(0)


if __name__ == "__main__":
    main()
