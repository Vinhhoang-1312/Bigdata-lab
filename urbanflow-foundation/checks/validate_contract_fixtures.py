#!/usr/bin/env python3
"""
UrbanFlow Foundation — Contract Fixture Validator
Foundation version: 1.0.0

Validates the supplied JSONL/CSV fixtures and core cross-contract
invariants using only the Python standard library.

Deliberately no third-party dependency: America/New_York offsets are
computed from the US DST rule in `nyc_offset_hours` rather than from
`zoneinfo`, which needs the `tzdata` package on Windows.

Usage:
    python checks/validate_contract_fixtures.py

Expected result:
    FOUNDATION FIXTURES = PASS

Run from the urbanflow-foundation root directory.
"""

import csv
import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FOUNDATION_VERSION = "1.0.0"

# Resolve paths relative to the script's parent directory (urbanflow-foundation/)
BASE_DIR = Path(__file__).resolve().parent.parent

FIXTURE_PATHS = {
    "bronze_source":       BASE_DIR / "fixtures" / "bronze" / "source_trips.csv",
    "zone_lookup":         BASE_DIR / "fixtures" / "bronze" / "zone_lookup.csv",
    "silver_trips":        BASE_DIR / "fixtures" / "silver" / "silver_trips.jsonl",
    "expected_silver":     BASE_DIR / "fixtures" / "silver" / "expected_silver_from_bronze.jsonl",
    "kafka_events":        BASE_DIR / "fixtures" / "kafka"  / "trip_events.jsonl",
    "historical_baseline": BASE_DIR / "fixtures" / "gold"   / "historical_zone_baseline.jsonl",
    "expected_live":       BASE_DIR / "fixtures" / "gold"   / "expected_live_zone_metrics.jsonl",
}

VERSION_FILE = BASE_DIR / "VERSION"

WINDOW_MINUTES = 5

# trip_duration_minutes must match the timestamps; fixtures carry exact values.
DURATION_TOLERANCE_MINUTES = 1e-4

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
# Bronze source columns
# ---------------------------------------------------------------------------

BRONZE_REQUIRED_COLUMNS = {
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "PULocationID", "DOLocationID",
    "trip_distance", "fare_amount",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class ValidationError(Exception):
    """Raised when a fixture fails a contract check."""


RFC3339_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$"
)


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
        if val is None:
            continue
        try:
            numeric = float(val)
        except (ValueError, TypeError):
            raise ValidationError(
                f"{source}:{lineno}: field '{field}' is not numeric: {val}"
            )
        if numeric < 0:
            raise ValidationError(
                f"{source}:{lineno}: field '{field}' is negative: {val}"
            )


def check_integer(row: dict, fields: list[str], source: str, lineno: int):
    """Verify fields are integers (or integer-valued)."""
    for field in fields:
        val = row.get(field)
        if val is None:
            continue
        try:
            numeric = float(val)
        except (ValueError, TypeError):
            raise ValidationError(
                f"{source}:{lineno}: field '{field}' is not numeric: {val}"
            )
        if int(numeric) != numeric:
            raise ValidationError(
                f"{source}:{lineno}: field '{field}' is not integer: {val}"
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


def parse_rfc3339(s, source: str, lineno: int, field: str) -> datetime:
    """
    Parse an RFC 3339 datetime string with an explicit UTC offset.

    `datetime.fromisoformat` alone is too permissive for the contract: it
    accepts a space separator and an omitted offset. The regex enforces the
    contract wording before parsing.
    """
    if not isinstance(s, str) or not RFC3339_RE.match(s):
        raise ValidationError(
            f"{source}:{lineno}: field '{field}' is not RFC 3339 with an "
            f"explicit UTC offset: {s!r}"
        )
    dt = datetime.fromisoformat(s.replace("Z", "+00:00").replace("z", "+00:00"))
    if dt.tzinfo is None:
        raise ValidationError(
            f"{source}:{lineno}: field '{field}' has no UTC offset: {s!r}"
        )
    return dt


# --- America/New_York offsets without a third-party tz database -------------

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> datetime:
    """Nth occurrence of `weekday` (Monday=0) in the given month, at 00:00."""
    first = datetime(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def _dst_bounds(year: int) -> tuple[datetime, datetime]:
    """US DST wall-clock bounds: 2nd Sunday March 02:00 .. 1st Sunday Nov 02:00."""
    sunday = 6
    start = _nth_weekday(year, 3, sunday, 2).replace(hour=2)
    end = _nth_weekday(year, 11, sunday, 1).replace(hour=2)
    return start, end


def nyc_offset_hours(wall: datetime) -> int:
    """UTC offset in hours for an America/New_York wall-clock time (-5 or -4)."""
    naive = wall.replace(tzinfo=None)
    start, end = _dst_bounds(naive.year)
    return -4 if start <= naive < end else -5


def nyc_offset_text(wall: datetime) -> str:
    """The offset of `wall` formatted for an RFC 3339 string, e.g. '-05:00'."""
    offset = nyc_offset_hours(wall)
    return f"{'-' if offset < 0 else '+'}{abs(offset):02d}:00"


def nyc_wall_status(wall: datetime) -> str:
    """'ok', 'nonexistent' (spring-forward gap) or 'ambiguous' (fall-back hour)."""
    naive = wall.replace(tzinfo=None)
    start, end = _dst_bounds(naive.year)
    if start <= naive < start + timedelta(hours=1):
        return "nonexistent"
    if end - timedelta(hours=1) <= naive < end:
        return "ambiguous"
    return "ok"


def check_nyc_offset(dt: datetime, source: str, lineno: int, field: str):
    """Verify the declared offset is the correct America/New_York offset."""
    wall = dt.replace(tzinfo=None)
    if nyc_wall_status(wall) == "nonexistent":
        raise ValidationError(
            f"{source}:{lineno}: field '{field}' is a non-existent "
            f"America/New_York wall-clock time (spring-forward gap): {dt.isoformat()}"
        )
    expected = nyc_offset_hours(wall)
    actual = dt.utcoffset().total_seconds() / 3600
    if actual != expected:
        raise ValidationError(
            f"{source}:{lineno}: field '{field}' declares offset "
            f"{actual:+.0f}:00 but America/New_York is {expected:+.0f}:00 "
            f"at {wall.isoformat()}"
        )


def derived_time_fields(dt: datetime) -> dict:
    """Contract-defined derived fields (contracts/time-semantics.md)."""
    return {
        "year": dt.year,
        "month": dt.month,
        "day_of_week": dt.isoweekday(),
        "hour": dt.hour,
        "five_minute_slot": dt.minute // WINDOW_MINUTES,
    }


def window_start_of(dt: datetime) -> datetime:
    """Floor an event time to its five-minute tumbling window start."""
    return dt.replace(
        minute=(dt.minute // WINDOW_MINUTES) * WINDOW_MINUTES,
        second=0,
        microsecond=0,
    )


def trip_tuple(pickup: datetime, zone_in, zone_out, distance, duration, fare):
    """Comparable identity of a trip, used for Silver <-> Kafka consistency."""
    return (
        pickup.timestamp(),
        int(zone_in), int(zone_out),
        round(float(distance), 6),
        round(float(duration), 6),
        round(float(fare), 6),
    )


# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------

def validate_version():
    """Check the VERSION file exists and matches."""
    if not VERSION_FILE.exists():
        raise ValidationError(f"VERSION file not found: {VERSION_FILE}")
    version = VERSION_FILE.read_text().strip()
    if version != FOUNDATION_VERSION:
        raise ValidationError(
            f"VERSION is '{version}', expected '{FOUNDATION_VERSION}'"
        )
    return f"OK ({version})"


def validate_file_existence():
    """Check all fixture files exist."""
    missing = []
    for name, path in FIXTURE_PATHS.items():
        if not path.exists():
            missing.append(f"{name}: {path}")
    if missing:
        raise ValidationError(
            "Missing fixture files:\n    " + "\n    ".join(missing)
        )
    return f"OK ({len(FIXTURE_PATHS)} files)"


def validate_zone_lookup():
    """Validate the taxi-zone lookup fixture and return the eligible zone IDs."""
    rows = load_csv(FIXTURE_PATHS["zone_lookup"])
    if not rows:
        raise ValidationError("zone lookup fixture is empty")

    zone_ids = set()
    for i, row in enumerate(rows, start=1):
        if "LocationID" not in row:
            raise ValidationError(f"zone_lookup.csv:{i}: missing LocationID column")
        try:
            zone_id = int(str(row["LocationID"]).strip())
        except (ValueError, TypeError):
            raise ValidationError(
                f"zone_lookup.csv:{i}: LocationID is not an integer: "
                f"{row['LocationID']!r}"
            )
        if zone_id < 1:
            raise ValidationError(f"zone_lookup.csv:{i}: LocationID must be >= 1")
        if zone_id in zone_ids:
            raise ValidationError(
                f"zone_lookup.csv:{i}: duplicate LocationID: {zone_id}"
            )
        zone_ids.add(zone_id)

    return f"OK ({len(zone_ids)} zones)", zone_ids


def classify_bronze_row(row: dict, zone_ids: set):
    """
    Apply the Silver validity rules to one raw Bronze row.

    Returns (None, silver_row) when the row is valid, or
    (rejection_reason, None) when the row must be rejected.
    """
    def parse_naive(value):
        text = str(value).strip() if value is not None else ""
        if not text:
            return None
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def parse_int(value):
        text = str(value).strip() if value is not None else ""
        if not text:
            return None
        try:
            return int(text)
        except ValueError:
            return None

    def parse_float(value):
        text = str(value).strip() if value is not None else ""
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    pickup = parse_naive(row.get("tpep_pickup_datetime"))
    if pickup is None:
        return "pickup_datetime is null or unparseable", None

    dropoff = parse_naive(row.get("tpep_dropoff_datetime"))
    if dropoff is None:
        return "dropoff_datetime is null or unparseable", None

    if dropoff < pickup:
        return "dropoff_datetime < pickup_datetime", None

    if nyc_wall_status(pickup) == "nonexistent":
        return "pickup_datetime is a non-existent America/New_York wall-clock time", None

    pickup_zone = parse_int(row.get("PULocationID"))
    dropoff_zone = parse_int(row.get("DOLocationID"))
    if pickup_zone is None or pickup_zone not in zone_ids:
        return "pickup_zone_id not in taxi-zone lookup", None
    if dropoff_zone is None or dropoff_zone not in zone_ids:
        return "dropoff_zone_id not in taxi-zone lookup", None

    distance = parse_float(row.get("trip_distance"))
    if distance is None or distance < 0:
        return "trip_distance is null or negative", None

    fare = parse_float(row.get("fare_amount"))
    if fare is None or fare < 0:
        return "fare_amount is null or negative", None

    passengers = parse_int(row.get("passenger_count"))
    if passengers is not None and passengers < 0:
        return "passenger_count is negative", None

    total = parse_float(row.get("total_amount"))
    if total is not None and total < 0:
        return "total_amount is negative", None

    silver = {
        "pickup_datetime": pickup.isoformat() + nyc_offset_text(pickup),
        "dropoff_datetime": dropoff.isoformat() + nyc_offset_text(dropoff),
        "pickup_zone_id": pickup_zone,
        "dropoff_zone_id": dropoff_zone,
        "trip_distance": distance,
        "passenger_count": passengers,
        "fare_amount": fare,
        "total_amount": total,
        "trip_duration_minutes": round((dropoff - pickup).total_seconds() / 60.0, 4),
    }
    silver.update(derived_time_fields(pickup))
    return None, silver


def validate_bronze_fixture(zone_ids: set):
    """
    Validate the Bronze source_trips.csv fixture, classify every row against
    the Silver validity rules, and compare the accepted rows against the
    expected Silver reference output.
    """
    rows = load_csv(FIXTURE_PATHS["bronze_source"])
    if len(rows) == 0:
        raise ValidationError("bronze fixture is empty")

    for i, row in enumerate(rows, start=1):
        missing = BRONZE_REQUIRED_COLUMNS - set(row.keys())
        if missing:
            raise ValidationError(
                f"source_trips.csv:{i}: missing columns: {sorted(missing)}"
            )

    accepted, rejected = [], []
    for i, row in enumerate(rows, start=1):
        reason, silver = classify_bronze_row(row, zone_ids)
        if reason is None:
            accepted.append(silver)
        else:
            rejected.append((i, reason))

    # The fixture must exercise the rejection path, otherwise urbanflow-batch
    # can report "rejected = 0" while implementing no cleaning rules at all.
    if not rejected:
        raise ValidationError(
            "bronze fixture contains no rejectable rows: the Silver cleaning "
            "path would be untested (see docs/acceptance-tests.md, Test H)"
        )
    if not accepted:
        raise ValidationError("bronze fixture contains no valid rows")

    expected = load_jsonl(FIXTURE_PATHS["expected_silver"])
    if len(expected) != len(accepted):
        raise ValidationError(
            f"expected_silver_from_bronze.jsonl has {len(expected)} rows but "
            f"{len(accepted)} of {len(rows)} bronze rows are valid"
        )

    for i, (want, got) in enumerate(zip(expected, accepted), start=1):
        if set(want.keys()) != set(got.keys()):
            raise ValidationError(
                f"expected_silver_from_bronze.jsonl:{i}: field set differs from "
                f"the derived Silver row: {sorted(set(want) ^ set(got))}"
            )
        for field, got_value in got.items():
            want_value = want[field]
            if want_value is None or got_value is None:
                if want_value is not None or got_value is not None:
                    raise ValidationError(
                        f"expected_silver_from_bronze.jsonl:{i}: field '{field}' "
                        f"is {want_value!r}, derived {got_value!r}"
                    )
            elif isinstance(got_value, float) or isinstance(want_value, float):
                if abs(float(want_value) - float(got_value)) > 1e-4:
                    raise ValidationError(
                        f"expected_silver_from_bronze.jsonl:{i}: field '{field}' "
                        f"is {want_value}, derived {got_value}"
                    )
            elif want_value != got_value:
                raise ValidationError(
                    f"expected_silver_from_bronze.jsonl:{i}: field '{field}' "
                    f"is {want_value!r}, derived {got_value!r}"
                )

    reasons = sorted({reason for _, reason in rejected})
    return (
        f"OK ({len(rows)} source rows, {len(accepted)} valid, "
        f"{len(rejected)} rejected, {len(reasons)} distinct reasons)"
    )


def validate_silver_rows(rows: list[dict], source: str, zone_ids: set):
    """Validate a list of Silver rows against contracts/silver-trips.yaml."""
    for i, row in enumerate(rows, start=1):
        check_required_fields(row, SILVER_REQUIRED_FIELDS, source, i)

        unknown = set(row.keys()) - SILVER_ALL_FIELDS
        if unknown:
            raise ValidationError(f"{source}:{i}: unknown fields: {sorted(unknown)}")

        check_non_negative(row, [
            "trip_distance", "fare_amount", "trip_duration_minutes",
        ], source, i)
        if row.get("passenger_count") is not None:
            check_non_negative(row, ["passenger_count"], source, i)
        if row.get("total_amount") is not None:
            check_non_negative(row, ["total_amount"], source, i)

        check_integer(row, [
            "pickup_zone_id", "dropoff_zone_id",
            "year", "month", "day_of_week", "hour", "five_minute_slot",
        ], source, i)

        check_range(row, "month", 1, 12, source, i)
        check_range(row, "day_of_week", 1, 7, source, i)
        check_range(row, "hour", 0, 23, source, i)
        check_range(row, "five_minute_slot", 0, 11, source, i)

        # Zone IDs must exist in the taxi-zone lookup.
        for field in ("pickup_zone_id", "dropoff_zone_id"):
            zone = int(row[field])
            if zone < 1:
                raise ValidationError(f"{source}:{i}: {field} must be >= 1")
            if zone not in zone_ids:
                raise ValidationError(
                    f"{source}:{i}: {field} = {zone} is not in the taxi-zone lookup"
                )

        # Timestamps: RFC 3339, correct America/New_York offset, ordered.
        pickup = parse_rfc3339(row["pickup_datetime"], source, i, "pickup_datetime")
        dropoff = parse_rfc3339(row["dropoff_datetime"], source, i, "dropoff_datetime")
        check_nyc_offset(pickup, source, i, "pickup_datetime")
        check_nyc_offset(dropoff, source, i, "dropoff_datetime")
        if dropoff < pickup:
            raise ValidationError(
                f"{source}:{i}: dropoff_datetime < pickup_datetime "
                f"({row['dropoff_datetime']} < {row['pickup_datetime']})"
            )

        # trip_duration_minutes must match the two timestamps.
        computed = (dropoff - pickup).total_seconds() / 60.0
        declared = float(row["trip_duration_minutes"])
        if abs(computed - declared) > DURATION_TOLERANCE_MINUTES:
            raise ValidationError(
                f"{source}:{i}: trip_duration_minutes = {declared}, but the "
                f"timestamps give {computed:.4f}"
            )

        # Derived fields must come from pickup_datetime in America/New_York.
        for field, expected_value in derived_time_fields(pickup).items():
            if int(row[field]) != expected_value:
                raise ValidationError(
                    f"{source}:{i}: {field} = {row[field]}, but pickup_datetime "
                    f"{row['pickup_datetime']} gives {expected_value}"
                )


def validate_silver_fixture(zone_ids: set):
    """Validate the Silver silver_trips.jsonl fixture."""
    rows = load_jsonl(FIXTURE_PATHS["silver_trips"])
    if len(rows) == 0:
        raise ValidationError("silver fixture is empty")
    validate_silver_rows(rows, "silver_trips.jsonl", zone_ids)
    return f"OK ({len(rows)} rows)", rows


def validate_expected_silver_fixture(zone_ids: set):
    """Validate the Bronze-derived Silver reference output."""
    rows = load_jsonl(FIXTURE_PATHS["expected_silver"])
    if len(rows) == 0:
        raise ValidationError("expected Silver fixture is empty")
    validate_silver_rows(rows, "expected_silver_from_bronze.jsonl", zone_ids)
    return f"OK ({len(rows)} rows)"


def validate_kafka_fixture(zone_ids: set):
    """Validate the Kafka trip_events.jsonl fixture."""
    rows = load_jsonl(FIXTURE_PATHS["kafka_events"])
    if len(rows) == 0:
        raise ValidationError("kafka fixture is empty")

    event_ids = set()
    for i, row in enumerate(rows, start=1):
        check_required_fields(row, KAFKA_REQUIRED_FIELDS, "trip_events.jsonl", i)

        unknown = set(row.keys()) - KAFKA_REQUIRED_FIELDS
        if unknown:
            raise ValidationError(
                f"trip_events.jsonl:{i}: unexpected fields: {sorted(unknown)}"
            )

        if not row["event_id"] or not str(row["event_id"]).strip():
            raise ValidationError(f"trip_events.jsonl:{i}: event_id is empty")

        eid = row["event_id"]
        if eid in event_ids:
            raise ValidationError(
                f"trip_events.jsonl:{i}: duplicate event_id: {eid}"
            )
        event_ids.add(eid)

        dt = parse_rfc3339(row["event_time"], "trip_events.jsonl", i, "event_time")
        check_nyc_offset(dt, "trip_events.jsonl", i, "event_time")

        check_integer(
            row, ["pickup_zone_id", "dropoff_zone_id"], "trip_events.jsonl", i
        )
        for field in ("pickup_zone_id", "dropoff_zone_id"):
            zone = int(row[field])
            if zone < 1:
                raise ValidationError(
                    f"trip_events.jsonl:{i}: {field} must be >= 1"
                )
            if zone not in zone_ids:
                raise ValidationError(
                    f"trip_events.jsonl:{i}: {field} = {zone} is not in the "
                    f"taxi-zone lookup"
                )

        check_non_negative(row, [
            "trip_distance", "trip_duration_minutes", "fare_amount",
        ], "trip_events.jsonl", i)

    return f"OK ({len(rows)} rows, {len(event_ids)} unique event_ids)", rows


def validate_baseline_fixture():
    """Validate the historical_zone_baseline.jsonl fixture."""
    source = "historical_zone_baseline.jsonl"
    rows = load_jsonl(FIXTURE_PATHS["historical_baseline"])
    if len(rows) == 0:
        raise ValidationError("baseline fixture is empty")

    keys_seen = set()
    for i, row in enumerate(rows, start=1):
        check_required_fields(row, BASELINE_REQUIRED_FIELDS, source, i)

        unknown = set(row.keys()) - (
            BASELINE_REQUIRED_FIELDS | BASELINE_OPTIONAL_FIELDS
        )
        if unknown:
            raise ValidationError(f"{source}:{i}: unknown fields: {sorted(unknown)}")

        check_integer(row, [
            "pickup_zone_id", "day_of_week", "hour", "five_minute_slot",
        ], source, i)

        check_range(row, "day_of_week", 1, 7, source, i)
        check_range(row, "hour", 0, 23, source, i)
        check_range(row, "five_minute_slot", 0, 11, source, i)

        check_non_negative(row, [
            "historical_mean_trip_count", "historical_std_trip_count",
        ], source, i)

        key = (
            int(row["pickup_zone_id"]),
            int(row["day_of_week"]),
            int(row["hour"]),
            int(row["five_minute_slot"]),
        )
        if key in keys_seen:
            raise ValidationError(f"{source}:{i}: duplicate grain key: {key}")
        keys_seen.add(key)

    return f"OK ({len(rows)} rows)", rows


def validate_expected_live_fixture():
    """Validate the expected_live_zone_metrics.jsonl fixture."""
    source = "expected_live_zone_metrics.jsonl"
    rows = load_jsonl(FIXTURE_PATHS["expected_live"])
    if len(rows) == 0:
        raise ValidationError("expected live fixture is empty")

    keys_seen = set()
    for i, row in enumerate(rows, start=1):
        check_required_fields(row, LIVE_REQUIRED_FIELDS, source, i)

        unknown = set(row.keys()) - (LIVE_REQUIRED_FIELDS | LIVE_NULLABLE_FIELDS)
        if unknown:
            raise ValidationError(f"{source}:{i}: unknown fields: {sorted(unknown)}")

        check_integer(row, ["pickup_zone_id", "current_trip_count"], source, i)
        if int(row["current_trip_count"]) < 0:
            raise ValidationError(f"{source}:{i}: current_trip_count is negative")

        # Window boundaries: RFC 3339, NY offset, aligned, exactly five minutes.
        start = parse_rfc3339(row["window_start"], source, i, "window_start")
        end = parse_rfc3339(row["window_end"], source, i, "window_end")
        check_nyc_offset(start, source, i, "window_start")
        check_nyc_offset(end, source, i, "window_end")

        if (end - start) != timedelta(minutes=WINDOW_MINUTES):
            raise ValidationError(
                f"{source}:{i}: window spans {end - start}, expected "
                f"{WINDOW_MINUTES} minutes"
            )
        if start.second or start.microsecond or start.minute % WINDOW_MINUTES:
            raise ValidationError(
                f"{source}:{i}: window_start {row['window_start']} is not "
                f"aligned to a {WINDOW_MINUTES}-minute tumbling boundary"
            )

        key = (int(row["pickup_zone_id"]), start)
        if key in keys_seen:
            raise ValidationError(
                f"{source}:{i}: duplicate (pickup_zone_id, window_start): "
                f"{int(row['pickup_zone_id'])} @ {row['window_start']}"
            )
        keys_seen.add(key)

    return f"OK ({len(rows)} rows)", rows


# ---------------------------------------------------------------------------
# Cross-contract invariants
# ---------------------------------------------------------------------------

def validate_silver_kafka_consistency(silver_rows: list[dict], kafka_events: list[dict]):
    """
    The Kafka fixture must describe exactly the same trips as the Silver
    fixture, so urbanflow-replay's input and urbanflow-streaming's input
    cannot drift apart.
    """
    silver_trips = sorted(
        trip_tuple(
            parse_rfc3339(
                r["pickup_datetime"], "silver_trips.jsonl", i, "pickup_datetime"
            ),
            r["pickup_zone_id"], r["dropoff_zone_id"],
            r["trip_distance"], r["trip_duration_minutes"], r["fare_amount"],
        )
        for i, r in enumerate(silver_rows, start=1)
    )
    kafka_trips = sorted(
        trip_tuple(
            parse_rfc3339(r["event_time"], "trip_events.jsonl", i, "event_time"),
            r["pickup_zone_id"], r["dropoff_zone_id"],
            r["trip_distance"], r["trip_duration_minutes"], r["fare_amount"],
        )
        for i, r in enumerate(kafka_events, start=1)
    )
    if silver_trips != kafka_trips:
        only_silver = [t for t in silver_trips if t not in kafka_trips]
        only_kafka = [t for t in kafka_trips if t not in silver_trips]
        raise ValidationError(
            "Silver and Kafka fixtures describe different trips: "
            f"{len(only_silver)} only in silver_trips.jsonl, "
            f"{len(only_kafka)} only in trip_events.jsonl"
        )
    return f"OK ({len(kafka_trips)} matching trips)"


def validate_cross_contract_invariants(
    kafka_events: list[dict],
    baseline_rows: list[dict],
    expected_live: list[dict],
):
    """
    Verify that the expected live metrics are the correct computation from
    the Kafka events and the historical baseline, per five-minute window.
    """
    # --- Count Kafka events per (zone, five-minute window) ---
    window_counts: dict[tuple, int] = {}
    for i, event in enumerate(kafka_events, start=1):
        dt = parse_rfc3339(event["event_time"], "trip_events.jsonl", i, "event_time")
        key = (int(event["pickup_zone_id"]), window_start_of(dt))
        window_counts[key] = window_counts.get(key, 0) + 1

    # --- Build baseline lookup ---
    baseline_lookup: dict[tuple, dict] = {}
    for row in baseline_rows:
        baseline_lookup[(
            int(row["pickup_zone_id"]),
            int(row["day_of_week"]),
            int(row["hour"]),
            int(row["five_minute_slot"]),
        )] = row

    covered = set()

    for i, expected in enumerate(expected_live, start=1):
        zone = int(expected["pickup_zone_id"])
        expected_count = int(expected["current_trip_count"])
        start = parse_rfc3339(
            expected["window_start"],
            "expected_live_zone_metrics.jsonl", i, "window_start",
        )
        where = f"zone {zone} @ {expected['window_start']}"
        key = (zone, start)
        covered.add(key)

        actual_count = window_counts.get(key, 0)
        if actual_count != expected_count:
            raise ValidationError(
                f"Cross-contract: {where}: expected current_trip_count="
                f"{expected_count}, but the Kafka events give {actual_count}"
            )

        # The baseline join key is derived from window_start in America/New_York.
        derived = derived_time_fields(start)
        baseline_key = (
            zone, derived["day_of_week"], derived["hour"],
            derived["five_minute_slot"],
        )
        baseline = baseline_lookup.get(baseline_key)

        if baseline is None:
            # Left join: the window survives, but every baseline-derived
            # metric must be null.
            for field in ("historical_mean_trip_count", "demand_ratio", "z_score"):
                if expected.get(field) is not None:
                    raise ValidationError(
                        f"Cross-contract: {where}: no baseline row for "
                        f"{baseline_key}, but expected {field} is not null"
                    )
            continue

        hist_mean = float(baseline["historical_mean_trip_count"])
        hist_std = float(baseline["historical_std_trip_count"])

        expected_hist_mean = expected.get("historical_mean_trip_count")
        if expected_hist_mean is None:
            raise ValidationError(
                f"Cross-contract: {where}: a baseline row exists "
                f"(mean={hist_mean}) but expected historical_mean_trip_count is null"
            )
        if abs(float(expected_hist_mean) - hist_mean) > 1e-6:
            raise ValidationError(
                f"Cross-contract: {where}: expected historical_mean="
                f"{expected_hist_mean}, baseline has {hist_mean}"
            )

        # demand_ratio
        expected_ratio = expected.get("demand_ratio")
        if hist_mean > 0:
            computed_ratio = actual_count / hist_mean
            if expected_ratio is None:
                raise ValidationError(
                    f"Cross-contract: {where}: demand_ratio should be "
                    f"{computed_ratio:.6f}, but is null"
                )
            if abs(float(expected_ratio) - computed_ratio) > 1e-6:
                raise ValidationError(
                    f"Cross-contract: {where}: expected demand_ratio="
                    f"{expected_ratio}, computed={computed_ratio:.6f}"
                )
        elif expected_ratio is not None:
            raise ValidationError(
                f"Cross-contract: {where}: historical_mean <= 0, demand_ratio "
                f"should be null but is {expected_ratio}"
            )

        # z_score
        expected_z = expected.get("z_score")
        if hist_std > 0:
            computed_z = (actual_count - hist_mean) / hist_std
            if expected_z is None:
                raise ValidationError(
                    f"Cross-contract: {where}: z_score should be "
                    f"{computed_z:.6f}, but is null"
                )
            if abs(float(expected_z) - computed_z) > 1e-6:
                raise ValidationError(
                    f"Cross-contract: {where}: expected z_score={expected_z}, "
                    f"computed={computed_z:.6f}"
                )
        elif expected_z is not None:
            raise ValidationError(
                f"Cross-contract: {where}: historical_std <= 0, z_score should "
                f"be null but is {expected_z}"
            )

    # Every (zone, window) present in the events must appear in the expected
    # output, so a five-minute window is never silently dropped.
    missing = sorted(
        (zone, start.isoformat())
        for (zone, start) in window_counts
        if (zone, start) not in covered
    )
    if missing:
        raise ValidationError(
            "Cross-contract: the Kafka events produce (zone, window) "
            f"combinations absent from expected_live_zone_metrics.jsonl: {missing}"
        )

    return f"OK ({len(window_counts)} (zone, window) combinations)"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_check(label: str, fn, errors: list):
    """Run one check and print its label and result on a single line."""
    try:
        result = fn()
    except ValidationError as e:
        print(f"  {label} ... FAIL")
        errors.append(str(e))
        return None
    if isinstance(result, tuple):
        message, payload = result[0], result[1]
    else:
        message, payload = result, True
    print(f"  {label} ... {message}")
    return payload


def main():
    print(f"UrbanFlow Foundation Fixture Validator - v{FOUNDATION_VERSION}")
    print(f"Base directory: {BASE_DIR}")
    print()

    errors: list[str] = []

    run_check("Checking VERSION file", validate_version, errors)

    if run_check("Checking fixture file existence", validate_file_existence, errors) is None:
        print()
        print(f"FOUNDATION FIXTURES = FAIL ({len(errors)} error(s))")
        for err in errors:
            print(f"  ERROR: {err}")
        sys.exit(1)

    zone_ids = run_check(
        "Validating bronze/zone_lookup.csv", validate_zone_lookup, errors
    )

    silver_rows = kafka_events = None
    if zone_ids:
        run_check(
            "Validating bronze/source_trips.csv",
            lambda: validate_bronze_fixture(zone_ids), errors,
        )
        run_check(
            "Validating silver/expected_silver_from_bronze.jsonl",
            lambda: validate_expected_silver_fixture(zone_ids), errors,
        )
        silver_rows = run_check(
            "Validating silver/silver_trips.jsonl",
            lambda: validate_silver_fixture(zone_ids), errors,
        )
        kafka_events = run_check(
            "Validating kafka/trip_events.jsonl",
            lambda: validate_kafka_fixture(zone_ids), errors,
        )
    else:
        errors.append(
            "Cannot validate trip fixtures: the taxi-zone lookup failed to load"
        )

    baseline_rows = run_check(
        "Validating gold/historical_zone_baseline.jsonl",
        validate_baseline_fixture, errors,
    )
    expected_live = run_check(
        "Validating gold/expected_live_zone_metrics.jsonl",
        validate_expected_live_fixture, errors,
    )

    if silver_rows and kafka_events:
        run_check(
            "Validating Silver <-> Kafka fixture consistency",
            lambda: validate_silver_kafka_consistency(silver_rows, kafka_events),
            errors,
        )

    if kafka_events and baseline_rows and expected_live:
        run_check(
            "Validating cross-contract invariants",
            lambda: validate_cross_contract_invariants(
                kafka_events, baseline_rows, expected_live
            ),
            errors,
        )
    elif not errors:
        errors.append(
            "Cannot validate cross-contract invariants: fixture loading failed"
        )

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
