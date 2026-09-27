#!/usr/bin/env python3
"""Download the frozen NYC TLC source files using only the standard library."""

from __future__ import annotations

import argparse
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

TRIP_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{period}.parquet"
)
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def download(url: str, destination: Path) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and destination.stat().st_size > 0:
        print(f"already_present={destination} bytes={destination.stat().st_size}")
        return destination.stat().st_size

    temporary = destination.with_suffix(destination.suffix + ".part")
    print(f"downloading={url}")
    try:
        with urllib.request.urlopen(url, timeout=60) as response, temporary.open(
            "wb"
        ) as output:
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    temporary.replace(destination)
    size = destination.stat().st_size
    print(f"downloaded={destination} bytes={size}")
    return size


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--period", action="append", required=True, help="YYYY-MM")
    parser.add_argument("--output-dir", default="/workspace/local-data/nyc-tlc")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    periods = sorted(set(args.period))
    for period in periods:
        try:
            datetime.strptime(period, "%Y-%m")
        except ValueError as error:
            raise ValueError(
                f"Invalid period {period!r}; expected YYYY-MM"
            ) from error

    total_bytes = 0
    for period in periods:
        total_bytes += download(
            TRIP_URL.format(period=period),
            output_dir / f"yellow_tripdata_{period}.parquet",
        )
    total_bytes += download(ZONE_URL, output_dir / "taxi_zone_lookup.csv")
    print(f"source_files={len(periods) + 1}")
    print(f"source_bytes={total_bytes}")
    print("SOURCE ACQUISITION = PASS")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"SOURCE ACQUISITION = FAIL: {error}", file=sys.stderr)
        raise
