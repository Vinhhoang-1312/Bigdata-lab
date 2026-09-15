#!/usr/bin/env python3
"""Build and write the experimental 30-minute zone demand frame."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from forecasting.demand_frame import build_demand_frame

DEFAULT_SILVER = "s3a://urbanflow/silver/trips/"
DEFAULT_ZONE_LOOKUP = "/workspace/urbanflow-foundation/fixtures/bronze/zone_lookup.csv"
DEFAULT_OUTPUT_ROOT = "s3a://urbanflow/dev/ml/demand_frame"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-path", default=DEFAULT_SILVER)
    parser.add_argument("--zone-lookup", default=DEFAULT_ZONE_LOOKUP)
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("UrbanFlow ML Demand Frame")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        silver = spark.read.parquet(args.silver_path)
        zone_lookup = spark.read.option("header", True).csv(args.zone_lookup)
        frame = build_demand_frame(silver, zone_lookup, args.start_date, args.end_date)
        output_path = f"{args.output_root.rstrip('/')}/{args.run_id}/"
        to_write = (
            frame.withColumn("year", F.year("service_date").cast("int"))
            .withColumn("month_partition", F.month("service_date").cast("int"))
        )
        to_write.write.mode("errorifexists").partitionBy("year", "month_partition").parquet(output_path)
        summary = {
            "status": "PASS",
            "run_id": args.run_id,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "output_path": output_path,
            "rows": int(frame.count()),
            "zones": int(frame.select("pickup_zone_id").distinct().count()),
        }
        print(json.dumps(summary, sort_keys=True))
        print("ML DEMAND-FRAME = PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
