#!/usr/bin/env python3
"""Prove equivalent pickup-zone trip counts with RDD, DataFrame, and SQL."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-path", default="s3a://urbanflow/silver/trips/")
    return parser.parse_args()


def rows_to_dict(rows) -> dict[int, int]:
    return {int(row[0]): int(row[1]) for row in rows}


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("UrbanFlow Spark API Equivalence").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    silver = spark.read.parquet(args.silver_path).select("pickup_zone_id").cache()

    rdd_result = dict(
        silver.rdd.map(lambda row: (int(row.pickup_zone_id), 1))
        .reduceByKey(lambda left, right: left + right)
        .collect()
    )
    dataframe_result = rows_to_dict(
        silver.groupBy("pickup_zone_id")
        .agg(F.count(F.lit(1)).alias("trip_count"))
        .collect()
    )
    silver.createOrReplaceTempView("silver_pickups")
    sql_result = rows_to_dict(
        spark.sql(
            "SELECT pickup_zone_id, COUNT(*) AS trip_count "
            "FROM silver_pickups GROUP BY pickup_zone_id"
        ).collect()
    )

    if not (rdd_result == dataframe_result == sql_result):
        raise AssertionError("RDD, DataFrame, and SQL results differ")
    print(f"zones={len(rdd_result)}")
    print(f"trips={sum(rdd_result.values())}")
    print("RDD = PASS")
    print("DATAFRAME = PASS")
    print("SQL = PASS")
    print("SPARK API EQUIVALENCE = PASS")
    silver.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
