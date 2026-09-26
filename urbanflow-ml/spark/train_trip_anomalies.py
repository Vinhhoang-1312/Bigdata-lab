#!/usr/bin/env python3
"""Fit a distributed trip-pattern model and publish a bounded February review sample."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
import sys

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession, functions as F

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from forecasting.trip_anomaly import explain_deviation, score_distance

BUCKET = "urbanflow"
DEFAULT_RUN_ID = "trip-anomaly-20260926-001"
FEATURE_COLUMNS = ("log_distance", "log_duration", "log_fare", "log_speed")


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--silver", default="s3a://urbanflow/silver/trips/")
    parser.add_argument("--fraction", type=float, default=0.02)
    parser.add_argument("--top", type=int, default=80)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    if not 0 < args.fraction <= 1 or not 1 <= args.top <= 200:
        parser.error("fraction must be in (0, 1] and top must be in [1, 200]")
    return args


def s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3", endpoint_url=os.getenv("URBANFLOW_S3_ENDPOINT", "http://seaweedfs:8333"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}),
    )


def prepare_sample(spark: SparkSession, silver: str, year: int, month: int,
                   fraction: float, seed: int):
    path = f"{silver.rstrip('/')}/year={year}/month={month}/"
    source = spark.read.parquet(path)
    valid = source.where(
        (F.col("pickup_zone_id") > 0) & (F.col("dropoff_zone_id") > 0)
        & (F.col("trip_distance") > 0) & (F.col("trip_duration_minutes") > 0)
        & (F.col("fare_amount") >= 0)
        & (F.col("pickup_datetime") >= F.lit(f"{year}-{month:02d}-01"))
        & (F.col("pickup_datetime") < F.add_months(F.lit(f"{year}-{month:02d}-01"), 1))
    )
    sampled = valid.sample(withReplacement=False, fraction=fraction, seed=seed)
    return sampled.select(
        "pickup_datetime", "pickup_zone_id", "dropoff_zone_id", "trip_distance",
        "trip_duration_minutes", "fare_amount",
        F.log1p("trip_distance").alias("log_distance"),
        F.log1p("trip_duration_minutes").alias("log_duration"),
        F.log1p("fare_amount").alias("log_fare"),
        F.log1p(F.col("trip_distance") * 60 / F.col("trip_duration_minutes")).alias("log_speed"),
    )


def main() -> int:
    args = arguments()
    key = f"dev/ml/anomaly/runs/{args.run_id}/result.json"
    client = s3_client()
    try:
        prior = json.loads(client.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except client.exceptions.NoSuchKey:
        prior = None
    if prior is not None:
        if args.resume and prior.get("status") == "PASS":
            print(json.dumps({"status": "PASS", "resumed": True, "artifact": f"s3a://{BUCKET}/{key}"}))
            return 0
        raise FileExistsError(f"Anomaly artifact already exists: s3a://{BUCKET}/{key}")

    spark = (SparkSession.builder.appName("UrbanFlow Trip Anomaly MLlib")
             .config("spark.sql.session.timeZone", "America/New_York")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        hosts = sorted(set(spark.sparkContext.parallelize(range(64), 64)
                           .map(lambda _: socket.gethostname()).collect()))
        if len(hosts) < 2:
            raise RuntimeError(f"Distributed training requires two Spark worker hosts; found {hosts}")

        assembler = VectorAssembler(inputCols=list(FEATURE_COLUMNS), outputCol="raw_features")
        train = assembler.transform(prepare_sample(spark, args.silver, 2022, 11,
                                                   args.fraction, 20260926)).cache()
        calibration = assembler.transform(prepare_sample(spark, args.silver, 2023, 1,
                                                         args.fraction, 20260927)).cache()
        test = assembler.transform(prepare_sample(spark, args.silver, 2023, 2,
                                                  args.fraction, 20260928)).cache()
        counts = {"train": train.count(), "calibration": calibration.count(), "test": test.count()}
        if min(counts.values()) < 1000:
            raise ValueError(f"Not enough sampled trips to fit/calibrate/screen: {counts}")

        scaler = StandardScaler(inputCol="raw_features", outputCol="features",
                                withMean=True, withStd=True).fit(train)
        scaled_train = scaler.transform(train)
        model = KMeans(k=8, seed=20260926, maxIter=12, featuresCol="features",
                       predictionCol="cluster").fit(scaled_train)
        centers = [[float(value) for value in center] for center in model.clusterCenters()]

        @F.udf("double")
        def score(features, cluster):
            return score_distance(features, centers[int(cluster)])

        def scored(frame):
            assigned = model.transform(scaler.transform(frame))
            return assigned.withColumn("score", score("features", "cluster"))

        threshold = float(scored(calibration).approxQuantile("score", [0.995], 0.001)[0])
        screened = scored(test).cache()
        flagged = screened.where(F.col("score") >= threshold)
        flagged_count = flagged.count()
        top = (flagged.select(
            "pickup_zone_id", "dropoff_zone_id", "trip_distance",
            "trip_duration_minutes", "fare_amount", "score", "cluster", "features",
            F.date_format("pickup_datetime", "yyyy-MM-dd'T'HH:mm:ssXXX").alias("pickup_at"),
        ).orderBy(F.desc("score"), "pickup_at", "pickup_zone_id", "dropoff_zone_id")
            .limit(args.top).collect())
        trips = []
        for index, row in enumerate(top, 1):
            values = list(row.features)
            center = centers[int(row.cluster)]
            trips.append({
                "rank": index, "pickup_at": row.pickup_at,
                "pickup_zone_id": int(row.pickup_zone_id),
                "dropoff_zone_id": int(row.dropoff_zone_id),
                "trip_distance_miles": float(row.trip_distance),
                "trip_duration_minutes": float(row.trip_duration_minutes),
                "fare_amount_usd": float(row.fare_amount),
                "score": float(row.score), "reason": explain_deviation(values, center),
            })
        result = {
            "status": "PASS", "run_id": args.run_id, "method": "spark_mllib_kmeans",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "source_uri": args.silver, "model_artifact_uri": f"s3a://{BUCKET}/{key}",
            "training_period": "2022-11", "calibration_period": "2023-01",
            "screening_period": "2023-02", "sample_fraction": args.fraction,
            "training_sample_count": counts["train"],
            "calibration_sample_count": counts["calibration"],
            "screened_sample_count": counts["test"],
            "flagged_sample_count": flagged_count,
            "reported_count": len(trips), "threshold": threshold,
            "threshold_percentile": 99.5, "cluster_count": len(centers),
            "spark_worker_hosts": hosts,
            "features": list(FEATURE_COLUMNS),
            "scaler_mean": [float(value) for value in scaler.mean],
            "scaler_std": [float(value) for value in scaler.std],
            "cluster_centers": centers,
            "trips": trips,
            "limitations": [
                "No ground-truth anomaly labels; precision and recall are unknown.",
                "Only a reproducible 2% sample of February trips was screened.",
                "The model compares distance, duration, fare and speed; it does not model route geometry.",
                "A high score requests human review and is not evidence of fraud or unsafe driving.",
                "The map connects taxi-zone centers, not the vehicle's GPS path.",
            ],
        }
        client.put_object(Bucket=BUCKET, Key=key,
                          Body=json.dumps(result, ensure_ascii=True).encode("utf-8"),
                          ContentType="application/json")
        print(json.dumps({"status": "PASS", "artifact": result["model_artifact_uri"],
                          "sample_counts": counts, "flagged": flagged_count,
                          "reported": len(trips), "worker_hosts": hosts}))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
