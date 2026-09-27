"""Read the immutable Spark MLlib trip anomaly result from SeaweedFS."""

from __future__ import annotations

import json
import math
import os
from functools import lru_cache

from forecasting.trip_anomaly import (
    raw_trip_features,
    score_nearest_cluster,
    standardize_features,
)


DEFAULT_RUN_ID = "trip-anomaly-20260926-001"


def validate_anomaly_snapshot(payload: dict) -> dict:
    if payload.get("status") != "PASS":
        raise ValueError("anomaly run is not complete")
    required = ("run_id", "method", "threshold", "screened_sample_count",
                "flagged_sample_count", "trips")
    if any(key not in payload for key in required):
        raise ValueError("anomaly sample manifest is incomplete")
    if not isinstance(payload["trips"], list):
        raise ValueError("anomaly sample trips must be a list")
    return payload


def validate_anomaly_inference_artifact(payload: dict) -> dict:
    validate_anomaly_snapshot(payload)
    required = ("features", "scaler_mean", "scaler_std", "cluster_centers")
    if any(key not in payload for key in required):
        raise ValueError("anomaly inference artifact is incomplete")
    if not isinstance(payload["features"], list) or len(payload["features"]) != 4:
        raise ValueError("anomaly inference artifact features must have four dimensions")
    if not isinstance(payload["cluster_centers"], list) or not payload["cluster_centers"]:
        raise ValueError("anomaly inference artifact cluster centers are invalid")
    if not isinstance(payload["threshold"], (int, float)) or not math.isfinite(float(payload["threshold"])):
        raise ValueError("anomaly inference artifact threshold is invalid")
    if float(payload["threshold"]) < 0:
        raise ValueError("anomaly inference artifact threshold is invalid")
    try:
        standardize_features(payload["scaler_mean"], payload["scaler_mean"], payload["scaler_std"])
        for center in payload["cluster_centers"]:
            standardize_features(center, center, [1.0, 1.0, 1.0, 1.0])
    except (TypeError, ValueError) as exc:
        raise ValueError("anomaly inference artifact metadata is invalid") from exc
    return payload


def score_trip(
    snapshot: dict,
    *,
    distance_miles: float,
    duration_minutes: float,
    fare_usd: float,
) -> dict:
    artifact = validate_anomaly_inference_artifact(snapshot)
    raw = raw_trip_features(distance_miles, duration_minutes, fare_usd)
    standardized = standardize_features(raw, artifact["scaler_mean"], artifact["scaler_std"])
    cluster, score, reason = score_nearest_cluster(standardized, artifact["cluster_centers"])
    threshold = float(artifact["threshold"])
    return {
        "run_id": artifact["run_id"],
        "method": artifact["method"],
        "score": float(score),
        "threshold": threshold,
        "review_required": bool(score >= threshold),
        "nearest_cluster": int(cluster),
        "reason": reason,
        "derived_speed_mph": float(distance_miles * 60.0 / duration_minutes),
        "features_used": list(artifact["features"]),
        "use_status": "research_only",
    }


def read_anomaly_snapshot(client, bucket: str, key: str) -> dict:
    response = client.get_object(Bucket=bucket, Key=key)
    return validate_anomaly_snapshot(json.loads(response["Body"].read()))


def load_anomaly_snapshot(run_id: str = DEFAULT_RUN_ID) -> dict:
    import boto3
    from botocore.config import Config

    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("URBANFLOW_S3_ENDPOINT", "http://seaweedfs:8333"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}),
    )
    key = f"dev/ml/anomaly/runs/{run_id}/result.json"
    result = read_anomaly_snapshot(client, "urbanflow", key)
    if result["run_id"] != run_id:
        raise ValueError("anomaly run ID mismatch")
    return result


@lru_cache(maxsize=1)
def get_anomaly_snapshot(run_id: str = DEFAULT_RUN_ID) -> dict:
    return load_anomaly_snapshot(run_id)
