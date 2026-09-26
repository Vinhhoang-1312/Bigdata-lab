"""Read the immutable Spark MLlib trip anomaly result from SeaweedFS."""

from __future__ import annotations

import json
import os


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
