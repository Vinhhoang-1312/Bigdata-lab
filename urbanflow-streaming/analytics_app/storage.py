"""S3-compatible read options shared by thin analytics readers."""
from __future__ import annotations

import os
from typing import Any


def storage_options(path: str) -> dict[str, Any]:
    if not path.startswith(("s3://", "s3a://")):
        return {}
    endpoint = os.getenv("URBANFLOW_S3_ENDPOINT") or os.getenv("AWS_ENDPOINT_URL")
    options: dict[str, Any] = {}
    if endpoint:
        options["client_kwargs"] = {"endpoint_url": endpoint}
        options["use_ssl"] = endpoint.startswith("https://")
    key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    if key:
        options["key"] = key
    if secret:
        options["secret"] = secret
    return {"storage_options": options}


def readable_path(path: str) -> str:
    return "s3://" + path[len("s3a://"):] if path.startswith("s3a://") else path
