"""Read Spark Master job presence without guessing Kafka consumer health."""

from __future__ import annotations

import json
from urllib.request import urlopen


def classify_stream_job(master_document: dict) -> str:
    active = master_document.get("activeapps")
    if not isinstance(active, list):
        return "unavailable"
    return "active" if any(
        isinstance(app, dict) and app.get("name") == "UrbanFlowLiveMetrics" for app in active
    ) else "inactive"


def read_stream_job_status(master_url: str) -> str:
    try:
        with urlopen(master_url, timeout=2) as response:
            document = json.load(response)
        return classify_stream_job(document)
    except (OSError, ValueError, TypeError):
        return "unavailable"
