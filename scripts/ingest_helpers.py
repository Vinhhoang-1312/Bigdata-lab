"""Small ingestion helpers that do not depend on Spark's pandas bridge."""

from __future__ import annotations

from typing import Any

import pandas as pd


def metadata_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    """Return plain Python records for Spark DataFrame construction."""
    columns = ["patch_id", "country", "split", "labels"]
    records = frame.loc[:, columns].to_dict(orient="records")
    for record in records:
        labels = record.get("labels")
        if hasattr(labels, "tolist"):
            record["labels"] = labels.tolist()
        elif labels is None:
            record["labels"] = None
        else:
            record["labels"] = list(labels)
    return records
