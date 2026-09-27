"""Helpers for collecting small aggregated Spark results for plotting."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd


def records_to_frame(records: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    """Build a local pandas frame from already-aggregated records."""
    return pd.DataFrame([dict(record) for record in records])
