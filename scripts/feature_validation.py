"""Validation helpers that avoid Spark's pandas bridge on Python 3.12."""

from __future__ import annotations

from typing import Any, Iterable, Mapping


def describe_rows_to_dict(rows: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    """Convert collected summary rows to the shape returned by pandas ``to_dict``."""
    materialized = [dict(row) for row in rows]
    if not materialized:
        return {}
    columns = list(materialized[0])
    return {
        column: {str(index): row.get(column) for index, row in enumerate(materialized)}
        for column in columns
    }
