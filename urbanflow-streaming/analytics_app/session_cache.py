"""Per-browser-session snapshots for read-only dashboard data."""
from __future__ import annotations

from collections.abc import Callable, MutableMapping
from typing import TypeVar


T = TypeVar("T")
_SOURCE_KEY = "urbanflow_gold_source_paths"
_SNAPSHOT_KEY = "urbanflow_gold_snapshot"


def load_once_per_session(
    session: MutableMapping[str, object],
    source_paths: tuple[str, str, str],
    loader: Callable[[], T],
) -> T:
    """Return a stable Gold snapshot until its configured source changes."""
    if session.get(_SOURCE_KEY) != source_paths or _SNAPSHOT_KEY not in session:
        session[_SOURCE_KEY] = source_paths
        session[_SNAPSHOT_KEY] = loader()
    return session[_SNAPSHOT_KEY]  # type: ignore[return-value]
