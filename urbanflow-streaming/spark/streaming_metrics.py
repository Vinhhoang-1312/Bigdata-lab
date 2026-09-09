"""Thread-safe Spark query progress metrics suitable for UI/JSON export."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from threading import Lock
from typing import Any

from pyspark.sql.streaming import StreamingQueryListener


@dataclass(frozen=True)
class MetricsSnapshot:
    query_name: str = ""
    batch_id: int = -1
    input_rows: int = 0
    input_rows_per_second: float = 0.0
    processed_rows_per_second: float = 0.0
    batch_duration_ms: int = 0
    processing_latency_ms: int = 0
    backlog_rows: int | None = None
    kafka_end_offsets: dict[str, Any] | None = None
    invalid_events: int = 0
    output_rows: int = 0


def snapshot_from_progress(progress: dict[str, Any], invalid_events: int = 0) -> MetricsSnapshot:
    durations = progress.get("durationMs", {})
    sources = progress.get("sources", [])
    backlog = sum(int(s.get("numInputRows", 0)) for s in sources if s.get("isTriggerActive") is True) or None
    offsets = {s.get("description", str(i)): s.get("endOffset") for i, s in enumerate(sources)}
    return MetricsSnapshot(
        query_name=progress.get("name", ""), batch_id=int(progress.get("batchId", -1)),
        input_rows=int(progress.get("numInputRows", 0)),
        input_rows_per_second=float(progress.get("inputRowsPerSecond", 0.0)),
        processed_rows_per_second=float(progress.get("processedRowsPerSecond", 0.0)),
        batch_duration_ms=int(durations.get("triggerExecution", 0)),
        processing_latency_ms=int(durations.get("addBatch", durations.get("triggerExecution", 0))),
        backlog_rows=backlog, kafka_end_offsets=offsets,
        invalid_events=invalid_events,
        output_rows=int(progress.get("sink", {}).get("numOutputRows", 0)),
    )


class UrbanFlowMetricsListener(StreamingQueryListener):
    def __init__(self) -> None:
        super().__init__()
        self._lock, self._snapshot, self._invalid = Lock(), MetricsSnapshot(), 0

    def record_invalid(self, count: int) -> None:
        with self._lock:
            self._invalid += count

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return asdict(self._snapshot)

    def onQueryProgress(self, event) -> None:  # noqa: N802 - Spark API
        with self._lock:
            progress = event.progress.json
            import json
            self._snapshot = snapshot_from_progress(json.loads(progress), self._invalid)

    def onQueryStarted(self, event) -> None:  # noqa: N802
        return None

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        return None

    def onQueryIdle(self, event) -> None:  # noqa: N802 - required by Spark 4
        return None
