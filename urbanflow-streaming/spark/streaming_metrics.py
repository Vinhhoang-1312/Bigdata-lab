"""Thread-safe Spark query progress metrics suitable for UI/JSON export."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from threading import Lock
from typing import Any

try:
    from pyspark.sql.streaming import StreamingQueryListener
except ImportError:
    class StreamingQueryListener:  # type: ignore[no-redef]
        """Stub for environments without PySpark installed (e.g. host test runners)."""
        pass


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
    watermark: str | None = None
    max_event_time: str | None = None
    kafka_partition_end_offsets: dict[str, int] | None = None
    kafka_partition_latest_offsets: dict[str, int] | None = None
    trigger_interval_ms: int = 5000
    trigger_load: float | None = None
    throughput_ratio: float | None = None


def _extract_partition_offsets(raw_offset: Any) -> dict[str, int] | None:
    if not raw_offset:
        return None
    import json
    parsed = raw_offset
    if isinstance(raw_offset, str):
        try:
            parsed = json.loads(raw_offset)
        except Exception:
            return None
    if not isinstance(parsed, dict):
        return None
    # Check if nested by topic: {"urbanflow.trip-events": {"0": 123, "1": 456}}
    for val in parsed.values():
        if isinstance(val, dict):
            return {str(k): int(v) for k, v in val.items()}
    # Flat format: {"0": 123, "1": 456}
    return {str(k): int(v) for k, v in parsed.items()}


def snapshot_from_progress(
    progress: dict[str, Any], invalid_events: int = 0, trigger_interval_ms: int = 5000
) -> MetricsSnapshot:
    durations = progress.get("durationMs", {})
    sources = progress.get("sources", [])
    backlog = sum(int(s.get("numInputRows", 0)) for s in sources if s.get("isTriggerActive") is True) or None
    offsets = {s.get("description", str(i)): s.get("endOffset") for i, s in enumerate(sources)}
    
    part_end: dict[str, int] | None = None
    part_latest: dict[str, int] | None = None
    if sources:
        first_src = sources[0]
        part_end = _extract_partition_offsets(first_src.get("endOffset"))
        part_latest = _extract_partition_offsets(first_src.get("latestOffset"))

    event_time = progress.get("eventTime", {})
    watermark_str = event_time.get("watermark") if event_time else None
    max_event = event_time.get("max") if event_time else None

    batch_duration_ms = int(durations.get("triggerExecution", 0))
    # Correct trigger_load formula: trigger_execution_ms / trigger_interval_ms
    trigger_load = (
        round(batch_duration_ms / trigger_interval_ms, 3)
        if trigger_interval_ms > 0
        else None
    )

    in_rate = float(progress.get("inputRowsPerSecond", 0.0))
    proc_rate = float(progress.get("processedRowsPerSecond", 0.0))
    # Separate throughput_ratio: input_rows_per_second / processed_rows_per_second
    throughput_ratio = round(in_rate / proc_rate, 3) if proc_rate > 0 else None

    return MetricsSnapshot(
        query_name=progress.get("name", ""), batch_id=int(progress.get("batchId", -1)),
        input_rows=int(progress.get("numInputRows", 0)),
        input_rows_per_second=in_rate,
        processed_rows_per_second=proc_rate,
        batch_duration_ms=batch_duration_ms,
        processing_latency_ms=int(durations.get("addBatch", durations.get("triggerExecution", 0))),
        backlog_rows=backlog, kafka_end_offsets=offsets,
        invalid_events=invalid_events,
        output_rows=int(progress.get("sink", {}).get("numOutputRows", 0)),
        watermark=watermark_str, max_event_time=max_event,
        kafka_partition_end_offsets=part_end,
        kafka_partition_latest_offsets=part_latest,
        trigger_interval_ms=trigger_interval_ms,
        trigger_load=trigger_load,
        throughput_ratio=throughput_ratio,
    )


class UrbanFlowMetricsListener(StreamingQueryListener):
    def __init__(self, trigger_interval_ms: int = 5000) -> None:
        super().__init__()
        self._lock = Lock()
        self._snapshots: dict[str, MetricsSnapshot] = {}
        self._snapshot = MetricsSnapshot(trigger_interval_ms=trigger_interval_ms)
        self._invalid_total = 0
        self._trigger_interval_ms = trigger_interval_ms

    def record_invalid(self, count: int) -> None:
        with self._lock:
            self._invalid_total += count

    def snapshot(self, query_name: str | None = None) -> dict[str, Any]:
        with self._lock:
            if query_name and query_name in self._snapshots:
                snap = self._snapshots[query_name]
            elif "live_gold" in self._snapshots:
                snap = self._snapshots["live_gold"]
            elif self._snapshots:
                candidates = [s for name, s in self._snapshots.items() if name != "invalid_events"]
                snap = candidates[0] if candidates else next(iter(self._snapshots.values()))
            else:
                snap = self._snapshot
            d = asdict(snap)
            d["invalid_events"] = self._invalid_total
            return d

    def onQueryProgress(self, event) -> None:  # noqa: N802 - Spark API
        with self._lock:
            progress_json = event.progress.json
            import json
            progress = json.loads(progress_json)
            name = progress.get("name", "")

            # Invalid rows are counted by the foreachBatch sink callback. Spark's
            # numInputRows here describes source rows before the filter and would overcount.
            snap = snapshot_from_progress(
                progress, self._invalid_total, self._trigger_interval_ms
            )
            self._snapshots[name] = snap
            if name != "invalid_events":
                self._snapshot = snap

    def onQueryStarted(self, event) -> None:  # noqa: N802
        return None

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        return None

    def onQueryIdle(self, event) -> None:  # noqa: N802 - required by Spark 4
        return None
