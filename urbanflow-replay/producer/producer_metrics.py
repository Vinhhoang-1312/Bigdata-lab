"""Replay run metrics with JSON-safe reporting."""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime


@dataclass
class ProducerMetrics:
    requested_events_per_second: float
    records_read: int = 0
    records_serialized: int = 0
    records_rejected: int = 0
    records_sent: int = 0
    send_errors: int = 0
    started_at: str = field(
        default_factory=lambda: datetime.now(UTC).isoformat().replace("+00:00", "Z")
    )
    send_duration_seconds: float = 0.0
    _start_clock: float = field(default_factory=time.perf_counter, repr=False)

    def finish(self) -> None:
        self.send_duration_seconds = max(0.0, time.perf_counter() - self._start_clock)

    @property
    def achieved_events_per_second(self) -> float:
        if self.send_duration_seconds <= 0:
            return 0.0
        return self.records_sent / self.send_duration_seconds

    def as_dict(self) -> dict[str, object]:
        result = asdict(self)
        result.pop("_start_clock", None)
        result["achieved_events_per_second"] = self.achieved_events_per_second
        return result

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), sort_keys=True)
