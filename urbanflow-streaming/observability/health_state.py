"""Health state classification for the UrbanFlow Glass Cockpit.

Semantic definitions:
  - trigger_load = batch_duration_ms / trigger_interval_ms
    (Duty cycle of Spark micro-batch execution relative to trigger period)
  - throughput_ratio = input_rows_per_second / processed_rows_per_second
    (Throughput pressure ratio; >1.0 indicates input arriving faster than batch processing)

Decision rules (in priority order):
  1. UNAVAILABLE:
     - Telemetry heartbeat missing, corrupt, or > 60 s old.
     - Spark Structured Streaming query is stopped (query_active is False).

  2. DEGRADED:
     - Backlog increasing while Spark processing is stalled
       (trend == 'increasing' and processed_rows_per_second == 0).
     - High trigger load (trigger_load > 0.80); Spark spends >80% of trigger window executing.
     - Input arriving but zero processed (input_rows_per_second > 0 and processed_rows_per_second == 0).

  3. STALE:
     - Input is expected (Kafka Source Backlog > 0 or input arriving), but batch_id has not
       advanced for 3 consecutive observations.
     - Input is expected, but Live Gold table has not received a new window for > 300 s.

  4. HEALTHY:
     - Legitimate source idle: 0 backlog, 0 eps input, query active, heartbeat fresh.
       (Unchanged batch_id or gold age > 300 s during idle periods is expected, NOT a failure).
     - Nominal streaming: Micro-batches actively advancing within comfortable trigger load (<= 0.80).
"""
from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from observability.telemetry_reader import telemetry_age_seconds


class HealthState(Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    STALE = "STALE"
    UNAVAILABLE = "UNAVAILABLE"


# Module-level rolling history (last 5 observations per process).
_backlog_history: deque[int | None] = deque(maxlen=5)
_batch_id_history: deque[int] = deque(maxlen=3)
_last_observation_timestamp: str | None = None


def _trigger_load(telemetry: dict[str, Any]) -> float | None:
    """Spark trigger load: batch_duration_ms / trigger_interval_ms."""
    tl = telemetry.get("trigger_load")
    if tl is not None:
        try:
            return float(tl)
        except (ValueError, TypeError):
            pass
    dur = telemetry.get("batch_duration_ms")
    interval = telemetry.get("trigger_interval_ms", 5000)
    if dur is not None and interval and interval > 0:
        return round(float(dur) / float(interval), 3)
    return None


def _throughput_ratio(telemetry: dict[str, Any]) -> float | None:
    """Throughput ratio: input_rows_per_second / processed_rows_per_second."""
    tr = telemetry.get("throughput_ratio")
    if tr is not None:
        try:
            return float(tr)
        except (ValueError, TypeError):
            pass
    proc = telemetry.get("processed_rows_per_second", 0.0)
    if not proc or proc <= 0:
        return None
    inp = telemetry.get("input_rows_per_second", 0.0)
    return round(float(inp) / float(proc), 3) if inp else 0.0


def _backlog_total(telemetry: dict[str, Any]) -> int | None:
    """Total Kafka source backlog across partitions."""
    tot = telemetry.get("kafka_total_backlog_rows")
    if tot is not None:
        try:
            return int(tot)
        except (ValueError, TypeError):
            pass
    per_part = telemetry.get("kafka_per_partition_backlog")
    if isinstance(per_part, dict):
        try:
            return sum(int(v.get("backlog", 0)) for v in per_part.values() if isinstance(v, dict))
        except (TypeError, ValueError):
            pass
    return None


def _backlog_trend() -> str | None:
    """Classify trend from the last 5 backlog observations."""
    values = [v for v in _backlog_history if v is not None]
    if len(values) < 2:
        return None
    if all(values[i] > values[i - 1] for i in range(1, len(values))):
        return "increasing"
    if all(values[i] < values[i - 1] for i in range(1, len(values))):
        return "decreasing"
    diff = values[-1] - values[0]
    if diff > 5:
        return "increasing"
    if diff < -5:
        return "decreasing"
    return "stable"


def _watermark_advancing(telemetry: dict[str, Any]) -> bool | None:
    wm = telemetry.get("watermark")
    return bool(wm and str(wm).strip())


def classify_health(
    telemetry: dict[str, Any] | None,
    live_gold_age_seconds: int | None,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Return health classification adhering to the 4-state priority hierarchy."""
    now = now_utc or datetime.now(timezone.utc)
    age = telemetry_age_seconds(telemetry, now)

    # =========================================================================
    # 1. UNAVAILABLE CHECKS
    # =========================================================================
    if telemetry is None:
        return {
            "state": HealthState.UNAVAILABLE.value,
            "reason": "Telemetry heartbeat is missing.",
            "telemetry_age_seconds": None,
            "trigger_load": None,
            "throughput_ratio": None,
            "batch_id": None,
            "watermark_advancing": None,
            "backlog_trend": None,
            "source_idle": None,
        }

    if age is None or age > 60:
        return {
            "state": HealthState.UNAVAILABLE.value,
            "reason": f"Telemetry heartbeat is stale ({age} s old, threshold 60 s).",
            "telemetry_age_seconds": age,
            "trigger_load": None,
            "throughput_ratio": None,
            "batch_id": None,
            "watermark_advancing": None,
            "backlog_trend": None,
            "source_idle": None,
        }

    query_active = telemetry.get("query_active", True)
    if not query_active:
        return {
            "state": HealthState.UNAVAILABLE.value,
            "reason": "Spark Structured Streaming query is stopped or terminated.",
            "telemetry_age_seconds": age,
            "trigger_load": None,
            "throughput_ratio": None,
            "batch_id": telemetry.get("batch_id"),
            "watermark_advancing": False,
            "backlog_trend": None,
            "source_idle": None,
        }

    batch_id = telemetry.get("batch_id")
    tl = _trigger_load(telemetry)
    tr = _throughput_ratio(telemetry)
    bl = _backlog_total(telemetry)

    # Track each telemetry heartbeat at most once. Repeated HTTP/UI reads must not
    # manufacture history or change health state merely because clients poll faster.
    global _last_observation_timestamp
    if not _backlog_history and not _batch_id_history:
        _last_observation_timestamp = None
    observation_timestamp = str(telemetry.get("timestamp") or "")
    if observation_timestamp != _last_observation_timestamp:
        _backlog_history.append(bl)
        if batch_id is not None:
            _batch_id_history.append(batch_id)
        _last_observation_timestamp = observation_timestamp

    bt = _backlog_trend()
    wm_ok = _watermark_advancing(telemetry)

    # Evaluate whether new source input is actively arriving or expected
    inp = telemetry.get("input_rows_per_second", 0.0)
    proc = telemetry.get("processed_rows_per_second", 0.0)
    num_input = telemetry.get("num_input_rows", 0)

    has_pending_backlog = bl is not None and bl > 0
    has_active_input = (inp is not None and inp > 0) or (num_input is not None and num_input > 0)
    input_expected = has_pending_backlog or has_active_input
    source_idle = not input_expected

    # =========================================================================
    # 2. STALE CHECKS (Only when input is actively expected/pending)
    # =========================================================================
    # A. Batch stuck while input is expected (3 consecutive observations unchanged)
    batch_stuck = len(_batch_id_history) >= 3 and len(set(_batch_id_history)) == 1
    if batch_stuck and input_expected:
        return {
            "state": HealthState.STALE.value,
            "reason": f"Batch ID {batch_id} has not advanced for 3 consecutive observations despite pending input (backlog: {bl}, input: {inp} eps).",
            "telemetry_age_seconds": age,
            "trigger_load": tl,
            "throughput_ratio": tr,
            "batch_id": batch_id,
            "watermark_advancing": wm_ok,
            "backlog_trend": bt,
            "source_idle": False,
        }

    # B. Live Gold table has not received a new window for > 300 s while input is expected
    if live_gold_age_seconds is not None and live_gold_age_seconds > 300 and input_expected:
        return {
            "state": HealthState.STALE.value,
            "reason": f"Live Gold table has not received a new window for {live_gold_age_seconds} s (threshold 300 s) despite pending input.",
            "telemetry_age_seconds": age,
            "trigger_load": tl,
            "throughput_ratio": tr,
            "batch_id": batch_id,
            "watermark_advancing": wm_ok,
            "backlog_trend": bt,
            "source_idle": False,
        }


    # =========================================================================
    # 3. DEGRADED CHECKS
    # =========================================================================
    # A. Backlog increasing while processing is stalled
    if bt == "increasing" and (proc is None or proc == 0):
        return {
            "state": HealthState.DEGRADED.value,
            "reason": f"Kafka source backlog is increasing ({bl} rows) while Spark micro-batch processing is stalled.",
            "telemetry_age_seconds": age,
            "trigger_load": tl,
            "throughput_ratio": tr,
            "batch_id": batch_id,
            "watermark_advancing": wm_ok,
            "backlog_trend": bt,
            "source_idle": False,
        }

    # B. High trigger load (> 0.80): Spark execution occupies >80% of trigger interval
    if tl is not None and tl > 0.80:
        dur_ms = telemetry.get("batch_duration_ms", 0)
        int_ms = telemetry.get("trigger_interval_ms", 5000)
        return {
            "state": HealthState.DEGRADED.value,
            "reason": f"High trigger load ({tl:.3f} > 0.80); execution ({dur_ms} ms) exceeds 80% of trigger interval ({int_ms} ms).",
            "telemetry_age_seconds": age,
            "trigger_load": tl,
            "throughput_ratio": tr,
            "batch_id": batch_id,
            "watermark_advancing": wm_ok,
            "backlog_trend": bt,
            "source_idle": source_idle,
        }

    # C. Events arriving into Spark but zero rows processed
    if inp and inp > 0 and (proc is None or proc == 0):
        return {
            "state": HealthState.DEGRADED.value,
            "reason": f"Events arriving ({inp:.1f} eps) but processing rate is 0.0 rows/s.",
            "telemetry_age_seconds": age,
            "trigger_load": tl,
            "throughput_ratio": tr,
            "batch_id": batch_id,
            "watermark_advancing": wm_ok,
            "backlog_trend": bt,
            "source_idle": False,
        }

    # =========================================================================
    # 4. HEALTHY (Legitimate idle source OR nominal advancing stream)
    # =========================================================================
    if source_idle:
        return {
            "state": HealthState.HEALTHY.value,
            "reason": "Pipeline nominal and ready; source is legitimately idle (0 backlog, 0 eps).",
            "telemetry_age_seconds": age,
            "trigger_load": tl,
            "throughput_ratio": tr,
            "batch_id": batch_id,
            "watermark_advancing": wm_ok,
            "backlog_trend": bt,
            "source_idle": True,
        }

    return {
        "state": HealthState.HEALTHY.value,
        "reason": "All systems nominal; micro-batches actively advancing within trigger cadence.",
        "telemetry_age_seconds": age,
        "trigger_load": tl,
        "throughput_ratio": tr,
        "batch_id": batch_id,
        "watermark_advancing": wm_ok,
        "backlog_trend": bt,
        "source_idle": False,
    }
