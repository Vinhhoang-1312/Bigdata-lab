import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from .streaming_metrics import UrbanFlowMetricsListener
except ImportError:
    from streaming_metrics import UrbanFlowMetricsListener

def _telemetry_loop(listener: UrbanFlowMetricsListener, path: str, interval: float) -> None:
    p = Path(path)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(p.parent, 0o777)
    except Exception:
        pass
    tmp_path = p.with_suffix(".tmp")
    
    while True:
        try:
            snapshot = listener.snapshot()
            
            now_utc = datetime.now(timezone.utc).isoformat()
            batch_id = int(snapshot.get("batch_id", -1))
            query_active = batch_id >= 0
            batch_duration_ms = snapshot.get("batch_duration_ms", 0)
            trigger_interval_ms = snapshot.get("trigger_interval_ms", 5000)
            # Correct definition: trigger_execution_ms / trigger_interval_ms
            trigger_load = (
                round(batch_duration_ms / trigger_interval_ms, 3)
                if trigger_interval_ms > 0
                else None
            )
            in_rate = snapshot.get("input_rows_per_second", 0.0)
            out_rate = snapshot.get("processed_rows_per_second", 0.0)
            # Separate throughput ratio: input_rows_per_second / processed_rows_per_second
            throughput_ratio = round(in_rate / out_rate, 3) if out_rate > 0 else None
            
            part_end = snapshot.get("kafka_partition_end_offsets") or {}
            part_latest = snapshot.get("kafka_partition_latest_offsets") or {}
            per_partition_backlog: dict[str, dict[str, int]] = {}
            total_backlog = 0
            if part_end:
                for p_str, end_off in part_end.items():
                    lat_off = part_latest.get(p_str, end_off)
                    diff = max(0, lat_off - end_off)
                    total_backlog += diff
                    per_partition_backlog[p_str] = {
                        "spark_end_offset": end_off,
                        "latest_offset": lat_off,
                        "backlog": diff,
                    }

            telemetry = {
                "timestamp": now_utc,
                "query_active": query_active,
                "batch_id": batch_id,
                "input_rows_per_second": in_rate,
                "processed_rows_per_second": out_rate,
                "num_input_rows": snapshot.get("input_rows", 0),
                "batch_duration_ms": batch_duration_ms,
                "trigger_interval_ms": trigger_interval_ms,
                "trigger_load": trigger_load,
                "throughput_ratio": throughput_ratio,
                "watermark": snapshot.get("watermark"),
                "max_event_time": snapshot.get("max_event_time"),
                "kafka_source_backlog": snapshot.get("kafka_end_offsets"),
                "kafka_partition_end_offsets": part_end or None,
                "kafka_partition_latest_offsets": part_latest or None,
                "kafka_per_partition_backlog": per_partition_backlog or None,
                "kafka_total_backlog_rows": total_backlog if per_partition_backlog else None,
                "invalid_events_total": snapshot.get("invalid_events", 0),
                "output_rows": snapshot.get("output_rows", 0),
            }
            
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(telemetry, f)
            try:
                os.chmod(tmp_path, 0o666)
            except Exception:
                pass
            os.replace(tmp_path, p)
            try:
                os.chmod(p, 0o666)
            except Exception:
                pass
            
        except Exception as exc:
            import sys
            sys.stderr.write(f"[TELEMETRY_WRITER] Error: {exc}\n")
            sys.stderr.flush()
        
        time.sleep(interval)


def start_telemetry_writer(
    listener: UrbanFlowMetricsListener, 
    path: str = '/workspace/.telemetry/streaming_health.json', 
    interval: float = 5.0
) -> threading.Thread:
    thread = threading.Thread(
        target=_telemetry_loop,
        args=(listener, path, interval),
        daemon=True,
        name="TelemetryWriter"
    )
    thread.start()
    return thread
