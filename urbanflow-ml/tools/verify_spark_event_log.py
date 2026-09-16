#!/usr/bin/env python3
"""Verify Spark event logs prove non-driver distributed execution."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Iterable


def _event_lines(paths: Iterable[Path]):
    for path in paths:
        if path.is_dir():
            yield from _event_lines(sorted(child for child in path.iterdir() if child.is_file()))
        else:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if line:
                        yield json.loads(line)


def summarize_event_logs(paths: Iterable[str | Path]) -> dict:
    executor_successes: Counter[str] = Counter()
    seen_successful_partitions: set[tuple[int, int]] = set()
    failed_tasks = 0
    failed_stages = 0
    applications: list[str] = []

    resolved_paths = [Path(path) for path in paths]
    for event in _event_lines(resolved_paths):
        event_name = event.get("Event")
        if event_name == "SparkListenerApplicationStart":
            app_id = event.get("App ID") or event.get("App Name")
            if app_id:
                applications.append(str(app_id))
        elif event_name == "SparkListenerTaskEnd":
            task_info = event.get("Task Info", {})
            executor_id = str(task_info.get("Executor ID", "unknown"))
            stage_id = int(event.get("Stage ID", -1))
            partition_id = int(task_info.get("Index", task_info.get("Partition ID", -1)))
            successful = bool(task_info.get("Successful", False))
            if successful and executor_id != "driver":
                task_key = (stage_id, partition_id)
                if task_key not in seen_successful_partitions:
                    executor_successes[executor_id] += 1
                    seen_successful_partitions.add(task_key)
            else:
                reason = event.get("Task End Reason", {})
                if reason and reason.get("Reason") not in {"Success", None}:
                    failed_tasks += 1
        elif event_name == "SparkListenerStageCompleted":
            stage_info = event.get("Stage Info", {})
            if stage_info.get("Failure Reason"):
                failed_stages += 1

    successful_executor_ids = sorted(executor for executor, count in executor_successes.items() if count > 0)
    distributed = len(successful_executor_ids) >= 2 and failed_tasks == 0 and failed_stages == 0
    return {
        "status": "PASS" if distributed else "FAIL",
        "applications": applications,
        "successful_executor_ids": successful_executor_ids,
        "successful_task_counts": dict(sorted(executor_successes.items())),
        "failed_tasks": failed_tasks,
        "failed_stages": failed_stages,
        "non_driver_executor_count": len(successful_executor_ids),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+")
    args = parser.parse_args()
    summary = summarize_event_logs(args.paths)
    print(json.dumps(summary, indent=2, sort_keys=True))
    print(f"DISTRIBUTED = {summary['status']}")
    return 0 if summary["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
