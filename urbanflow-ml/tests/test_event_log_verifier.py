from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import sys

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from tools.verify_spark_event_log import summarize_event_logs


def task_end(stage_id: int, partition_id: int, executor_id: str, *, successful: bool = True, reason: str = "Success"):
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage_id,
        "Task Info": {
            "Index": partition_id,
            "Executor ID": executor_id,
            "Successful": successful,
        },
        "Task End Reason": {"Reason": reason},
    }


def stage_completed(stage_id: int, *, failure_reason: str | None = None):
    return {
        "Event": "SparkListenerStageCompleted",
        "Stage Info": {
            "Stage ID": stage_id,
            "Failure Reason": failure_reason,
        },
    }


class EventLogVerifierTest(unittest.TestCase):
    def write_log(self, events: list[dict]) -> str:
        tmp = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        with tmp:
            for event in events:
                tmp.write(json.dumps(event) + "\n")
        return tmp.name

    def test_two_executors_pass(self):
        path = self.write_log(
            [
                task_end(1, 0, "1"),
                task_end(1, 1, "2"),
                stage_completed(1),
            ]
        )

        summary = summarize_event_logs([path])

        self.assertEqual(summary["status"], "PASS")
        self.assertEqual(summary["successful_task_counts"], {"1": 1, "2": 1})

    def test_driver_only_fails(self):
        path = self.write_log([task_end(1, 0, "driver"), task_end(1, 1, "driver")])

        summary = summarize_event_logs([path])

        self.assertEqual(summary["status"], "FAIL")
        self.assertEqual(summary["non_driver_executor_count"], 0)

    def test_failed_task_fails(self):
        path = self.write_log(
            [
                task_end(1, 0, "1"),
                task_end(1, 1, "2", successful=False, reason="ExceptionFailure"),
            ]
        )

        summary = summarize_event_logs([path])

        self.assertEqual(summary["status"], "FAIL")
        self.assertEqual(summary["failed_tasks"], 1)

    def test_failed_stage_fails(self):
        path = self.write_log(
            [
                task_end(1, 0, "1"),
                task_end(1, 1, "2"),
                stage_completed(1, failure_reason="Job aborted"),
            ]
        )

        summary = summarize_event_logs([path])

        self.assertEqual(summary["status"], "FAIL")
        self.assertEqual(summary["failed_stages"], 1)

    def test_speculative_duplicate_success_counts_partition_once(self):
        path = self.write_log(
            [
                task_end(1, 0, "1"),
                task_end(1, 0, "2"),
                task_end(1, 1, "2"),
            ]
        )

        summary = summarize_event_logs([path])

        self.assertEqual(summary["status"], "PASS")
        self.assertEqual(sum(summary["successful_task_counts"].values()), 2)


if __name__ == "__main__":
    unittest.main()
