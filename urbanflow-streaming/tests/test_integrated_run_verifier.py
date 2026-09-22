from __future__ import annotations

import importlib.util
import uuid
from pathlib import Path


def load_verifier():
    path = Path(__file__).with_name("verify_integrated_run.py")
    spec = importlib.util.spec_from_file_location("integrated_run", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_run_marker_matches_replay_event_suffix():
    verifier = load_verifier()

    assert verifier.run_marker("task6-20260914-001") == "-task6-20260914-001"


def test_markdown_records_pass_summary():
    verifier = load_verifier()
    summary = {
        "status": "PASS",
        "run_id": "task6-20260914-001",
        "replay_events": 10000,
        "live_rows": 50,
        "invalid_rows": 0,
        "mismatched_rows": 0,
        "output_path": "s3a://urbanflow/gold/live_zone_metrics/task6/",
        "checkpoint_path": "s3a://urbanflow/checkpoints/live_trip_metrics/task6/",
        "limitations": ["Run-scoped Live Gold path used for safe verification."],
    }

    text = verifier.render_markdown(summary)

    assert "Status: `PASS`" in text
    assert "task6-20260914-001" in text
    assert "Run-scoped Live Gold path" in text


def test_markdown_serializes_spark_progress_ids():
    verifier = load_verifier()
    progress_id = uuid.uuid4()
    summary = {
        "status": "PASS",
        "run_id": "task6-20260914-001",
        "streaming_progress": {"id": progress_id, "batchId": 0},
        "limitations": [],
    }

    text = verifier.render_markdown(summary)

    assert str(progress_id) in text


def test_json_safe_serializes_nested_spark_progress_ids():
    verifier = load_verifier()
    progress_id = uuid.uuid4()

    safe = verifier.json_safe({"progress": {"id": progress_id}})

    assert safe == {"progress": {"id": str(progress_id)}}
