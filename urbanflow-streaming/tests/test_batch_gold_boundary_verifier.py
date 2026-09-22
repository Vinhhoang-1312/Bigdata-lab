from __future__ import annotations

import importlib.util
from pathlib import Path


def load_verifier():
    path = Path(__file__).with_name("verify_batch_gold_boundary.py")
    spec = importlib.util.spec_from_file_location("batch_gold_boundary", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_markdown_records_blocked_missing_artifact():
    verifier = load_verifier()
    summary = {
        "status": "BLOCKED_MISSING_ARTIFACT",
        "path": "s3a://urbanflow/gold/historical_zone_baseline/",
        "source_commit": "749ea2e",
        "reason": "No Parquet files were found at the contract path.",
        "row_count": None,
        "schema": [],
        "unique_key_count": None,
        "duplicate_key_count": None,
        "sample_join": None,
    }

    text = verifier.render_markdown(summary)

    assert "Status: `BLOCKED_MISSING_ARTIFACT`" in text
    assert "s3a://urbanflow/gold/historical_zone_baseline/" in text
    assert "No Parquet files were found" in text


def test_exit_code_matches_verification_status():
    verifier = load_verifier()

    assert verifier.exit_code({"status": "PASS"}) == 0
    assert verifier.exit_code({"status": "BLOCKED_MISSING_ARTIFACT"}) == 2
    assert verifier.exit_code({"status": "FAIL"}) == 1
