"""Integration contract for the one-command Windows launcher."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[2]
LAUNCHER = ROOT / "start-urbanflow.bat"


def test_launcher_targets_react_and_not_archived_streamlit():
    source = LAUNCHER.read_text(encoding="utf-8")

    assert "5173/streaming" in source
    assert "5173/ml" in source
    assert "urbanflow-streamlit" not in source
    assert "8501" not in source


@pytest.mark.skipif(
    os.getenv("URBANFLOW_RUN_LAUNCHER_TEST") != "1",
    reason="starts the local Docker services",
)
def test_launcher_starts_services_from_an_unrelated_working_directory(tmp_path):
    assert LAUNCHER.is_file(), "start-urbanflow.bat is missing from the repository root"
    environment = os.environ.copy()
    environment["URBANFLOW_SKIP_OPEN"] = "1"
    environment["URBANFLOW_NONINTERACTIVE"] = "1"

    result = subprocess.run(
        ["cmd.exe", "/d", "/c", str(LAUNCHER)],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "URBANFLOW READY" in result.stdout
    assert "http://127.0.0.1:5173/streaming" in result.stdout
    assert "http://127.0.0.1:5173/ml" in result.stdout
    assert "http://127.0.0.1:8080" in result.stdout
    assert "http://127.0.0.1:8501" not in result.stdout
