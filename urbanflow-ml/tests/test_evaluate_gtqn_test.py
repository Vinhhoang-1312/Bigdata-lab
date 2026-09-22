"""Tests for the PASS-only GTQN test-evaluation evidence contract."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


def _evaluation_module():
    path = Path(__file__).resolve().parents[1] / "spark" / "evaluate_gtqn_test.py"
    spec = importlib.util.spec_from_file_location("evaluate_gtqn_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class EvaluateGtqnTest(unittest.TestCase):
    def test_build_evidence_rejects_non_pass_metrics(self):
        """A failed test evaluation must never become UI-ready evidence."""
        module = _evaluation_module()

        with self.assertRaisesRegex(ValueError, "PASS"):
            module.build_evidence({"status": "FAIL"}, run_id="r1", variant="full")
