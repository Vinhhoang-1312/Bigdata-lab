from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from pypdf import PdfReader


ROOT = Path(__file__).resolve().parents[1]


def test_assignment3_report_is_submission_ready() -> None:
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "render_a3_report_pdf.py")],
        cwd=ROOT,
        check=True,
    )

    pdf_path = ROOT / "reports" / "a3_final_report.pdf"
    reader = PdfReader(str(pdf_path))
    text = "\n".join(page.extract_text() or "" for page in reader.pages)

    assert 5 <= len(reader.pages) <= 10
    for required in (
        "Tran Phan Vinh Hoang",
        "Abstract",
        "Model selection and validation",
        "Assignment 2 baselines",
        "Per-class multi-label performance",
        "KMeans model selection",
        "Domain shift",
        "Conclusion",
    ):
        assert required in text

    assert "Requirement checkpoint audit" not in text
    assert "C:/Users/DELL" not in text
    assert "C:\\Users\\DELL" not in text
