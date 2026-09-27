"""Assemble a concise assignment report from generated JSON evidence."""
from __future__ import annotations

import json
from pathlib import Path


def read(path: str):
    p = Path(path)
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def main() -> None:
    manifest = read("reports/hdf5_manifest_summary.json") or {}
    step2 = read("reports/hdf5_step2_validation.json") or {}
    sql = read("reports/step3_summary.json") or {}
    clf = read("reports/step4_metrics.json") or {}
    v = step2.get("validation", {})
    lines = [
        "# BDA501 Individual Assignment 2 — BigEarthNet partial HDF5 run",
        "",
        "## Dataset and scope",
        "",
        "Only four HDF5 shards were used on `D:\\BigEarthNet-partial`; the full repository was not downloaded.",
        f"The manifest contains **{manifest.get('rows', 'n/a')}** patches from {', '.join(manifest.get('countries', []))}.",
        f"Official split counts: `{manifest.get('splits', {})}`.",
        "",
        "## Distributed feature engineering",
        "",
        "`hdf5_features.py` uses Spark `mapPartitions`; each executor opens a shard once, reads the indexed `images` row, selects B02/B03/B04/B08, and emits an explicit schema.",
        f"Input/output/errors: **{v.get('n_in', 'n/a')} / {v.get('n_out', 'n/a')} / {v.get('n_error', 'n/a')}**; countries={v.get('n_countries', 'n/a')}, splits={v.get('n_splits', 'n/a')}; elapsed={v.get('elapsed_sec', 'n/a')} s.",
        f"NumPy checks: `{v.get('numpy_checks', [])}`.",
        "",
        "## Spark SQL and classification",
        "",
        f"SQL summary: `{sql}`.",
        f"Classification metrics: `{clf}`.",
        "",
        "## Limitation",
        "",
        "The main run reads HDF5 directly and does not materialise tens of thousands of original TIFF files. A small TIFF packing pilot is retained only to demonstrate the small-files issue.",
    ]
    Path("reports/final_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("wrote reports/final_report.md")


if __name__ == "__main__":
    main()
