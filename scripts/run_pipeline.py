#!/usr/bin/env python3
"""Run the full pipeline in order (local mode by default)."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> None:
    print("\n==>", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True, cwd=ROOT)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-root", default=None)
    ap.add_argument("--n-patches", type=int, default=60_000)
    ap.add_argument("--limit", type=int, default=None, help="Debug limit for pack/features")
    ap.add_argument("--n-classes", type=int, default=4)
    ap.add_argument("--skip-benchmark", action="store_true")
    args = ap.parse_args()

    py = sys.executable
    data = ["--data-root", args.data_root] if args.data_root else []

    run(
        [
            py,
            "scripts/00_build_subset.py",
            *data,
            "--n-patches",
            str(args.n_patches),
        ]
    )
    pack = [py, "scripts/01_ingest_and_pack.py", *data]
    if args.limit:
        pack += ["--limit", str(args.limit)]
    run(pack)

    feat = [py, "scripts/02_feature_engineering.py"]
    if args.skip_benchmark:
        feat.append("--skip-benchmark")
    run(feat)

    run([py, "scripts/03_spark_sql_analysis.py"])
    run([py, "scripts/04_classification.py", "--n-classes", str(args.n_classes)])
    print("\nPipeline complete. See reports/ and hdfs_mirror/bigearthnet/")


if __name__ == "__main__":
    main()
