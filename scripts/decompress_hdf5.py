"""Decompress selected HDF5 gzip archives without deleting the originals."""
from __future__ import annotations

import argparse
import gzip
import shutil
from pathlib import Path


def decompress(src: Path) -> Path:
    dst = src.with_suffix("")
    if dst.exists() and dst.stat().st_size > 0:
        return dst
    tmp = dst.with_suffix(dst.suffix + ".partial")
    with gzip.open(src, "rb") as inp, tmp.open("wb") as out:
        shutil.copyfileobj(inp, out, length=16 * 1024 * 1024)
    tmp.replace(dst)
    return dst


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("root", type=Path, default=Path(r"D:\BigEarthNet-partial\hdf5"), nargs="?")
    args = ap.parse_args()
    for src in sorted(args.root.glob("bigearthnet_*.hdf5.gz")):
        if src.name.endswith("_p0.hdf5.gz") or src.name.endswith("_p1.hdf5.gz"):
            print(f"decompressing {src}", flush=True)
            dst = decompress(src)
            print(f"ready {dst} ({dst.stat().st_size} bytes)", flush=True)


if __name__ == "__main__":
    main()
