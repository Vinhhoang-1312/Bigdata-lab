"""Parallel byte-range downloader for a single Hugging Face file."""
from __future__ import annotations

import argparse
import concurrent.futures
import os
from pathlib import Path

import requests
from huggingface_hub import get_hf_file_metadata, hf_hub_url


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--filename", required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--chunk-mb", type=int, default=64)
    args = ap.parse_args()
    url = get_hf_file_metadata(hf_hub_url(args.repo, args.filename, repo_type="dataset")).location
    total = int(get_hf_file_metadata(hf_hub_url(args.repo, args.filename, repo_type="dataset")).size)
    part_dir = args.out.with_suffix(args.out.suffix + ".parts")
    part_dir.mkdir(parents=True, exist_ok=True)
    chunk = args.chunk_mb * 1024 * 1024
    ranges = [(start, min(total - 1, start + chunk - 1)) for start in range(0, total, chunk)]

    def fetch(item):
        idx, (start, end) = item
        part = part_dir / f"{idx:05d}.part"
        expected = end - start + 1
        if part.exists() and part.stat().st_size == expected:
            return idx, expected
        tmp = part.with_suffix(".tmp")
        last = None
        for attempt in range(1, 7):
            try:
                if tmp.exists():
                    tmp.unlink()
                with requests.get(url, headers={"Range": f"bytes={start}-{end}"}, stream=True, timeout=180) as r:
                    r.raise_for_status()
                    with tmp.open("wb") as fh:
                        for block in r.iter_content(1024 * 1024):
                            if block:
                                fh.write(block)
                if tmp.stat().st_size != expected:
                    raise RuntimeError(f"range {start}-{end}: {tmp.stat().st_size} != {expected}")
                os.replace(tmp, part)
                break
            except Exception as exc:  # retry transient CDN disconnects
                last = exc
                if attempt == 6:
                    raise
        if not part.exists() or part.stat().st_size != expected:
            raise RuntimeError(f"range failed: {start}-{end}: {last}")
        return idx, expected

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        done = 0
        for idx, size in pool.map(fetch, enumerate(ranges)):
            done += size
            print(f"{done}/{total} bytes ({done / total:.1%})", flush=True)

    tmp_out = args.out.with_suffix(args.out.suffix + ".tmp")
    with tmp_out.open("wb") as out:
        for idx in range(len(ranges)):
            with (part_dir / f"{idx:05d}.part").open("rb") as src:
                while block := src.read(8 * 1024 * 1024):
                    out.write(block)
    if tmp_out.stat().st_size != total:
        raise RuntimeError(f"assembled {tmp_out.stat().st_size} != {total}")
    os.replace(tmp_out, args.out)
    print(f"completed {args.out} ({total} bytes)")


if __name__ == "__main__":
    main()
