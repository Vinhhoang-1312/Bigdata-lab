#!/usr/bin/env python3
"""Step 0 — Build a graded-compliant subset from metadata.parquet (no images required)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

# Allow `python scripts/00_*.py` without installing a package
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.lib.constants import DEFAULT_N_COUNTRIES, DEFAULT_N_PATCHES
from scripts.lib.dataset_layout import discover_metadata_only, tile_id_from_patch_id
from scripts.lib.paths import subset_dir


def build_subset(
    metadata_path: Path,
    n_patches: int,
    n_countries: int,
    seed: int,
) -> pd.DataFrame:
    df = pd.read_parquet(metadata_path)
    required = {"patch_id", "labels", "split", "country"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"metadata missing columns: {missing}")

    country_counts = df["country"].value_counts()
    top_countries = list(country_counts.head(n_countries).index)
    scoped = df[df["country"].isin(top_countries)].copy()

    if len(scoped) <= n_patches:
        subset = scoped
    else:
        # Stratify by country × split to preserve official split proportions
        scoped["_strata"] = scoped["country"].astype(str) + "|" + scoped["split"].astype(str)
        frac = n_patches / len(scoped)
        parts = []
        for _, g in scoped.groupby("_strata", sort=False):
            k = max(1, int(round(len(g) * frac)))
            k = min(k, len(g))
            parts.append(g.sample(n=k, random_state=seed))
        subset = pd.concat(parts, ignore_index=True)
        if len(subset) > n_patches:
            subset = subset.sample(n=n_patches, random_state=seed)
        elif len(subset) < n_patches:
            need = n_patches - len(subset)
            remain = scoped.drop(index=subset.index, errors="ignore")
            if len(remain) >= need:
                subset = pd.concat(
                    [subset, remain.sample(n=need, random_state=seed)],
                    ignore_index=True,
                )

    subset = subset.drop(columns=["_strata"], errors="ignore").reset_index(drop=True)
    subset["tile_id"] = subset["patch_id"].map(tile_id_from_patch_id)
    return subset


def write_outputs(subset: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    meta_out = out_dir / "subset_metadata.parquet"
    subset.to_parquet(meta_out, index=False)

    (out_dir / "patch_ids.txt").write_text(
        "\n".join(subset["patch_id"].astype(str).tolist()) + "\n", encoding="utf-8"
    )
    tiles = sorted(subset["tile_id"].unique().tolist())
    (out_dir / "tiles.txt").write_text("\n".join(tiles) + "\n", encoding="utf-8")

    lines = [
        "# Subset counts",
        "",
        f"- total_patches: **{len(subset)}**",
        f"- n_countries: **{subset['country'].nunique()}**",
        f"- n_tiles: **{len(tiles)}**",
        "",
        "## Per country",
        "",
    ]
    for c, n in subset["country"].value_counts().items():
        lines.append(f"- {c}: {n}")
    lines += ["", "## Per split", ""]
    for s, n in subset["split"].value_counts().items():
        lines.append(f"- {s}: {n}")
    lines += ["", "## Per country × split", ""]
    ct = subset.groupby(["country", "split"]).size().reset_index(name="n")
    for _, row in ct.iterrows():
        lines.append(f"- {row['country']} / {row['split']}: {row['n']}")
    (out_dir / "COUNTS.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    print(f"\nWrote {meta_out}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-root", default=None, help="Folder containing metadata.parquet")
    p.add_argument("--metadata", default=None, help="Explicit path to metadata.parquet")
    p.add_argument("--n-patches", type=int, default=DEFAULT_N_PATCHES)
    p.add_argument("--n-countries", type=int, default=DEFAULT_N_COUNTRIES)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out-dir", default=None)
    args = p.parse_args()

    meta = Path(args.metadata) if args.metadata else discover_metadata_only(args.data_root)
    out = Path(args.out_dir) if args.out_dir else subset_dir()
    print(f"Using metadata: {meta}")
    subset = build_subset(meta, args.n_patches, args.n_countries, args.seed)
    write_outputs(subset, out)

    if len(subset) < 50_000:
        print(
            f"WARNING: subset has {len(subset)} patches (<50000). "
            "For graded run, increase data coverage or lower filters.",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
