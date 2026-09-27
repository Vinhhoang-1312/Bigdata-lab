"""Discover BigEarthNet drop-in folders under Lab2."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .constants import BANDS_10M
from .paths import data_incoming, repo_root


@dataclass(frozen=True)
class DatasetPaths:
    root: Path
    metadata: Path
    images_root: Path  # directory that contains <TILE>/<PATCH>/...

    def describe(self) -> str:
        return (
            f"root={self.root}\n"
            f"metadata={self.metadata}\n"
            f"images_root={self.images_root}"
        )


def tile_id_from_patch_id(patch_id: str) -> str:
    parts = patch_id.split("_")
    if len(parts) < 3:
        raise ValueError(f"Unexpected patch_id: {patch_id}")
    return "_".join(parts[:-2])


def band_filename(patch_id: str, band: str) -> str:
    return f"{patch_id}_{band}.tif"


def patch_band_path(images_root: Path, patch_id: str, band: str) -> Path:
    tile = tile_id_from_patch_id(patch_id)
    return images_root / tile / patch_id / band_filename(patch_id, band)


def _looks_like_images_root(path: Path) -> bool:
    if not path.is_dir():
        return False
    # Heuristic: has at least one subdir that looks like a tile (contains patch dirs)
    for child in path.iterdir():
        if child.is_dir() and not child.name.startswith("."):
            return True
    return False


def _find_metadata(candidates: list[Path]) -> Path | None:
    for c in candidates:
        if c.is_file() and c.name == "metadata.parquet":
            return c
        nested = c / "metadata.parquet" if c.is_dir() else None
        if nested and nested.is_file():
            return nested
    return None


def _find_images_root(root: Path) -> Path | None:
    ordered = [
        root / "BigEarthNet-S2",
        root / "bigearthnet-s2",
        root / "raw",
        root / "S2",
        root,
    ]
    for p in ordered:
        if _looks_like_images_root(p):
            # Prefer a folder that has TILE/PATCH nesting with a B02 sample
            if p.name in {"BigEarthNet-S2", "bigearthnet-s2", "raw", "S2"}:
                return p
    # Fallback: first candidate that looks like images
    for p in ordered:
        if _looks_like_images_root(p):
            return p
    return None


def discover_dataset(explicit_root: str | Path | None = None) -> DatasetPaths:
    """
    Resolve drop-in dataset location.

    Search order:
      1) --data-root / BEN_DATA_ROOT
      2) <repo>/data/incoming
      3) <repo>/data
    """
    roots: list[Path] = []
    if explicit_root:
        roots.append(Path(explicit_root))
    env = os.environ.get("BEN_DATA_ROOT")
    if env:
        roots.append(Path(env))
    roots.extend([data_incoming(), repo_root() / "data"])

    tried: list[str] = []
    for root in roots:
        root = root.resolve()
        tried.append(str(root))
        if not root.exists():
            continue
        meta = _find_metadata([root])
        if meta is None:
            continue
        images = _find_images_root(root)
        if images is None:
            # metadata-only is OK for step 00
            images = root / "BigEarthNet-S2"
        return DatasetPaths(root=root, metadata=meta, images_root=images)

    raise FileNotFoundError(
        "Could not find metadata.parquet. Drop dataset under data/incoming/ "
        f"or set BEN_DATA_ROOT. Tried: {tried}"
    )


def discover_metadata_only(explicit_root: str | Path | None = None) -> Path:
    """Like discover_dataset but only requires metadata.parquet."""
    roots: list[Path] = []
    if explicit_root:
        roots.append(Path(explicit_root))
    env = os.environ.get("BEN_DATA_ROOT")
    if env:
        roots.append(Path(env))
    roots.extend([data_incoming(), repo_root() / "data"])

    for root in roots:
        root = root.resolve()
        if not root.exists():
            continue
        meta = _find_metadata([root])
        if meta is not None:
            return meta
    raise FileNotFoundError(
        "metadata.parquet not found under data/incoming/ (or BEN_DATA_ROOT)."
    )


def required_bands_exist(images_root: Path, patch_id: str) -> bool:
    return all(patch_band_path(images_root, patch_id, b).is_file() for b in BANDS_10M)
