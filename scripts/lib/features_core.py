"""Pure NumPy feature kernel (shared by Spark executors + driver validation)."""

from __future__ import annotations

from typing import Any

import numpy as np

from .constants import BANDS_10M, EPS, FEATURE_COLS, PATCH_HW, REFLECTANCE_SCALE

try:
    import rasterio
    from rasterio.io import MemoryFile
except ImportError:  # pragma: no cover - optional until installed
    rasterio = None
    MemoryFile = None


def _as_reflectance(arr: np.ndarray) -> np.ndarray:
    return arr.astype(np.float32) / np.float32(REFLECTANCE_SCALE)


def compute_from_arrays(
    b02: np.ndarray,
    b03: np.ndarray,
    b04: np.ndarray,
    b08: np.ndarray,
) -> dict[str, float]:
    """Compute assignment features from float-capable arrays (any numeric dtype)."""
    blue = _as_reflectance(np.asarray(b02))
    green = _as_reflectance(np.asarray(b03))
    red = _as_reflectance(np.asarray(b04))
    nir = _as_reflectance(np.asarray(b08))

    bands = {"b02": blue, "b03": green, "b04": red, "b08": nir}
    out: dict[str, float] = {}
    means = []
    for key, arr in bands.items():
        mu = float(arr.mean())
        # population std (ddof=0) — document in report
        sigma = float(arr.std(ddof=0))
        out[f"mean_{key}"] = mu
        out[f"std_{key}"] = sigma
        means.append(mu)

    ndvi = (nir - red) / (nir + red + EPS)
    ndwi = (green - nir) / (green + nir + EPS)
    out["ndvi"] = float(ndvi.mean())
    out["ndwi"] = float(ndwi.mean())
    out["brightness"] = float(np.mean(means))
    return out


def decode_band_bytes(blob: bytes | bytearray | memoryview) -> np.ndarray:
    if MemoryFile is None:
        raise ImportError("rasterio is required to decode GeoTIFF bytes")
    with MemoryFile(bytes(blob)) as mem:
        with mem.open() as ds:
            data = ds.read(1)
    if data.shape != (PATCH_HW, PATCH_HW):
        # Still allow compute, but callers may flag; keep data
        pass
    return data


def compute_from_bytes(
    b02: bytes,
    b03: bytes,
    b04: bytes,
    b08: bytes,
) -> dict[str, Any]:
    """
    Decode four band blobs and compute features.
    On any failure returns error_flag=True and null-like feature values.
    """
    empty = {c: None for c in FEATURE_COLS}
    try:
        arrays = [decode_band_bytes(x) for x in (b02, b03, b04, b08)]
        for arr, band in zip(arrays, BANDS_10M):
            if arr.ndim != 2:
                raise ValueError(f"{band} not 2D")
        feats = compute_from_arrays(*arrays)
        feats["error_flag"] = False
        feats["error_message"] = None
        return feats
    except Exception as exc:  # noqa: BLE001 — must not crash Spark jobs
        empty["error_flag"] = True
        empty["error_message"] = f"{type(exc).__name__}: {exc}"
        return empty
