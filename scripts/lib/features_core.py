"""Pure NumPy feature kernel (shared by Spark executors + driver validation)."""

from __future__ import annotations

from typing import Any

import numpy as np

from .constants import BANDS_10M, EPS, FEATURE_COLS, FEATURE_V2_COLS, PATCH_HW, REFLECTANCE_SCALE

try:
    import rasterio
    from rasterio.io import MemoryFile
except ImportError:  # pragma: no cover - optional until installed
    rasterio = None
    MemoryFile = None


def _as_reflectance(arr: np.ndarray) -> np.ndarray:
    return arr.astype(np.float32) / np.float32(REFLECTANCE_SCALE)


def _as_reflectance_auto(arr: np.ndarray) -> np.ndarray:
    arr = np.asarray(arr, dtype=np.float32)
    if arr.size and float(np.nanmax(arr)) > 2.0:
        return arr / np.float32(REFLECTANCE_SCALE)
    return arr


def _native_20m(arr: np.ndarray) -> np.ndarray:
    """Return a 60x60 view for 20 m bands, without stacking with 10 m bands."""
    arr = _as_reflectance_auto(arr)
    if arr.shape == (60, 60):
        return arr
    if arr.shape == (120, 120):
        return arr.reshape(60, 2, 60, 2).mean(axis=(1, 3))
    return arr


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


def compute_richer_from_hdf5_image(image: np.ndarray) -> dict[str, float]:
    """Compute Assignment 3 richer feature vector from one 12-band HDF5 image."""
    image = np.asarray(image)
    if image.ndim != 3 or image.shape[0] < 12:
        raise ValueError(f"Expected image shape (12,H,W), got {image.shape}")

    # TorchGeo/BigEarthNet order: B01,B02,B03,B04,B05,B06,B07,B08,B8A,B09,B11,B12.
    b02, b03, b04, b08 = (_as_reflectance_auto(image[i]) for i in (1, 2, 3, 7))
    base = compute_from_arrays(*(b * REFLECTANCE_SCALE for b in (b02, b03, b04, b08)))

    out: dict[str, float] = dict(base)
    twenty = {
        "b05": _native_20m(image[4]),
        "b06": _native_20m(image[5]),
        "b07": _native_20m(image[6]),
        "b8a": _native_20m(image[8]),
        "b11": _native_20m(image[10]),
        "b12": _native_20m(image[11]),
    }
    for name, arr in twenty.items():
        out[f"mean_{name}"] = float(arr.mean())
        out[f"std_{name}"] = float(arr.std(ddof=0))

    b8a = twenty["b8a"]
    b11 = twenty["b11"]
    ndmi = (b8a - b11) / (b8a + b11 + EPS)
    ndbi = (b11 - b8a) / (b11 + b8a + EPS)
    out["ndmi"] = float(ndmi.mean())
    out["ndbi"] = float(ndbi.mean())

    ndvi_pixels = (b08 - b04) / (b08 + b04 + EPS)
    out["ndvi_p10"] = float(np.percentile(ndvi_pixels, 10))
    out["ndvi_p50"] = float(np.percentile(ndvi_pixels, 50))
    out["ndvi_p90"] = float(np.percentile(ndvi_pixels, 90))
    return {c: out[c] for c in FEATURE_V2_COLS}


def decode_band_bytes(blob: bytes | bytearray | memoryview) -> np.ndarray:
    if MemoryFile is None:
        raise ImportError("rasterio is required to decode GeoTIFF bytes")
    with MemoryFile(bytes(blob)) as mem:
        with mem.open() as ds:
            data = ds.read(1)
    if data.shape != (PATCH_HW, PATCH_HW):
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
    except Exception as exc:  # noqa: BLE001 - must not crash Spark jobs
        empty["error_flag"] = True
        empty["error_message"] = f"{type(exc).__name__}: {exc}"
        return empty
