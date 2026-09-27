"""BigEarthNet constants shared across the pipeline."""

from __future__ import annotations

BANDS_10M = ("B02", "B03", "B04", "B08")
BANDS_20M = ("B05", "B06", "B07", "B8A", "B11", "B12")
BAND_TO_NAME = {
    "B02": "blue",
    "B03": "green",
    "B04": "red",
    "B08": "nir",
}

# Reflectance scale: uint16 DN = reflectance * 1e4
REFLECTANCE_SCALE = 1e4
EPS = 1e-6
PATCH_HW = 120  # 10 m bands
PATCH_HW_20M = 60

COUNTRIES_V2 = (
    "Austria",
    "Belgium",
    "Finland",
    "Ireland",
    "Kosovo",
    "Lithuania",
    "Luxembourg",
    "Portugal",
    "Serbia",
    "Switzerland",
)

CLASSES_19 = (
    "Urban fabric",
    "Industrial or commercial units",
    "Arable land",
    "Permanent crops",
    "Pastures",
    "Complex cultivation patterns",
    "Land principally occupied by agriculture, with significant areas of natural vegetation",
    "Agro-forestry areas",
    "Broad-leaved forest",
    "Coniferous forest",
    "Mixed forest",
    "Natural grassland and sparsely vegetated areas",
    "Moors, heathland and sclerophyllous vegetation",
    "Transitional woodland, shrub",
    "Beaches, dunes, sands",
    "Inland wetlands",
    "Coastal wetlands",
    "Inland waters",
    "Marine waters",
)

FEATURE_COLS = (
    "mean_b02",
    "mean_b03",
    "mean_b04",
    "mean_b08",
    "std_b02",
    "std_b03",
    "std_b04",
    "std_b08",
    "ndvi",
    "ndwi",
    "brightness",
)

FEATURE_V2_EXTRA_COLS = (
    "mean_b05",
    "mean_b06",
    "mean_b07",
    "mean_b8a",
    "mean_b11",
    "mean_b12",
    "std_b05",
    "std_b06",
    "std_b07",
    "std_b8a",
    "std_b11",
    "std_b12",
    "ndmi",
    "ndbi",
    "ndvi_p10",
    "ndvi_p50",
    "ndvi_p90",
)

FEATURE_V2_COLS = FEATURE_COLS + FEATURE_V2_EXTRA_COLS

DEFAULT_N_PATCHES = 60_000
DEFAULT_N_COUNTRIES = 3
DEFAULT_N_CLASSES = 35
DEBUG_N_PATCHES = 500
RANDOM_SEED = 501
