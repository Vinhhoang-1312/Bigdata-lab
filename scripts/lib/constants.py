"""BigEarthNet constants shared across the pipeline."""

from __future__ import annotations

BANDS_10M = ("B02", "B03", "B04", "B08")
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

DEFAULT_N_PATCHES = 60_000
DEFAULT_N_COUNTRIES = 3
DEFAULT_N_CLASSES = 4  # in [3, 5]
DEBUG_N_PATCHES = 500
