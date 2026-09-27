"""Central configuration for BDA501 Individual Assignment 3."""

from __future__ import annotations

from pathlib import Path

from scripts.lib.constants import CLASSES_19, FEATURE_V2_COLS, RANDOM_SEED

ASSIGNMENT3_SEED = RANDOM_SEED
ASSIGNMENT3_FEATURE_COLS = list(FEATURE_V2_COLS)
ASSIGNMENT3_LABELS_19 = list(CLASSES_19)
ASSIGNMENT3_N_CLASSES = 35
ASSIGNMENT3_TOP_K = [5, 10]
ASSIGNMENT3_THRESHOLD_GRID = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]
ASSIGNMENT3_PCA_K = 2
ASSIGNMENT3_KMEANS_KS = list(range(3, 11))
ASSIGNMENT3_SCALE_FRACTIONS = [0.25, 0.50, 1.00]
ASSIGNMENT3_PARTITION_COUNTS = [4, 8]
ASSIGNMENT3_HELD_OUT_COUNTRY = "Lithuania"
ASSIGNMENT3_DEFAULT_MANIFEST = Path(r"D:\BigEarthNet-partial\manifest\manifest.parquet")

# Small but real grids. They satisfy ParamGridBuilder + validation tuning without using random folds.
ASSIGNMENT3_LR_GRID = {
    "regParam": [0.0, 0.01],
    "elasticNetParam": [0.0, 0.5],
}
ASSIGNMENT3_RF_GRID = {
    "numTrees": [20, 40],
    "maxDepth": [5, 8],
}
ASSIGNMENT3_GBT_GRID = {
    "maxIter": [5, 10],
    "maxDepth": [3],
}

