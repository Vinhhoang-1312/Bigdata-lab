from __future__ import annotations

import importlib.util
from pathlib import Path

from pyspark.sql import SparkSession


_SPEC = importlib.util.spec_from_file_location(
    "classification", Path(__file__).parents[1] / "scripts" / "04_classification.py"
)
_MODULE = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(_MODULE)
nearest_centroid = _MODULE.nearest_centroid


def test_nearest_centroid_keeps_prediction_label_unambiguous():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-nearest-centroid")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        schema = "patch_id string, target string, z_f1 double, split string"
        all_z = spark.createDataFrame(
            [("p1", "A", 0.1, "test"), ("p2", "B", 1.1, "test")], schema
        )
        train_z = spark.createDataFrame(
            [("p3", "A", 0.0, "train"), ("p4", "B", 1.0, "train")], schema
        )
        scored, _ = nearest_centroid(all_z, train_z, ["f1"])
        assert "pred_centroid" in scored.columns
        assert scored.select("pred_centroid").count() == 2
    finally:
        spark.stop()
