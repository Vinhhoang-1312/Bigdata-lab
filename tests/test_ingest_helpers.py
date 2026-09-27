from __future__ import annotations

import pandas as pd
import numpy as np

from scripts.ingest_helpers import metadata_records


def test_metadata_records_avoids_pandas_spark_conversion():
    frame = pd.DataFrame(
        {
            "patch_id": ["p1"],
            "country": ["Austria"],
            "split": ["train"],
            "labels": [["Forest"]],
        }
    )
    assert metadata_records(frame) == [
        {"patch_id": "p1", "country": "Austria", "split": "train", "labels": ["Forest"]}
    ]


def test_metadata_records_normalizes_numpy_label_arrays():
    frame = pd.DataFrame(
        {
            "patch_id": ["p2"],
            "country": ["Finland"],
            "split": ["validation"],
            "labels": [np.array(["Water", "Forest"])],
        }
    )
    assert metadata_records(frame)[0]["labels"] == ["Water", "Forest"]
