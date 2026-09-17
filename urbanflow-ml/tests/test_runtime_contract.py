from __future__ import annotations

import json
import unittest


class RuntimeContractTest(unittest.TestCase):
    def test_pytorch_and_torch_distributor_are_available(self):
        import boto3
        import numpy as np
        import pandas as pd
        import pyarrow
        import torch
        from pyspark.ml.torch.distributor import TorchDistributor

        layer = torch.nn.Sequential(
            torch.nn.Linear(3, 4),
            torch.nn.ReLU(),
            torch.nn.Linear(4, 1),
        )
        output = layer(torch.ones((2, 3), dtype=torch.float32))

        self.assertEqual(tuple(output.shape), (2, 1))
        self.assertTrue(torch.isfinite(output).all().item())
        self.assertTrue(callable(TorchDistributor))
        print(
            json.dumps(
                {
                    "numpy": np.__version__,
                    "pandas": pd.__version__,
                    "pyarrow": pyarrow.__version__,
                    "pytorch": torch.__version__,
                    "boto3": boto3.__version__,
                },
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    unittest.main()
