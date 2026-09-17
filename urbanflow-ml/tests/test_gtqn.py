from __future__ import annotations

import sys
import unittest
from pathlib import Path

import torch

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from forecasting.gtqn import GTQNConfig, GTQNLite, pinball_loss


class GTQNLiteTest(unittest.TestCase):
    def build_model(self) -> GTQNLite:
        return GTQNLite(
            GTQNConfig(
                num_zones=4,
                history_channels=2,
                calendar_channels=3,
                hidden_size=8,
                zone_embedding_size=4,
                temporal_dilations=(1, 2),
                dropout=0.0,
            )
        )

    def test_forward_shape_and_parameter_budget(self):
        model = self.build_model()
        history = torch.randn(2, 4, 16, 2)
        calendar = torch.randn(2, 16, 3)
        adjacency = torch.eye(4)

        predictions = model(history, calendar, adjacency)

        self.assertEqual(tuple(predictions.shape), (2, 4, 3))
        self.assertLess(sum(parameter.numel() for parameter in model.parameters()), 500_000)

    def test_quantiles_are_ordered_by_construction(self):
        model = self.build_model()

        predictions = model(torch.randn(3, 4, 16, 2), torch.randn(3, 16, 3), torch.eye(4))

        self.assertTrue(torch.all(predictions[..., 0] <= predictions[..., 1]).item())
        self.assertTrue(torch.all(predictions[..., 1] <= predictions[..., 2]).item())

    def test_backward_reaches_adaptive_graph_and_fusion_gate(self):
        model = self.build_model()
        predictions = model(torch.randn(2, 4, 16, 2), torch.randn(2, 16, 3), torch.eye(4))
        target = torch.rand(2, 4)

        loss = pinball_loss(predictions, target)
        loss.backward()

        for name in ("source_embedding", "target_embedding", "graph_gate.weight"):
            parameter = dict(model.named_parameters())[name]
            self.assertIsNotNone(parameter.grad, name)
            self.assertTrue(torch.isfinite(parameter.grad).all().item(), name)

    def test_temporal_zone_chunks_preserve_predictions_and_gradients(self):
        baseline = self.build_model()
        chunked = GTQNLite(
            GTQNConfig(
                num_zones=4,
                history_channels=2,
                calendar_channels=3,
                hidden_size=8,
                zone_embedding_size=4,
                temporal_dilations=(1, 2),
                temporal_zone_chunk_size=2,
                dropout=0.0,
            )
        )
        chunked.load_state_dict(baseline.state_dict())
        history = torch.randn(2, 4, 16, 2)
        calendar = torch.randn(2, 16, 3)
        adjacency = torch.eye(4)

        torch.testing.assert_close(
            chunked(history, calendar, adjacency),
            baseline(history, calendar, adjacency),
            rtol=0.0,
            atol=1e-6,
        )
        pinball_loss(chunked(history, calendar, adjacency), torch.rand(2, 4)).backward()
        self.assertIsNotNone(chunked.input_projection.weight.grad)

    def test_pinball_loss_matches_hand_calculated_under_and_over_prediction(self):
        quantiles = (0.1, 0.5, 0.9)
        under = pinball_loss(torch.zeros(1, 1, 3), torch.ones(1, 1), quantiles=quantiles)
        over = pinball_loss(torch.ones(1, 1, 3), torch.zeros(1, 1), quantiles=quantiles)

        self.assertAlmostEqual(float(under), 0.5, places=6)
        self.assertAlmostEqual(float(over), 0.5, places=6)

    def test_default_model_handles_real_zone_and_history_shape(self):
        model = GTQNLite(GTQNConfig(num_zones=265, dropout=0.0)).eval()

        with torch.no_grad():
            predictions = model(
                torch.zeros(1, 265, 336, 2),
                torch.zeros(1, 336, 7),
                torch.eye(265),
            )

        self.assertEqual(tuple(predictions.shape), (1, 265, 3))
        self.assertTrue(torch.isfinite(predictions).all().item())


if __name__ == "__main__":
    unittest.main()
