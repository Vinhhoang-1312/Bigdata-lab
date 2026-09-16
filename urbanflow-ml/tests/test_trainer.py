from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import torch

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from forecasting.gtqn import GTQNConfig, GTQNLite
from forecasting.trainer import EarlyStopper, evaluate_epoch, load_checkpoint, save_checkpoint, train_epoch


class TrainerTest(unittest.TestCase):
    def test_train_and_evaluate_epoch_use_real_model_batches(self):
        torch.manual_seed(5)
        model = GTQNLite(
            GTQNConfig(
                num_zones=2,
                history_channels=2,
                calendar_channels=1,
                hidden_size=4,
                zone_embedding_size=2,
                temporal_dilations=(1,),
                dropout=0.0,
            )
        )
        dataset = torch.utils.data.TensorDataset(
            torch.randn(4, 2, 6, 2),
            torch.randn(4, 6, 1),
            torch.rand(4, 2),
            torch.arange(4),
        )
        loader = torch.utils.data.DataLoader(dataset, batch_size=2)
        optimizer = torch.optim.AdamW(model.parameters(), lr=0.01)
        before = model.output_head[-1].weight.detach().clone()

        loss, batches = train_epoch(
            model,
            loader,
            torch.eye(2),
            optimizer,
            gradient_clip_norm=1.0,
            device=torch.device("cpu"),
        )
        validation_loss, predictions, targets, indices = evaluate_epoch(
            model,
            loader,
            torch.eye(2),
            device=torch.device("cpu"),
        )

        self.assertTrue(np.isfinite(loss))
        self.assertTrue(np.isfinite(validation_loss))
        self.assertEqual(batches, 2)
        self.assertFalse(torch.equal(before, model.output_head[-1].weight.detach()))
        self.assertEqual(tuple(predictions.shape), (4, 2, 3))
        self.assertEqual(tuple(targets.shape), (4, 2))
        self.assertEqual(indices.tolist(), [0, 1, 2, 3])

    def test_early_stopper_restores_best_model_after_patience(self):
        model = torch.nn.Linear(1, 1, bias=False)
        stopper = EarlyStopper(patience=2)

        with torch.no_grad():
            model.weight.fill_(3.0)
        self.assertFalse(stopper.update(3.0, model))
        with torch.no_grad():
            model.weight.fill_(2.0)
        self.assertFalse(stopper.update(2.0, model))
        with torch.no_grad():
            model.weight.fill_(9.0)
        self.assertFalse(stopper.update(2.1, model))
        self.assertTrue(stopper.update(2.2, model))

        stopper.restore_best(model)

        self.assertAlmostEqual(float(model.weight.item()), 2.0)
        self.assertEqual(stopper.best_metric, 2.0)
        self.assertEqual(stopper.bad_epochs, 2)

    def test_checkpoint_reload_restores_prediction_optimizer_and_epoch(self):
        torch.manual_seed(11)
        model = torch.nn.Linear(2, 1)
        optimizer = torch.optim.AdamW(model.parameters(), lr=0.01)
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer)
        stopper = EarlyStopper(patience=3)
        sample = torch.tensor([[1.0, 2.0]])
        expected = model(sample).detach().clone()
        stopper.update(1.25, model)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.pt"
            save_checkpoint(
                path,
                model=model,
                optimizer=optimizer,
                scheduler=scheduler,
                stopper=stopper,
                epoch=7,
                config_hash="config-a",
                dataset_fingerprint="data-a",
            )
            with torch.no_grad():
                model.weight.add_(100.0)
            restored_epoch = load_checkpoint(
                path,
                model=model,
                optimizer=optimizer,
                scheduler=scheduler,
                stopper=stopper,
                expected_config_hash="config-a",
                expected_dataset_fingerprint="data-a",
            )

        torch.testing.assert_close(model(sample), expected, rtol=0.0, atol=1e-6)
        self.assertEqual(restored_epoch, 7)
        self.assertEqual(stopper.best_metric, 1.25)

    def test_checkpoint_rejects_different_dataset(self):
        model = torch.nn.Linear(1, 1)
        optimizer = torch.optim.AdamW(model.parameters())
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer)
        stopper = EarlyStopper(patience=1)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.pt"
            save_checkpoint(
                path,
                model=model,
                optimizer=optimizer,
                scheduler=scheduler,
                stopper=stopper,
                epoch=1,
                config_hash="config-a",
                dataset_fingerprint="data-a",
            )
            with self.assertRaisesRegex(ValueError, "dataset fingerprint"):
                load_checkpoint(
                    path,
                    model=model,
                    optimizer=optimizer,
                    scheduler=scheduler,
                    stopper=stopper,
                    expected_config_hash="config-a",
                    expected_dataset_fingerprint="data-b",
                )


if __name__ == "__main__":
    unittest.main()
