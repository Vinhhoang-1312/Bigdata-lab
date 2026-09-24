"""Training, early stopping, and restart-safe checkpoints for GTQN-Lite."""

from __future__ import annotations

import random
from contextlib import nullcontext
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import torch

from forecasting.gtqn import pinball_loss


@dataclass
class TrainingResult:
    best_epoch: int
    last_epoch: int
    best_validation_loss: float
    stopped_early: bool
    history: list[dict]

    def as_dict(self) -> dict:
        return asdict(self)


class EarlyStopper:
    def __init__(self, patience: int, min_delta: float = 0.0) -> None:
        if patience <= 0:
            raise ValueError("patience must be positive")
        self.patience = int(patience)
        self.min_delta = float(min_delta)
        self.best_metric = float("inf")
        self.best_step: int | None = None
        self.bad_epochs = 0
        self.best_state: dict[str, torch.Tensor] | None = None

    def update(self, metric: float, model: torch.nn.Module, *, step: int | None = None) -> bool:
        if not np.isfinite(metric):
            raise ValueError("early-stopping metric must be finite")
        if metric < self.best_metric - self.min_delta:
            self.best_metric = float(metric)
            self.best_step = None if step is None else int(step)
            self.bad_epochs = 0
            self.best_state = {
                name: value.detach().cpu().clone()
                for name, value in model.state_dict().items()
            }
        else:
            self.bad_epochs += 1
        return self.bad_epochs >= self.patience

    def restore_best(self, model: torch.nn.Module) -> None:
        if self.best_state is None:
            raise RuntimeError("no best model state has been recorded")
        model.load_state_dict(self.best_state)

    def state_dict(self) -> dict:
        return {
            "patience": self.patience,
            "min_delta": self.min_delta,
            "best_metric": self.best_metric,
            "best_step": self.best_step,
            "bad_epochs": self.bad_epochs,
            "best_state": self.best_state,
        }

    def load_state_dict(self, state: dict) -> None:
        if int(state["patience"]) != self.patience:
            raise ValueError("checkpoint early-stopping patience differs from configuration")
        self.min_delta = float(state["min_delta"])
        self.best_metric = float(state["best_metric"])
        self.best_step = state.get("best_step")
        self.bad_epochs = int(state["bad_epochs"])
        self.best_state = state["best_state"]


def save_checkpoint(
    path: str | Path,
    *,
    model: torch.nn.Module,
    optimizer: torch.optim.Optimizer,
    scheduler,
    stopper: EarlyStopper,
    epoch: int,
    config_hash: str,
    dataset_fingerprint: str,
) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    payload = {
        "model": model.state_dict(),
        "optimizer": optimizer.state_dict(),
        "scheduler": scheduler.state_dict(),
        "stopper": stopper.state_dict(),
        "epoch": int(epoch),
        "config_hash": config_hash,
        "dataset_fingerprint": dataset_fingerprint,
        "python_rng_state": random.getstate(),
        "numpy_rng_state": np.random.get_state(),
        "torch_rng_state": torch.get_rng_state(),
    }
    torch.save(payload, temporary)
    temporary.replace(target)


def load_checkpoint(
    path: str | Path,
    *,
    model: torch.nn.Module,
    optimizer: torch.optim.Optimizer,
    scheduler,
    stopper: EarlyStopper,
    expected_config_hash: str,
    expected_dataset_fingerprint: str,
) -> int:
    payload = torch.load(Path(path), map_location="cpu", weights_only=False)
    if payload["config_hash"] != expected_config_hash:
        raise ValueError("checkpoint config hash does not match requested run")
    if payload["dataset_fingerprint"] != expected_dataset_fingerprint:
        raise ValueError("checkpoint dataset fingerprint does not match requested run")
    model.load_state_dict(payload["model"])
    optimizer.load_state_dict(payload["optimizer"])
    scheduler.load_state_dict(payload["scheduler"])
    stopper.load_state_dict(payload["stopper"])
    random.setstate(payload["python_rng_state"])
    np.random.set_state(payload["numpy_rng_state"])
    torch.set_rng_state(payload["torch_rng_state"])
    return int(payload["epoch"])


def train_epoch(
    model: torch.nn.Module,
    loader: Iterable,
    fixed_adjacency: torch.Tensor,
    optimizer: torch.optim.Optimizer,
    *,
    gradient_clip_norm: float,
    gradient_accumulation_steps: int = 1,
    device: torch.device,
) -> tuple[float, int]:
    if gradient_accumulation_steps <= 0:
        raise ValueError("gradient_accumulation_steps must be positive")
    loader_batches = len(loader)  # type: ignore[arg-type]
    model.train()
    total_loss = 0.0
    batches = 0
    optimizer.zero_grad(set_to_none=True)
    for batch_index, (history, calendar, target, _) in enumerate(loader, start=1):
        history = history.to(device)
        calendar = calendar.to(device)
        target = target.to(device)
        should_step = batch_index % gradient_accumulation_steps == 0 or batch_index == loader_batches
        group_start = ((batch_index - 1) // gradient_accumulation_steps) * gradient_accumulation_steps
        group_size = min(gradient_accumulation_steps, loader_batches - group_start)
        sync_context = nullcontext()
        if not should_step and hasattr(model, "no_sync"):
            sync_context = model.no_sync()
        with sync_context:
            predictions = model(history, calendar, fixed_adjacency)
            loss = pinball_loss(predictions, target)
            if not torch.isfinite(loss):
                raise FloatingPointError("non-finite training loss")
            (loss / group_size).backward()
        if should_step:
            torch.nn.utils.clip_grad_norm_(model.parameters(), gradient_clip_norm)
            optimizer.step()
            optimizer.zero_grad(set_to_none=True)
        total_loss += float(loss.detach())
        batches += 1
    if batches == 0:
        raise ValueError("training loader produced no batches")
    return total_loss / batches, batches


@torch.no_grad()
def evaluate_epoch(
    model: torch.nn.Module,
    loader: Iterable,
    fixed_adjacency: torch.Tensor,
    *,
    device: torch.device,
) -> tuple[float, torch.Tensor, torch.Tensor, torch.Tensor]:
    model.eval()
    losses: list[float] = []
    predictions: list[torch.Tensor] = []
    targets: list[torch.Tensor] = []
    indices: list[torch.Tensor] = []
    for history, calendar, target, target_index in loader:
        output = model(history.to(device), calendar.to(device), fixed_adjacency)
        loss = pinball_loss(output, target.to(device))
        losses.append(float(loss))
        predictions.append(output.cpu())
        targets.append(target.cpu())
        indices.append(target_index.cpu())
    if not losses:
        raise ValueError("evaluation loader produced no batches")
    return (
        sum(losses) / len(losses),
        torch.cat(predictions),
        torch.cat(targets),
        torch.cat(indices),
    )
