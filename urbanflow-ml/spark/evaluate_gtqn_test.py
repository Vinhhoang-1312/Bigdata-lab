#!/usr/bin/env python3
"""Evaluate a completed GTQN variant on the chronological test split."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import sys
import tempfile
from pathlib import Path

import torch
import yaml
from torch.utils.data import DataLoader

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from forecasting.dataset import ZoneDemandDataset
from forecasting.distributed import S3CheckpointStore, _model_config, _training_config_hash, download_s3a_uri
from forecasting.gtqn import GTQNLite
from forecasting.probabilistic_evaluation import probabilistic_metrics, regression_metrics
from forecasting.scaling import RobustZoneScaler
from forecasting.tensor_data import load_tensor_artifact
from forecasting.trainer import evaluate_epoch

DEFAULT_CONFIG = "/workspace/urbanflow-ml/config/settings.example.yaml"


def build_evidence(
    metrics: dict,
    *,
    run_id: str,
    variant: str,
    artifact_uri: str | None = None,
    dataset_fingerprint: str | None = None,
    config_hash: str | None = None,
) -> dict:
    """Build UI-readable evidence only from a successful held-out evaluation."""
    if metrics.get("status") != "PASS":
        raise ValueError("evaluation metrics must have PASS status")
    return {
        "status": "PASS",
        "run_id": run_id,
        "variant": variant,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "artifact": {
            "uri": artifact_uri,
            "dataset_fingerprint": dataset_fingerprint,
            "config_hash": config_hash,
        },
        "metrics": metrics,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--variant", choices=("no_graph", "fixed_graph", "full"), required=True)
    return parser.parse_args()


def load_best_model(config: dict, arrays: dict, metadata: dict, store: S3CheckpointStore, variant: str) -> tuple[GTQNLite, int]:
    model = GTQNLite(
        _model_config(
            config,
            num_zones=int(arrays["counts"].shape[1]),
            calendar_channels=int(arrays["calendar"].shape[1]),
            variant=variant,
        )
    )
    with tempfile.TemporaryDirectory(prefix="urbanflow-gtqn-evaluation-") as directory:
        checkpoint_path = Path(directory) / "best.pt"
        store.client.download_file(store.bucket, f"{store.prefix}/best.pt", str(checkpoint_path))
        payload = torch.load(checkpoint_path, map_location="cpu", weights_only=False)
    if payload["config_hash"] != _training_config_hash(config, variant):
        raise ValueError("best checkpoint config hash does not match requested evaluation")
    if payload["dataset_fingerprint"] != metadata["dataset_fingerprint"]:
        raise ValueError("best checkpoint dataset fingerprint does not match requested evaluation")
    model.load_state_dict(payload["model"])
    return model, int(payload["epoch"])


def main() -> int:
    args = parse_args()
    with open(args.config, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    run_root = config["paths"]["work"].rstrip("/") + f"/runs/{args.run_id}"
    artifact_uri = f"{run_root}/tensor/artifact.npz"
    prefix = f"dev/ml/runs/{args.run_id}/training/{args.variant}"
    store = S3CheckpointStore(bucket="urbanflow", prefix=prefix)

    with tempfile.TemporaryDirectory(prefix="urbanflow-artifact-evaluation-") as directory:
        artifact_path = Path(directory) / "artifact.npz"
        download_s3a_uri(artifact_uri, artifact_path)
        arrays, metadata = load_tensor_artifact(artifact_path)

    model, checkpoint_epoch = load_best_model(config, arrays, metadata, store, args.variant)
    dataset = ZoneDemandDataset(
        arrays,
        metadata,
        "test",
        lookback=int(config["tensor"]["lookback_buckets"]),
        target_stride=1,
    )
    loader = DataLoader(dataset, batch_size=int(config["training"]["evaluation_batch_size_per_rank"]), shuffle=False)
    adjacency = torch.from_numpy(arrays["fixed_adjacency"]).float()
    scaled_loss, scaled_predictions, _, indices = evaluate_epoch(model, loader, adjacency, device=torch.device("cpu"))

    target_indices = indices.numpy()
    scaler = RobustZoneScaler(center=arrays["scaler_center"], scale=arrays["scaler_scale"])
    raw_predictions = scaler.inverse_quantiles(scaled_predictions.numpy())
    raw_actual = arrays["counts"][target_indices]
    observed = arrays["observed"][target_indices]
    actual_values = raw_actual[observed]
    prediction_values = raw_predictions[observed]
    if actual_values.size == 0:
        raise ValueError("test split contains no observed targets")

    metrics = {
        "status": "PASS",
        "run_id": args.run_id,
        "variant": args.variant,
        "checkpoint_epoch": checkpoint_epoch,
        "test_bucket_count": int(len(target_indices)),
        "observed_target_count": int(actual_values.size),
        "scaled_quantile_loss": float(scaled_loss),
        **regression_metrics(actual_values, prediction_values),
        **probabilistic_metrics(actual_values, prediction_values),
    }
    evidence = build_evidence(
        metrics,
        run_id=args.run_id,
        variant=args.variant,
        artifact_uri=artifact_uri,
        dataset_fingerprint=str(metadata["dataset_fingerprint"]),
        config_hash=_training_config_hash(config, args.variant),
    )
    store.put_json(f"{prefix}/test_metrics.json", evidence)
    print(json.dumps(metrics, sort_keys=True))
    print("ML TEST EVALUATION = PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
