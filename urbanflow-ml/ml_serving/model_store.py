"""Load an immutable GTQN run and serve past-only, half-hour forecasts."""
from __future__ import annotations

import csv
from datetime import datetime
from functools import lru_cache
import io
import json
import os
from pathlib import Path
import tempfile
from zoneinfo import ZoneInfo

import numpy as np
import torch
import yaml

from forecasting.dataset import ZoneDemandDataset
from forecasting.distributed import (
    S3CheckpointStore, _model_config, _training_config_hash, download_s3a_uri,
)
from forecasting.gtqn import GTQNLite
from forecasting.scaling import RobustZoneScaler
from forecasting.tensor_data import load_tensor_artifact
from forecasting.weekly_naive_audit import weekly_naive_report
from ml_serving.training_information import build_training_information

NEW_YORK = ZoneInfo("America/New_York")
DEFAULT_RUN_ID = "gtqn-20260917-001"
DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / "config/settings.example.yaml"


def retrospective_reference(arrays: dict, target_index: int, zone_index: int) -> dict:
    observed = arrays["observed"]
    counts = arrays["counts"]
    prior_index = target_index - 336
    return {
        "observed_actual": float(counts[target_index, zone_index])
        if observed[target_index, zone_index] else None,
        "weekly_naive": float(counts[prior_index, zone_index])
        if prior_index >= 0 and observed[prior_index, zone_index] else None,
    }


def resolve_target_index(timestamps: np.ndarray, forecast_at: str) -> int:
    try:
        requested = datetime.fromisoformat(forecast_at)
    except ValueError as exc:
        raise ValueError("forecast_at must be an ISO 8601 timestamp with offset") from exc
    if requested.tzinfo is None or requested.utcoffset() is None:
        raise ValueError("forecast_at must include a UTC offset")
    local = requested.astimezone(NEW_YORK)
    if local.minute not in (0, 30) or local.second or local.microsecond:
        raise ValueError("forecast_at must align to a 30-minute New York bucket")
    target = requested.timestamp()
    for index, value in enumerate(timestamps):
        if datetime.fromisoformat(str(value)).timestamp() == target:
            return index
    raise ValueError("forecast_at is outside the supported dataset buckets")


def predict_at(model, arrays: dict, metadata: dict, zone_id: int, forecast_at: str, *, lookback: int) -> dict:
    target_index = resolve_target_index(arrays["timestamps"], forecast_at)
    left, right = (int(value) for value in metadata["split_indices"]["test"])
    if not left <= target_index < right:
        raise ValueError("forecast_at must be in the held-out February 2023 period")
    matches = np.flatnonzero(arrays["zone_ids"] == zone_id)
    if len(matches) != 1:
        raise ValueError("zone_id is not in the trained zone set")
    dataset = ZoneDemandDataset(arrays, metadata, "test", lookback=lookback)
    first = dataset.target_indices[0]
    history, calendar, _, _ = dataset[target_index - first]
    adjacency = torch.from_numpy(arrays["fixed_adjacency"]).float()
    with torch.inference_mode():
        scaled = model(history.unsqueeze(0), calendar.unsqueeze(0), adjacency).detach().cpu().numpy()
    scaler = RobustZoneScaler(center=arrays["scaler_center"], scale=arrays["scaler_scale"])
    values = scaler.inverse_quantiles(scaled)[0, int(matches[0])]
    if not np.isfinite(values).all() or not values[0] <= values[1] <= values[2]:
        raise ValueError("model returned invalid quantiles")
    return {
        "zone_id": int(zone_id),
        "forecast_at": str(arrays["timestamps"][target_index]),
        "horizon_minutes": 30,
        "p10": float(values[0]), "p50": float(values[1]), "p90": float(values[2]),
        "observed_history_buckets": int(arrays["observed"][target_index - lookback:target_index].any(axis=1).sum()),
        "history_buckets": lookback,
    }


def validate_report_provenance(report: dict, dataset_fingerprint: str) -> bool:
    reported = (report.get("artifact") or {}).get("dataset_fingerprint")
    if reported is None:
        return False
    if reported != dataset_fingerprint:
        raise ValueError("test report dataset fingerprint mismatch")
    return True


def evidence_document(metrics: dict | None, result: dict | None, baseline: dict | None = None,
                      *, report_fingerprint_verified: bool = False) -> dict:
    distribution = (result or {}).get("distribution", {})
    comparison = None
    comparison_missing = []
    preferred_method = "undetermined"
    if metrics and metrics.get("status") == "PASS" and baseline is not None:
        same_targets = (
            int(metrics["observed_target_count"]) == int(baseline["target_count"])
            and int(baseline["matched_count"]) == int(baseline["target_count"])
            and int(metrics["test_bucket_count"]) == int(baseline["test_bucket_count"])
        )
        if same_targets:
            model_wape = float(metrics["wape"])
            baseline_wape = baseline["wape"]
            comparison = {
                **baseline,
                "same_test_targets": True,
                "model_wape": model_wape,
                "model_mae": float(metrics["mae"]),
                "wape_gap": model_wape - baseline_wape if baseline_wape is not None else None,
            }
            if baseline_wape is not None and baseline_wape < model_wape:
                preferred_method = "weekly_naive"
        else:
            comparison_missing.append("Baseline and model lack the same observed test targets.")
    return {
        "benchmark_status": "preliminary" if metrics and metrics.get("status") == "PASS" else "unavailable",
        "use_status": "research_only",
        "preferred_method": preferred_method,
        "baseline_comparison": comparison,
        "test_report_fingerprint_verified": report_fingerprint_verified,
        "metrics": metrics if metrics and metrics.get("status") == "PASS" else None,
        "distributed_training": distribution,
        "run_id": (result or {}).get("run_id", DEFAULT_RUN_ID),
        "best_epoch": (result or {}).get("best_epoch"),
        "training_end_epoch": (result or {}).get("last_epoch"),
        "parameter_count": (result or {}).get("parameter_count"),
        "missing_evidence": comparison_missing + (["Legacy test report has no dataset fingerprint or exact key manifest."]
                             if not report_fingerprint_verified else []) + [
            "same-key Spark MLlib GBT and remaining model comparisons",
            "validation-WAPE checkpoint selection",
            "interval calibration and coverage by zone",
        ],
        "limitations": [
            "October and December 2022 source coverage is incomplete.",
            "The February 2023 test report is not a production-quality claim.",
        ],
    }


def verified_best_step(payload: dict) -> int:
    stopper = payload.get("stopper", {})
    best_step = stopper.get("best_step")
    best_state = stopper.get("best_state")
    if not isinstance(best_step, int) or not 1 <= best_step <= int(payload.get("epoch", 0)):
        raise ValueError("checkpoint best step is invalid")
    if not isinstance(best_state, dict) or payload["model"].keys() != best_state.keys():
        raise ValueError("checkpoint best weights are missing")
    if not all(torch.equal(value, best_state[name]) for name, value in payload["model"].items()):
        raise ValueError("checkpoint best weights do not match saved best state")
    return best_step


def load_verified_model(config: dict, arrays: dict, metadata: dict,
                        store: S3CheckpointStore, variant: str) -> tuple[GTQNLite, int]:
    model = GTQNLite(_model_config(config, num_zones=int(arrays["counts"].shape[1]),
                                   calendar_channels=int(arrays["calendar"].shape[1]), variant=variant))
    with tempfile.TemporaryDirectory(prefix="urbanflow-checkpoint-serving-") as directory:
        path = Path(directory) / "best.pt"
        store.client.download_file(store.bucket, f"{store.prefix}/best.pt", str(path))
        payload = torch.load(path, map_location="cpu", weights_only=False)
    if payload["config_hash"] != _training_config_hash(config, variant):
        raise ValueError("checkpoint config hash mismatch")
    if payload["dataset_fingerprint"] != metadata["dataset_fingerprint"]:
        raise ValueError("checkpoint dataset fingerprint mismatch")
    best_step = verified_best_step(payload)
    model.load_state_dict(payload["model"])
    return model, best_step


class ModelStore:
    def __init__(self, config_path: str | Path, run_id: str = DEFAULT_RUN_ID, variant: str = "full") -> None:
        self.config = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
        self.run_id = run_id
        self.variant = variant
        self.prefix = f"dev/ml/runs/{run_id}/training/{variant}"
        self.store = S3CheckpointStore(bucket="urbanflow", prefix=self.prefix,
                                      endpoint_url=os.getenv("URBANFLOW_S3_ENDPOINT", "http://seaweedfs:8333"))
        uri = f"s3a://urbanflow/dev/ml/runs/{run_id}/tensor/artifact.npz"
        with tempfile.TemporaryDirectory(prefix="urbanflow-serving-") as directory:
            path = Path(directory) / "artifact.npz"
            download_s3a_uri(uri, path)
            self.arrays, self.metadata = load_tensor_artifact(path)
        self.model, self.best_epoch = load_verified_model(
            self.config, self.arrays, self.metadata, self.store, variant
        )
        self.model.eval()
        self.result = self._read_json(f"{self.prefix}/result.json")
        if self.result is None or self.result.get("status") != "PASS":
            raise ValueError("training result is missing or not PASS")
        if int(self.result.get("best_epoch", -1)) != self.best_epoch:
            raise ValueError("training manifest best epoch does not match checkpoint")
        self.metrics = self._read_json(f"{self.prefix}/test_metrics.json")
        self.report_fingerprint_verified = (
            validate_report_provenance(self.metrics, self.metadata["dataset_fingerprint"])
            if self.metrics is not None else False
        )
        self.weekly_baseline = weekly_naive_report(self.arrays, self.metadata)
        self.zone_names = self._read_zone_names()

    def _read_json(self, key: str) -> dict | None:
        try:
            response = self.store.client.get_object(Bucket="urbanflow", Key=key)
        except self.store.client.exceptions.NoSuchKey:
            return None
        return json.loads(response["Body"].read())

    def _read_zone_names(self) -> dict[int, str]:
        key = "bronze/zone_lookup/taxi_zone_lookup.csv"
        try:
            response = self.store.client.get_object(Bucket="urbanflow", Key=key)
            rows = csv.DictReader(io.StringIO(response["Body"].read().decode("utf-8-sig")))
            return {int(row["LocationID"]): row["Zone"] for row in rows}
        except self.store.client.exceptions.NoSuchKey:
            return {}

    def options(self) -> dict:
        left, right = (int(value) for value in self.metadata["split_indices"]["test"])
        return {
            "zones": [{"id": int(zone_id), "name": self.zone_names.get(int(zone_id), f"Zone {zone_id}")}
                      for zone_id in self.arrays["zone_ids"]],
            "available_buckets": self.arrays["timestamps"][left:right].tolist(),
            "run_id": self.run_id,
            "best_epoch": self.best_epoch,
            "training_end_epoch": int(self.result["last_epoch"]),
            "timezone": "America/New_York",
        }

    def forecast(self, zone_id: int, forecast_at: str) -> dict:
        result = predict_at(self.model, self.arrays, self.metadata, zone_id, forecast_at,
                            lookback=int(self.config["tensor"]["lookback_buckets"]))
        target_index = resolve_target_index(self.arrays["timestamps"], forecast_at)
        zone_index = int(np.flatnonzero(self.arrays["zone_ids"] == zone_id)[0])
        reference = retrospective_reference(self.arrays, target_index, zone_index)
        return {**result, **reference, "run_id": self.run_id, "best_epoch": self.best_epoch,
                "training_end_epoch": int(self.result["last_epoch"]),
                "benchmark_status": "preliminary", "use_status": "research_only"}

    def evidence(self) -> dict:
        return evidence_document(self.metrics, self.result, self.weekly_baseline,
                                 report_fingerprint_verified=self.report_fingerprint_verified)

    def information(self, anomaly: dict | None, anomaly_error: str | None = None) -> dict:
        training_evidence = {
            **self.result,
            "metrics": self.metrics,
            "weekly_baseline": self.weekly_baseline,
            "distribution": self.result.get("distribution", {}),
        }
        return build_training_information(
            config=self.config,
            metadata=self.metadata,
            evidence=training_evidence,
            anomaly=anomaly,
            anomaly_error=anomaly_error,
        )


@lru_cache(maxsize=1)
def get_model_store() -> ModelStore:
    return ModelStore(os.getenv("URBANFLOW_ML_CONFIG", str(DEFAULT_CONFIG)),
                      os.getenv("URBANFLOW_ML_RUN_ID", DEFAULT_RUN_ID))
