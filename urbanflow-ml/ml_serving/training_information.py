"""Build the stable, artifact-backed information contract for the ML workspace."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _prefix(value: Any, length: int = 8) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text[:length] if text else None


def _checksum_evidence(distribution: Mapping[str, Any]) -> tuple[list[str], bool | None]:
    raw = distribution.get("parameter_checksums") or distribution.get("parameter_checksum") or {}
    values = raw.values() if isinstance(raw, Mapping) else raw if isinstance(raw, list) else []
    prefixes = [_prefix(value) for value in values]
    prefixes = [value for value in prefixes if value is not None]
    match = distribution.get("parameter_checksum_match")
    if match is None and len(values) > 1:
        match = len(set(str(value) for value in values)) == 1
    return prefixes, bool(match) if match is not None else None


def _hosts(distribution: Mapping[str, Any]) -> list[str]:
    values = (distribution.get("training_hosts") or distribution.get("hosts")
              or distribution.get("spark_worker_hosts") or [])
    return [str(value) for value in values]


def _periods(config: Mapping[str, Any]) -> dict[str, list[str]]:
    forecast = config.get("forecast", {})
    return {
        "train": [str(forecast.get("train_start", "unavailable")), str(forecast.get("train_end", "unavailable"))],
        "validation": [str(forecast.get("validation_start", "unavailable")), str(forecast.get("validation_end", "unavailable"))],
        "test": [str(forecast.get("test_start", "unavailable")), str(forecast.get("test_end", "unavailable"))],
    }


def _preprocessing() -> list[dict[str, str]]:
    return [
        {"input": "Required Silver fields", "policy": "Invalid or null rows are rejected upstream", "why": "Keep contract violations visible", "evidence": "Foundation validation and Silver contract"},
        {"input": "Demand zero", "policy": "Keep zero when an observed bucket has no pickups", "why": "Zero demand is a real signal", "evidence": "Zero-inclusive demand tensor"},
        {"input": "Source gap", "policy": "No mean/mode fill; keep count zero and pass observed_mask=false", "why": "Do not confuse missing source data with zero demand", "evidence": "Tensor observed mask"},
        {"input": "Seasonal lag unavailable", "policy": "Use numeric zero and add a missing indicator", "why": "The model can learn that the lag was unavailable", "evidence": "Feature builder lag flags"},
        {"input": "GTQN scaling", "policy": "Apply log1p, then per-zone median/IQR from the training split", "why": "Reduce skew without future leakage", "evidence": "RobustZoneScaler"},
        {"input": "K-means required features", "policy": "Invalid non-positive distance/duration and negative fare are rejected; do not fill", "why": "Invented trip measurements distort clusters", "evidence": "Spark eligibility filter"},
        {"input": "K-means scaling", "policy": "Apply log1p, then StandardScaler fitted on the November train sample", "why": "Put distance, duration, fare and speed on comparable scales", "evidence": "Spark MLlib StandardScaler artifact"},
        {"input": "Time split", "policy": "Use chronological train, validation and test periods", "why": "Prevent future information leakage", "evidence": "Config and tensor split indices"},
    ]


def _techniques() -> list[dict[str, str]]:
    rows = [
        ("Spark MLlib K-means", "clustering algorithm", "Apache Spark", "Group trips by learned feature patterns", "Assign each standardized trip to its nearest center", "DBSCAN", "A high score requests review; it does not prove fraud"),
        ("StandardScaler", "feature preprocessing", "Apache Spark MLlib", "Give features comparable scale", "Center and divide each feature using training statistics", "Robust scaling", "It cannot repair invalid source values"),
        ("GTQN-Lite", "graph-temporal neural model", "PyTorch and UrbanFlow", "Forecast demand for zones with uncertainty", "Mix temporal convolutions, zone graph messages and quantile heads", "LSTM or gradient boosting", "The current evidence is research-only"),
        ("Quantile regression", "probabilistic loss", "UrbanFlow", "Predict P10, P50 and P90 instead of one point", "Penalize under- and over-prediction at each quantile", "Mean squared error", "Intervals still need calibration checks"),
        ("DDP", "distributed training", "PyTorch", "Train one model across worker processes", "Synchronize gradients after each batch", "DataParallel", "It does not make a weak dataset stronger"),
        ("Gloo all-reduce", "gradient communication", "PyTorch", "Combine gradients across CPU ranks", "All ranks reduce matching tensors before the optimizer step", "NCCL", "The evidence is from the completed run, not current health"),
        ("TorchDistributor", "Spark-PyTorch bridge", "Apache Spark", "Launch distributed PyTorch work from Spark", "Spark allocates worker processes and passes rank context", "Ray Train", "It does not replace model design"),
        ("RankPartitionSampler", "distributed data sampling", "UrbanFlow", "Give each rank a disjoint balanced slice", "Partition deterministic indices by rank and epoch", "DistributedSampler", "Bad partitioning can still bias training"),
        ("AdamW", "optimizer", "PyTorch", "Update weights with adaptive steps and decoupled decay", "Track moments while applying weight decay separately", "SGD", "It does not prevent overfitting alone"),
        ("Gradient clipping", "optimization guard", "PyTorch", "Limit unstable gradient spikes", "Scale a step when its norm exceeds the configured bound", "Gradient normalization", "It cannot fix a wrong target"),
        ("Gradient accumulation", "memory technique", "UrbanFlow", "Approximate a larger batch on limited memory", "Add several micro-batch gradients before one update", "Larger physical batch", "It changes update timing"),
        ("Dropout", "regularization", "PyTorch", "Reduce co-adaptation and overfitting", "Randomly mask activations during training", "Weight decay", "It is disabled during evaluation"),
        ("Early stopping", "training control", "UrbanFlow", "Stop after validation quality stops improving", "Restore the best validation checkpoint after patience is used", "Fixed epoch budget", "It is not a target accuracy"),
        ("ReduceLROnPlateau", "learning-rate schedule", "PyTorch", "Take smaller steps when validation stalls", "Lower the learning rate after a monitored plateau", "Cosine decay", "It cannot create new information"),
        ("Checkpoint/resume", "reliability mechanism", "UrbanFlow and SeaweedFS", "Continue after a machine or worker interruption", "Persist model, optimizer, epoch and evidence to object storage", "Restart from scratch", "A resumed run still needs final verification"),
        ("Observed mask", "data-quality feature", "UrbanFlow", "Tell the demand model which zeros came from source gaps", "Carry a boolean availability signal beside counts", "Imputation", "It does not reconstruct missing trips"),
        ("Dataset fingerprint", "provenance check", "UrbanFlow", "Ensure model and evaluation use the same artifact", "Hash ordered data and split metadata", "Version tag only", "A fingerprint proves identity, not data quality"),
    ]
    return [
        {"name": name, "type": kind, "owner_ecosystem": owner, "purpose": purpose,
         "how": how, "alternative": alternative, "limit": limit}
        for name, kind, owner, purpose, how, alternative, limit in rows
    ]


def _demand_section(config: Mapping[str, Any], metadata: Mapping[str, Any], evidence: Mapping[str, Any]) -> dict[str, Any]:
    available = evidence.get("status") in (None, "PASS")
    distribution = evidence.get("distribution") or evidence.get("distributed_training") or {}
    hosts = _hosts(distribution)
    checksum_prefixes, checksum_match = _checksum_evidence(distribution)
    metrics = evidence.get("metrics")
    return {
        "availability": "available" if available else "unavailable",
        "model_name": "GTQN-Lite",
        "model_family": "graph_temporal_quantile_regression",
        "run_id": evidence.get("run_id"),
        "use_status": "research_only",
        "benchmark_status": "preliminary" if metrics and metrics.get("status") == "PASS" else "unavailable",
        "lookback_buckets": int(config.get("tensor", {}).get("lookback_buckets", 336)),
        "bucket_minutes": int(config.get("forecast", {}).get("bucket_minutes", 30)),
        "horizon_minutes": int(config.get("forecast", {}).get("horizon_minutes", 30)),
        "splits": _periods(config),
        "zone_count": metadata.get("zone_count"),
        "parameter_count": evidence.get("parameter_count"),
        "best_epoch": evidence.get("best_epoch"),
        "training_end_epoch": evidence.get("last_epoch", evidence.get("training_end_epoch")),
        "metrics": metrics,
        "weekly_baseline": evidence.get("weekly_baseline"),
        "dataset_fingerprint_available": bool(metadata.get("dataset_fingerprint")),
        "dataset_fingerprint_prefix": _prefix(metadata.get("dataset_fingerprint")),
        "distributed_training": {
            "status": distribution.get("status", "unavailable"),
            "world_size": distribution.get("world_size"),
            "ranks": distribution.get("ranks"),
            "training_hosts": hosts,
            "distinct_host_count": distribution.get("distinct_host_count", len(set(hosts)) if hosts else None),
            "batches": distribution.get("batches"),
            "parameter_checksums_match": checksum_match,
            "parameter_checksum_prefixes": checksum_prefixes,
        },
    }


def _anomaly_section(anomaly: Mapping[str, Any] | None, error: str | None) -> dict[str, Any]:
    if not anomaly or anomaly.get("status") != "PASS":
        return {"availability": "unavailable", "error": error or "artifact_unavailable", "use_status": "research_only"}
    return {
        "availability": "available",
        "use_status": "research_only",
        "run_id": anomaly.get("run_id"),
        "method": anomaly.get("method"),
        "k": anomaly.get("cluster_count"),
        "threshold": anomaly.get("threshold"),
        "threshold_percentile": anomaly.get("threshold_percentile"),
        "periods": {key: anomaly.get(key) for key in ("training_period", "calibration_period", "screening_period")},
        "sample_counts": {key: anomaly.get(key) for key in ("training_sample_count", "calibration_sample_count", "screened_sample_count")},
        "features": anomaly.get("features", []),
        "training_hosts": anomaly.get("spark_worker_hosts", []),
        "limitations": anomaly.get("limitations", []),
    }


def build_training_information(*, config: dict, metadata: dict, evidence: dict,
                               anomaly: dict | None, anomaly_error: str | None = None) -> dict:
    """Build a deterministic UI contract without network or storage calls."""
    return {
        "use_status": "research_only",
        "demand_forecast": _demand_section(config, metadata, evidence),
        "kmeans_anomaly": _anomaly_section(anomaly, anomaly_error),
        "preprocessing": _preprocessing(),
        "techniques": _techniques(),
        "limitations": [
            "The demand model and anomaly detector are research artifacts, not safety or fraud decisions.",
            "A completed worker record describes training-time evidence, not live service health.",
            "Anomaly labels are not verified ground truth; review candidates require human investigation.",
        ],
    }
