"""PyTorch DDP initialization and auditable rank evidence."""

from __future__ import annotations

import hashlib
import json
import os
import random
import socket
import tempfile
from pathlib import Path

import numpy as np
import torch
import torch.distributed as dist


def parse_s3a_uri(uri: str) -> tuple[str, str]:
    prefix = "s3a://"
    if not uri.startswith(prefix):
        raise ValueError(f"expected an s3a URI, got: {uri}")
    bucket, separator, key = uri[len(prefix) :].partition("/")
    if not separator or not bucket or not key:
        raise ValueError(f"expected an s3a URI with bucket and key, got: {uri}")
    return bucket, key


def is_training_complete(result: dict, *, target_max_epochs: int) -> bool:
    return bool(result.get("stopped_early")) or int(result.get("last_epoch", 0)) >= target_max_epochs


def ddp_find_unused_parameters(variant: str) -> bool:
    if variant not in {"no_graph", "fixed_graph", "full"}:
        raise ValueError(f"unknown model variant: {variant}")
    return variant != "full"


def download_s3a_uri(uri: str, local_path: str | Path) -> None:
    import boto3
    from botocore.config import Config

    bucket, key = parse_s3a_uri(uri)
    client = boto3.client(
        "s3",
        endpoint_url="http://seaweedfs:8333",
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}),
    )
    client.download_file(bucket, key, str(local_path))


def checkpoint_key(prefix: str, epoch: int) -> str:
    return f"{prefix.strip('/')}/checkpoints/epoch-{int(epoch):05d}.pt"


def summarize_rank_evidence(evidence: list[dict], *, require_distinct_hosts: bool) -> dict:
    ranks = {int(item["rank"]) for item in evidence}
    world_sizes = {int(item["world_size"]) for item in evidence}
    hosts = {str(item["hostname"]) for item in evidence}
    checksums = {str(item["parameter_checksum"]) for item in evidence}
    positive_batches = all(int(item["batches"]) > 0 for item in evidence)
    expected_world_size = next(iter(world_sizes)) if len(world_sizes) == 1 else -1
    complete_ranks = expected_world_size > 0 and ranks == set(range(expected_world_size))
    distinct_hosts = len(hosts) == expected_world_size
    passed = (
        complete_ranks
        and positive_batches
        and len(checksums) == 1
        and (distinct_hosts or not require_distinct_hosts)
    )
    return {
        "status": "PASS" if passed else "FAIL",
        "world_size": expected_world_size,
        "ranks": sorted(ranks),
        "hosts": sorted(hosts),
        "distinct_host_count": len(hosts),
        "parameter_checksums": sorted(checksums),
        "rank_evidence": sorted(evidence, key=lambda item: int(item["rank"])),
    }


class S3CheckpointStore:
    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        endpoint_url: str = "http://seaweedfs:8333",
    ) -> None:
        import boto3
        from botocore.config import Config

        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            config=Config(s3={"addressing_style": "path"}),
        )

    @property
    def latest_key(self) -> str:
        return f"{self.prefix}/checkpoints/latest.json"

    def upload_epoch(
        self,
        local_path: str | Path,
        *,
        epoch: int,
        config_hash: str,
        dataset_fingerprint: str,
    ) -> str:
        key = checkpoint_key(self.prefix, epoch)
        self.client.upload_file(str(local_path), self.bucket, key)
        manifest = {
            "status": "PASS",
            "epoch": int(epoch),
            "key": key,
            "config_hash": config_hash,
            "dataset_fingerprint": dataset_fingerprint,
        }
        self.client.put_object(
            Bucket=self.bucket,
            Key=self.latest_key,
            Body=json.dumps(manifest, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
        return key

    def download_latest(
        self,
        local_path: str | Path,
        *,
        expected_config_hash: str,
        expected_dataset_fingerprint: str,
    ) -> dict | None:
        from botocore.exceptions import ClientError

        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self.latest_key)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        manifest = json.loads(response["Body"].read().decode("utf-8"))
        if manifest["config_hash"] != expected_config_hash:
            raise ValueError("latest checkpoint config hash does not match requested run")
        if manifest["dataset_fingerprint"] != expected_dataset_fingerprint:
            raise ValueError("latest checkpoint dataset fingerprint does not match requested run")
        self.client.download_file(self.bucket, manifest["key"], str(local_path))
        return manifest

    def upload_file(self, local_path: str | Path, key: str) -> None:
        self.client.upload_file(str(local_path), self.bucket, key)

    def put_json(self, key: str, value: dict) -> None:
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(value, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )


def initialize_distributed(backend: str = "gloo") -> None:
    required = ("MASTER_ADDR", "MASTER_PORT", "RANK", "WORLD_SIZE")
    missing = [name for name in required if name not in os.environ]
    if missing:
        raise RuntimeError(f"missing DDP environment variables: {', '.join(missing)}")
    if not dist.is_initialized():
        dist.init_process_group(backend=backend, init_method="env://")


def parameter_checksum(model: torch.nn.Module) -> str:
    digest = hashlib.sha256()
    for name, value in sorted(model.state_dict().items()):
        tensor = value.detach().cpu().contiguous()
        digest.update(name.encode("utf-8"))
        digest.update(str(tensor.dtype).encode("ascii"))
        digest.update(str(tuple(tensor.shape)).encode("ascii"))
        digest.update(tensor.view(torch.uint8).numpy().tobytes())
    return digest.hexdigest()


def gather_rank_evidence(model: torch.nn.Module, *, batches: int) -> list[dict]:
    if not dist.is_initialized():
        raise RuntimeError("distributed process group is not initialized")
    local = {
        "rank": dist.get_rank(),
        "world_size": dist.get_world_size(),
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "batches": int(batches),
        "parameter_checksum": parameter_checksum(model),
    }
    gathered: list[dict | None] = [None] * dist.get_world_size()
    dist.all_gather_object(gathered, local)
    return [item for item in gathered if item is not None]


def destroy_distributed() -> None:
    if dist.is_initialized():
        dist.destroy_process_group()


def _training_config_hash(config: dict, variant: str) -> str:
    selected = {
        "model": config["model"],
        "tensor": config["tensor"],
        "training": config["training"],
        "variant": variant,
    }
    encoded = json.dumps(selected, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _model_config(config: dict, *, num_zones: int, calendar_channels: int, variant: str):
    from forecasting.gtqn import GTQNConfig

    if variant not in {"no_graph", "fixed_graph", "full"}:
        raise ValueError(f"unknown model variant: {variant}")
    model = config["model"]
    return GTQNConfig(
        num_zones=num_zones,
        history_channels=2,
        calendar_channels=calendar_channels,
        hidden_size=int(model["hidden_size"]),
        zone_embedding_size=int(model["zone_embedding_size"]),
        temporal_dilations=tuple(int(value) for value in model["temporal_dilations"]),
        temporal_zone_chunk_size=int(model["temporal_zone_chunk_size"]),
        activation_checkpointing=bool(model["activation_checkpointing"]),
        dropout=float(model["dropout"]),
        use_fixed_graph=variant in {"fixed_graph", "full"},
        use_adaptive_graph=variant == "full",
    )


def distributed_train(
    config: dict,
    artifact_uri: str,
    run_id: str,
    variant: str,
    resume: bool,
    max_epochs_override: int | None = None,
) -> dict:
    """Executor entry point used by Spark TorchDistributor."""

    from torch.nn.parallel import DistributedDataParallel
    from torch.utils.data import DataLoader

    from forecasting.dataset import RankPartitionSampler, ZoneDemandDataset
    from forecasting.gtqn import GTQNLite
    from forecasting.tensor_data import load_tensor_artifact
    from forecasting.trainer import (
        EarlyStopper,
        evaluate_epoch,
        load_checkpoint,
        save_checkpoint,
        train_epoch,
    )

    initialize_distributed("gloo")
    rank = dist.get_rank()
    world_size = dist.get_world_size()
    training = config["training"]
    seed = int(training["seed"])
    torch.set_num_threads(1)
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    try:
        with tempfile.TemporaryDirectory(prefix=f"urbanflow-artifact-rank-{rank}-") as artifact_directory:
            artifact_path = Path(artifact_directory) / "artifact.npz"
            download_s3a_uri(artifact_uri, artifact_path)
            arrays, metadata = load_tensor_artifact(artifact_path)
        dataset_fingerprint = metadata["dataset_fingerprint"]
        model_config = _model_config(
            config,
            num_zones=int(arrays["counts"].shape[1]),
            calendar_channels=int(arrays["calendar"].shape[1]),
            variant=variant,
        )
        model = GTQNLite(model_config)
        ddp_model = DistributedDataParallel(model, find_unused_parameters=ddp_find_unused_parameters(variant))
        optimizer = torch.optim.AdamW(
            ddp_model.parameters(),
            lr=float(training["learning_rate"]),
            weight_decay=float(training["weight_decay"]),
        )
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode="min", factor=0.5, patience=3)
        stopper = EarlyStopper(patience=int(training["early_stopping_patience"]))
        config_hash = _training_config_hash(config, variant)
        prefix = f"dev/ml/runs/{run_id}/training/{variant}"
        store = S3CheckpointStore(bucket="urbanflow", prefix=prefix)

        train_dataset = ZoneDemandDataset(
            arrays,
            metadata,
            "train",
            lookback=int(config["tensor"]["lookback_buckets"]),
            target_stride=int(training["train_target_stride"]),
        )
        validation_dataset = ZoneDemandDataset(
            arrays,
            metadata,
            "validation",
            lookback=int(config["tensor"]["lookback_buckets"]),
            target_stride=int(training["validation_target_stride"]),
        )
        sampler = RankPartitionSampler(
            train_dataset,
            rank=rank,
            world_size=world_size,
            seed=seed,
            shuffle=True,
        )
        train_loader = DataLoader(
            train_dataset,
            batch_size=int(training["batch_size_per_rank"]),
            sampler=sampler,
            num_workers=0,
        )
        validation_loader = DataLoader(
            validation_dataset,
            batch_size=int(training["evaluation_batch_size_per_rank"]),
            shuffle=False,
            num_workers=0,
        )
        adjacency = torch.from_numpy(arrays["fixed_adjacency"]).float()
        start_epoch = 1
        resumed_from_epoch = 0
        max_epochs = int(max_epochs_override or training["max_epochs"])
        history: list[dict] = []
        total_batches = 0

        with tempfile.TemporaryDirectory(prefix=f"urbanflow-rank-{rank}-") as directory:
            checkpoint_path = Path(directory) / "checkpoint.pt"
            if resume:
                manifest = store.download_latest(
                    checkpoint_path,
                    expected_config_hash=config_hash,
                    expected_dataset_fingerprint=dataset_fingerprint,
                )
                if manifest is not None:
                    resumed_from_epoch = load_checkpoint(
                        checkpoint_path,
                        model=model,
                        optimizer=optimizer,
                        scheduler=scheduler,
                        stopper=stopper,
                        expected_config_hash=config_hash,
                        expected_dataset_fingerprint=dataset_fingerprint,
                    )
                    start_epoch = resumed_from_epoch + 1
            dist.barrier()

            stopped_early = False
            last_epoch = resumed_from_epoch
            for epoch in range(start_epoch, max_epochs + 1):
                epoch_seed = seed + epoch * world_size + rank
                random.seed(epoch_seed)
                np.random.seed(epoch_seed)
                torch.manual_seed(epoch_seed)
                sampler.set_epoch(epoch)
                local_train_loss, batches = train_epoch(
                    ddp_model,
                    train_loader,
                    adjacency,
                    optimizer,
                    gradient_clip_norm=float(training["gradient_clip_norm"]),
                    gradient_accumulation_steps=int(training["gradient_accumulation_steps"]),
                    device=torch.device("cpu"),
                )
                total_batches += batches
                loss_tensor = torch.tensor(local_train_loss, dtype=torch.float64)
                dist.all_reduce(loss_tensor, op=dist.ReduceOp.SUM)
                train_loss = float(loss_tensor / world_size)
                validation_loss, _, _, _ = evaluate_epoch(
                    ddp_model,
                    validation_loader,
                    adjacency,
                    device=torch.device("cpu"),
                )
                scheduler.step(validation_loss)
                stopped_early = stopper.update(validation_loss, model, step=epoch)
                last_epoch = epoch
                epoch_record = {
                    "epoch": epoch,
                    "train_loss": train_loss,
                    "validation_loss": validation_loss,
                    "learning_rate": float(optimizer.param_groups[0]["lr"]),
                }
                history.append(epoch_record)

                if rank == 0:
                    save_checkpoint(
                        checkpoint_path,
                        model=model,
                        optimizer=optimizer,
                        scheduler=scheduler,
                        stopper=stopper,
                        epoch=epoch,
                        config_hash=config_hash,
                        dataset_fingerprint=dataset_fingerprint,
                    )
                    store.upload_epoch(
                        checkpoint_path,
                        epoch=epoch,
                        config_hash=config_hash,
                        dataset_fingerprint=dataset_fingerprint,
                    )
                    print(json.dumps({"rank": rank, **epoch_record}, sort_keys=True), flush=True)
                dist.barrier()
                if stopped_early:
                    break

            stopper.restore_best(model)
            evidence = gather_rank_evidence(model, batches=total_batches)
            distribution = summarize_rank_evidence(evidence, require_distinct_hosts=True)
            summary = {
                "status": distribution["status"],
                "run_id": run_id,
                "variant": variant,
                "config_hash": config_hash,
                "dataset_fingerprint": dataset_fingerprint,
                "resumed_from_epoch": resumed_from_epoch,
                "best_epoch": int(stopper.best_step or resumed_from_epoch),
                "last_epoch": int(last_epoch),
                "best_validation_loss": float(stopper.best_metric),
                "stopped_early": bool(stopped_early),
                "parameter_count": sum(parameter.numel() for parameter in model.parameters()),
                "history": history,
                "distribution": distribution,
            }
            if rank == 0:
                best_path = Path(directory) / "best.pt"
                save_checkpoint(
                    best_path,
                    model=model,
                    optimizer=optimizer,
                    scheduler=scheduler,
                    stopper=stopper,
                    epoch=last_epoch,
                    config_hash=config_hash,
                    dataset_fingerprint=dataset_fingerprint,
                )
                store.upload_file(best_path, f"{prefix}/best.pt")
                store.put_json(f"{prefix}/result.json", summary)
            dist.barrier()
            return summary
    finally:
        destroy_distributed()
