from __future__ import annotations

import json
import os
import socket
import sys
import tempfile
import unittest
from pathlib import Path

import torch
import torch.multiprocessing as mp

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from forecasting.distributed import (
    checkpoint_key,
    ddp_find_unused_parameters,
    destroy_distributed,
    gather_rank_evidence,
    initialize_distributed,
    is_training_complete,
    parse_s3a_uri,
    summarize_rank_evidence,
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _contract_worker(rank: int, world_size: int, port: int, output_path: str) -> None:
    os.environ.update(
        {
            "MASTER_ADDR": "127.0.0.1",
            "MASTER_PORT": str(port),
            "RANK": str(rank),
            "WORLD_SIZE": str(world_size),
            "LOCAL_RANK": str(rank),
        }
    )
    initialize_distributed("gloo")
    try:
        torch.manual_seed(23)
        model = torch.nn.parallel.DistributedDataParallel(torch.nn.Linear(2, 1, bias=False))
        optimizer = torch.optim.SGD(model.parameters(), lr=0.1)
        values = torch.tensor([[float(rank + 1), 1.0]])
        loss = model(values).square().mean()
        optimizer.zero_grad(set_to_none=True)
        loss.backward()
        optimizer.step()
        evidence = gather_rank_evidence(model.module, batches=1)
        if rank == 0:
            Path(output_path).write_text(json.dumps(evidence), encoding="utf-8")
    finally:
        destroy_distributed()


class DistributedContractTest(unittest.TestCase):
    def test_conditional_graph_variants_enable_unused_parameter_detection(self):
        self.assertFalse(ddp_find_unused_parameters("full"))
        self.assertTrue(ddp_find_unused_parameters("fixed_graph"))
        self.assertTrue(ddp_find_unused_parameters("no_graph"))

    def test_only_final_or_early_stopped_result_is_resume_complete(self):
        self.assertFalse(is_training_complete({"last_epoch": 2, "stopped_early": False}, target_max_epochs=60))
        self.assertTrue(is_training_complete({"last_epoch": 60, "stopped_early": False}, target_max_epochs=60))
        self.assertTrue(is_training_complete({"last_epoch": 11, "stopped_early": True}, target_max_epochs=60))

    def test_s3a_artifact_uri_is_split_for_executor_download(self):
        self.assertEqual(
            parse_s3a_uri("s3a://urbanflow/dev/ml/runs/run-a/tensor/artifact.npz"),
            ("urbanflow", "dev/ml/runs/run-a/tensor/artifact.npz"),
        )

    def test_checkpoint_key_is_epoch_ordered(self):
        self.assertEqual(
            checkpoint_key("runs/run-a/full", 12),
            "runs/run-a/full/checkpoints/epoch-00012.pt",
        )

    def test_spark_evidence_requires_distinct_hosts_and_equal_checksums(self):
        evidence = [
            {"rank": 0, "world_size": 2, "hostname": "worker-1", "batches": 3, "parameter_checksum": "abc"},
            {"rank": 1, "world_size": 2, "hostname": "worker-2", "batches": 3, "parameter_checksum": "abc"},
        ]
        self.assertEqual(summarize_rank_evidence(evidence, require_distinct_hosts=True)["status"], "PASS")

        evidence[1]["hostname"] = "worker-1"
        self.assertEqual(summarize_rank_evidence(evidence, require_distinct_hosts=True)["status"], "FAIL")

    def test_two_gloo_ranks_all_reduce_to_identical_parameters(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence.json"
            mp.spawn(_contract_worker, args=(2, _free_port(), str(output)), nprocs=2, join=True)
            evidence = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(len(evidence), 2)
        self.assertEqual({item["rank"] for item in evidence}, {0, 1})
        self.assertEqual({item["world_size"] for item in evidence}, {2})
        self.assertEqual({item["batches"] for item in evidence}, {1})
        self.assertEqual(len({item["parameter_checksum"] for item in evidence}), 1)


if __name__ == "__main__":
    unittest.main()
