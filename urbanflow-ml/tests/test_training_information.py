"""Stable, honest training-information contract for the ML workspace."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))


def complete_inputs() -> tuple[dict, dict, dict, dict]:
    config = {
        "forecast": {
            "bucket_minutes": 30,
            "horizon_minutes": 30,
            "train_start": "2022-08-08",
            "train_end": "2022-12-31",
            "validation_start": "2023-01-01",
            "validation_end": "2023-01-31",
            "test_start": "2023-02-01",
            "test_end": "2023-02-28",
        },
        "tensor": {"lookback_buckets": 336},
        "model": {"name": "GTQN-Lite"},
    }
    metadata = {
        "dataset_fingerprint": "abcdef0123456789",
        "split_indices": {"train": [0, 9000], "validation": [9000, 10500], "test": [10500, 11844]},
        "zone_count": 265,
        "calendar_feature_count": 7,
    }
    evidence = {
        "run_id": "gtqn-test-001",
        "status": "PASS",
        "best_epoch": 8,
        "last_epoch": 10,
        "parameter_count": 123456,
        "config_hash": "config-hash",
        "distribution": {
            "status": "PASS",
            "world_size": 2,
            "ranks": [0, 1],
            "distinct_host_count": 2,
            "hosts": ["spark-worker-1", "spark-worker-2"],
            "batches": 876,
            "parameter_checksums": {"0": "a" * 64, "1": "b" * 64},
            "parameter_checksum_match": True,
        },
        "metrics": {"status": "PASS", "mae": 1.2, "wape": 0.31, "coverage_10_90": 0.88},
        "weekly_baseline": {"method": "same_zone_same_half_hour_previous_week", "wape": 0.35},
    }
    anomaly = {
        "status": "PASS",
        "run_id": "trip-anomaly-test",
        "method": "spark_mllib_kmeans",
        "cluster_count": 8,
        "threshold": 2.4,
        "threshold_percentile": 99.5,
        "training_period": "2022-11",
        "calibration_period": "2023-01",
        "screening_period": "2023-02",
        "training_sample_count": 1000,
        "calibration_sample_count": 900,
        "screened_sample_count": 800,
        "features": ["log_distance", "log_duration", "log_fare", "log_speed"],
        "spark_worker_hosts": ["spark-worker-1", "spark-worker-2"],
        "limitations": ["No ground-truth anomaly labels."],
    }
    return config, metadata, evidence, anomaly


class TrainingInformationTest(unittest.TestCase):
    def test_complete_response_has_stable_semantics_and_artifact_evidence(self):
        from ml_serving.training_information import build_training_information

        config, metadata, evidence, anomaly = complete_inputs()
        result = build_training_information(
            config=config, metadata=metadata, evidence=evidence, anomaly=anomaly
        )

        self.assertEqual(
            {"use_status", "demand_forecast", "kmeans_anomaly", "preprocessing", "techniques", "limitations"},
            set(result),
        )
        self.assertEqual("research_only", result["use_status"])
        demand = result["demand_forecast"]
        self.assertEqual("available", demand["availability"])
        self.assertEqual("GTQN-Lite", demand["model_name"])
        self.assertEqual("graph_temporal_quantile_regression", demand["model_family"])
        self.assertEqual(336, demand["lookback_buckets"])
        self.assertEqual(["2022-08-08", "2022-12-31"], demand["splits"]["train"])
        distributed = demand["distributed_training"]
        self.assertEqual(2, distributed["world_size"])
        self.assertEqual(["spark-worker-1", "spark-worker-2"], distributed["training_hosts"])
        self.assertTrue(distributed["parameter_checksums_match"])
        self.assertEqual(["aaaaaaaa", "bbbbbbbb"], distributed["parameter_checksum_prefixes"])
        anomaly_view = result["kmeans_anomaly"]
        self.assertEqual("available", anomaly_view["availability"])
        self.assertEqual(8, anomaly_view["k"])
        self.assertEqual(2.4, anomaly_view["threshold"])
        self.assertEqual(["log_distance", "log_duration", "log_fare", "log_speed"], anomaly_view["features"])
        self.assertTrue(all(item["type"] and item["purpose"] for item in result["techniques"]))

    def test_partial_anomaly_evidence_degrades_only_anomaly_section(self):
        from ml_serving.training_information import build_training_information

        config, metadata, evidence, _ = complete_inputs()
        result = build_training_information(
            config=config, metadata=metadata, evidence=evidence,
            anomaly=None, anomaly_error="artifact_unavailable",
        )

        self.assertEqual("available", result["demand_forecast"]["availability"])
        self.assertEqual("unavailable", result["kmeans_anomaly"]["availability"])
        self.assertEqual("artifact_unavailable", result["kmeans_anomaly"]["error"])
        text = json.dumps(result).lower()
        for forbidden in ("active nodes", "healthy nodes", "current cluster health"):
            self.assertNotIn(forbidden, text)

    def test_missing_data_policies_are_explicit_and_do_not_impute_trip_measurements(self):
        from ml_serving.training_information import build_training_information

        config, metadata, evidence, anomaly = complete_inputs()
        result = build_training_information(config=config, metadata=metadata, evidence=evidence, anomaly=anomaly)
        policies = {item["input"]: item for item in result["preprocessing"]}
        self.assertIn("observed_mask", policies["Source gap"]["policy"])
        self.assertIn("missing indicator", policies["Seasonal lag unavailable"]["policy"])
        self.assertIn("rejected", policies["K-means required features"]["policy"])
        self.assertNotIn("mean_imputation", json.dumps(result))
        self.assertNotIn("mode_imputation", json.dumps(result))


if __name__ == "__main__":
    unittest.main()
