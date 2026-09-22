import importlib
import sys
import types

import pytest


def import_runner_without_pyspark(monkeypatch, block_yaml=False):
    if block_yaml:
        original_import = __import__

        def import_without_yaml(name, *args, **kwargs):
            if name == "yaml":
                raise ModuleNotFoundError("No module named 'yaml'")
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr("builtins.__import__", import_without_yaml)

    functions = types.SimpleNamespace()
    sql = types.SimpleNamespace(
        DataFrame=object,
        SparkSession=object,
        functions=functions,
    )
    monkeypatch.setitem(sys.modules, "pyspark", types.SimpleNamespace(sql=sql))
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions)
    monkeypatch.setitem(
        sys.modules,
        "spark.event_schema",
        types.SimpleNamespace(ParsedEvents=object, parse_events=lambda source: source),
    )
    monkeypatch.setitem(
        sys.modules,
        "spark.historical_lookup",
        types.SimpleNamespace(KEY=[], load_historical_baseline=lambda spark, path: path),
    )
    sys.modules.pop("spark.live_trip_metrics", None)
    return importlib.import_module("spark.live_trip_metrics")


def test_streaming_helpers_import_without_yaml_dependency(monkeypatch):
    runner = import_runner_without_pyspark(monkeypatch, block_yaml=True)

    assert callable(runner.join_historical)


def test_fixture_output_cannot_equal_foundation_fixture_path(monkeypatch):
    runner = import_runner_without_pyspark(monkeypatch)

    with pytest.raises(ValueError, match="Foundation fixture"):
        runner.validate_runtime_paths(
            "../urbanflow-foundation/fixtures/gold/expected_live_zone_metrics.jsonl",
            ".checkpoints/fixture-live-trip-metrics",
        )


def test_output_and_checkpoint_must_differ(monkeypatch):
    runner = import_runner_without_pyspark(monkeypatch)

    with pytest.raises(ValueError, match="must be different"):
        runner.validate_runtime_paths("output/fixture-live-gold", "output/fixture-live-gold/")


class DummyQuery:
    pass


class DummyWriter:
    def __init__(self):
        self.trigger_kwargs = None

    def format(self, value):
        return self

    def outputMode(self, value):
        return self

    def option(self, key, value):
        return self

    def trigger(self, **kwargs):
        self.trigger_kwargs = kwargs
        return self

    def start(self):
        return DummyQuery()


class DummyMetrics:
    def __init__(self):
        self.writeStream = DummyWriter()


def test_fixture_mode_selects_available_now_trigger(monkeypatch):
    runner = import_runner_without_pyspark(monkeypatch)
    metrics = DummyMetrics()

    query = runner.write_live_gold(
        metrics,
        "output/fixture-live-gold",
        ".checkpoints/fixture-live-trip-metrics",
        available_now=True,
    )

    assert isinstance(query, DummyQuery)
    assert metrics.writeStream.trigger_kwargs == {"availableNow": True}
