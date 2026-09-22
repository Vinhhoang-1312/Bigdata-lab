"""Spark MLlib model helpers for the UrbanFlow demand forecast experiment."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from forecasting.evaluation import score_predictions
from forecasting.features import FEATURE_COLUMNS


@dataclass(frozen=True, order=True)
class GBTParams:
    max_depth: int
    max_iter: int
    step_size: float
    subsampling_rate: float
    loss_type: str = "squared"
    max_memory_in_mb: int = 128
    cache_node_ids: bool = False

    def as_metadata(self) -> dict[str, bool | float | int | str]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateResult:
    params: GBTParams
    validation_mae: float

    def as_metadata(self) -> dict[str, object]:
        return {
            "params": self.params.as_metadata(),
            "validation_mae": self.validation_mae,
        }


@dataclass(frozen=True)
class SelectionResult:
    selected_params: GBTParams
    validation_mae: float
    candidate_results: list[CandidateResult]

    def as_metadata(self) -> dict[str, object]:
        return {
            "selected_params": self.selected_params.as_metadata(),
            "validation_mae": self.validation_mae,
            "candidate_results": [result.as_metadata() for result in self.candidate_results],
        }


DEFAULT_CANDIDATES = [
    GBTParams(max_depth=4, max_iter=30, step_size=0.05, subsampling_rate=0.8),
    GBTParams(max_depth=5, max_iter=40, step_size=0.05, subsampling_rate=0.8),
    GBTParams(max_depth=6, max_iter=40, step_size=0.05, subsampling_rate=0.8),
    GBTParams(max_depth=5, max_iter=30, step_size=0.10, subsampling_rate=0.8),
]


def numeric_feature_columns(feature_cols: list[str] | None = None) -> list[str]:
    columns = feature_cols or FEATURE_COLUMNS
    return [column for column in columns if column != "pickup_zone_id"]


def build_pipeline(seed: int, params: GBTParams, *, feature_cols: list[str] | None = None):
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
    from pyspark.ml.regression import GBTRegressor

    numeric_features = numeric_feature_columns(feature_cols)
    zone_indexer = StringIndexer(
        inputCol="pickup_zone_id",
        outputCol="pickup_zone_index",
        handleInvalid="keep",
    )
    zone_encoder = OneHotEncoder(
        inputCols=["pickup_zone_index"],
        outputCols=["pickup_zone_onehot"],
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=["pickup_zone_onehot", *numeric_features],
        outputCol="features",
        handleInvalid="keep",
    )
    regressor = GBTRegressor(
        featuresCol="features",
        labelCol="label",
        predictionCol="raw_prediction",
        maxDepth=params.max_depth,
        maxIter=params.max_iter,
        stepSize=params.step_size,
        subsamplingRate=params.subsampling_rate,
        lossType=params.loss_type,
        maxMemoryInMB=params.max_memory_in_mb,
        cacheNodeIds=params.cache_node_ids,
        seed=seed,
    )
    return Pipeline(stages=[zone_indexer, zone_encoder, assembler, regressor])


def apply_prediction_postprocessing(predictions):
    from pyspark.sql import functions as F

    return predictions.withColumn(
        "predicted_trip_count",
        F.greatest(F.lit(0.0), F.col("raw_prediction").cast("double")),
    )


def predict(model, frame):
    return apply_prediction_postprocessing(model.transform(frame))


def select_params(train, validation, candidates: list[GBTParams], *, seed: int) -> SelectionResult:
    if not candidates:
        raise ValueError("select_params requires at least one candidate")

    results = []
    for params in candidates:
        model = build_pipeline(seed=seed, params=params).fit(train)
        predictions = predict(model, validation)
        report = score_predictions(predictions, "predicted_trip_count")
        if report.mae is None:
            raise ValueError("validation MAE is undefined")
        results.append(CandidateResult(params=params, validation_mae=report.mae))

    ordered = sorted(
        results,
        key=lambda result: (
            result.validation_mae,
            result.params.max_depth,
            result.params.max_iter,
            result.params.step_size,
            result.params.subsampling_rate,
        ),
    )
    selected = ordered[0]
    return SelectionResult(
        selected_params=selected.params,
        validation_mae=selected.validation_mae,
        candidate_results=ordered,
    )
