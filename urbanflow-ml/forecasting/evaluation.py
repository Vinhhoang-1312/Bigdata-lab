"""Evaluation metrics for UrbanFlow demand forecasts."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class RegressionReport:
    row_count: int
    mae: float | None
    rmse: float | None
    wape: float | None
    signed_bias: float | None
    clamp_rate: float | None
    warnings: list[str]


def score_predictions(frame, prediction_col: str, *, label_col: str = "label") -> RegressionReport:
    from pyspark.sql import functions as F

    row = frame.select(
        F.count(F.lit(1)).alias("row_count"),
        F.avg(F.abs(F.col(prediction_col) - F.col(label_col))).alias("mae"),
        F.sqrt(F.avg(F.pow(F.col(prediction_col) - F.col(label_col), F.lit(2.0)))).alias("rmse"),
        F.sum(F.abs(F.col(prediction_col) - F.col(label_col))).alias("absolute_error_sum"),
        F.sum(F.col(label_col)).alias("actual_sum"),
        F.sum(F.col(prediction_col) - F.col(label_col)).alias("signed_error_sum"),
        F.avg(F.when(F.col(prediction_col) < 0, F.lit(1.0)).otherwise(F.lit(0.0))).alias("clamp_rate"),
    ).first()
    warnings = []
    actual_sum = float(row.actual_sum or 0.0)
    if actual_sum == 0.0:
        wape = None
        signed_bias = None
        warnings.append("WAPE undefined when total actual demand is zero")
    else:
        wape = float(row.absolute_error_sum) / actual_sum
        signed_bias = float(row.signed_error_sum) / actual_sum
    return RegressionReport(
        row_count=int(row.row_count),
        mae=float(row.mae) if row.mae is not None else None,
        rmse=float(row.rmse) if row.rmse is not None else None,
        wape=wape,
        signed_bias=signed_bias,
        clamp_rate=float(row.clamp_rate) if row.clamp_rate is not None else None,
        warnings=warnings,
    )


def top_k_recall(frame, prediction_col: str, *, k: int = 10, label_col: str = "label", id_col: str = "row_id") -> float:
    if k <= 0:
        raise ValueError("k must be positive")
    if id_col not in frame.columns:
        raise ValueError(f"top_k_recall requires an id column named {id_col!r}")

    actual = {
        row[id_col]
        for row in frame.orderBy(frame[label_col].desc(), frame[id_col].asc()).select(id_col).limit(k).collect()
    }
    predicted = {
        row[id_col]
        for row in frame.orderBy(frame[prediction_col].desc(), frame[id_col].asc()).select(id_col).limit(k).collect()
    }
    if not actual:
        return math.nan
    return len(actual & predicted) / len(actual)


def score_by_group(frame, group_cols: list[str], prediction_col: str, *, label_col: str = "label"):
    from pyspark.sql import functions as F

    grouped = frame.groupBy(*group_cols).agg(
        F.count(F.lit(1)).alias("row_count"),
        F.avg(F.abs(F.col(prediction_col) - F.col(label_col))).alias("mae"),
        F.sqrt(F.avg(F.pow(F.col(prediction_col) - F.col(label_col), F.lit(2.0)))).alias("rmse"),
        F.sum(F.abs(F.col(prediction_col) - F.col(label_col))).alias("absolute_error_sum"),
        F.sum(F.col(label_col)).alias("actual_sum"),
        F.sum(F.col(prediction_col) - F.col(label_col)).alias("signed_error_sum"),
        F.avg(F.when(F.col(prediction_col) < 0, F.lit(1.0)).otherwise(F.lit(0.0))).alias("clamp_rate"),
    )
    return (
        grouped.withColumn(
            "wape",
            F.when(F.col("actual_sum") == 0, F.lit(None).cast("double")).otherwise(
                F.col("absolute_error_sum") / F.col("actual_sum")
            ),
        )
        .withColumn(
            "signed_bias",
            F.when(F.col("actual_sum") == 0, F.lit(None).cast("double")).otherwise(
                F.col("signed_error_sum") / F.col("actual_sum")
            ),
        )
        .select(*group_cols, "row_count", "mae", "rmse", "wape", "signed_bias", "clamp_rate")
    )


def score_per_zone(frame, prediction_col: str, *, zone_col: str = "pickup_zone_id", label_col: str = "label"):
    return score_by_group(frame, [zone_col], prediction_col, label_col=label_col)


def score_by_borough(
    frame,
    prediction_col: str,
    *,
    zone_lookup=None,
    zone_col: str = "pickup_zone_id",
    borough_col: str = "borough",
    label_col: str = "label",
):
    if borough_col in frame.columns:
        report_frame = frame
    elif zone_lookup is not None:
        report_frame = frame.join(zone_lookup.select(zone_col, borough_col), on=zone_col, how="left")
    else:
        raise ValueError("score_by_borough requires a borough column or a zone_lookup DataFrame")
    return score_by_group(report_frame, [borough_col], prediction_col, label_col=label_col)


def score_demand_deciles(
    frame,
    prediction_col: str,
    *,
    label_col: str = "label",
    decile_col: str = "demand_decile",
):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    ranked = frame.withColumn(decile_col, F.ntile(10).over(Window.orderBy(F.col(label_col).asc())))
    return score_by_group(ranked, [decile_col], prediction_col, label_col=label_col)


def score_peak_windows(
    frame,
    prediction_col: str,
    *,
    k: int = 10,
    window_col: str = "forecast_window_start",
    id_col: str = "pickup_zone_id",
    label_col: str = "label",
):
    if k <= 0:
        raise ValueError("k must be positive")

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    actual_rank = Window.partitionBy(window_col).orderBy(F.col(label_col).desc(), F.col(id_col).asc())
    predicted_rank = Window.partitionBy(window_col).orderBy(F.col(prediction_col).desc(), F.col(id_col).asc())
    ranked = (
        frame.withColumn("_actual_rank", F.row_number().over(actual_rank))
        .withColumn("_predicted_rank", F.row_number().over(predicted_rank))
        .withColumn("_actual_top", F.col("_actual_rank") <= F.lit(k))
        .withColumn("_predicted_top", F.col("_predicted_rank") <= F.lit(k))
    )
    return (
        ranked.groupBy(window_col)
        .agg(
            F.sum(F.when(F.col("_actual_top"), F.lit(1.0)).otherwise(F.lit(0.0))).alias("actual_top_count"),
            F.sum(
                F.when(F.col("_actual_top") & F.col("_predicted_top"), F.lit(1.0)).otherwise(F.lit(0.0))
            ).alias("overlap_count"),
        )
        .withColumn("top_k_recall", F.col("overlap_count") / F.col("actual_top_count"))
        .select(window_col, "actual_top_count", "overlap_count", "top_k_recall")
    )
