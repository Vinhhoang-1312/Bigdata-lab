# Trip anomaly MLlib run

Date: 2026-09-26. Run: `trip-anomaly-20260926-001`.

## Data and method

- Source: `s3a://urbanflow/silver/trips/` (NYC TLC Silver Parquet).
- Training: November 2022, 63,780 sampled eligible trips.
- Threshold calibration: January 2023, 59,877 sampled eligible trips.
- Separate screening: February 2023, 56,912 sampled eligible trips.
- Reproducible sample fraction: 2% per period, with fixed different seeds.
- Model: Spark MLlib K-means, 8 learned centers, standardized log distance,
  duration, fare and average speed.
- Review threshold: 2.40427, January score percentile 99.5.
- February records at or above threshold: 299; top 80 published to the UI.
- Distributed task hosts observed: `spark-worker-1`, `spark-worker-2`.

Artifact: `s3a://urbanflow/dev/ml/anomaly/runs/trip-anomaly-20260926-001/result.json`.
The manifest stores the scaler, cluster centers, threshold, sample sizes,
worker hosts, top trip measurements, scores and review reasons.

## Interpretation

One top-ranked record says 4,203.57 miles in 18 minutes. This is a strong
candidate for a source-data error. Its score does not establish misconduct.
The model has no ground-truth anomaly labels, so no accuracy, precision or
recall can be claimed. The map uses taxi-zone polygons and an approximate
pickup-zone to dropoff-zone line; no vehicle route coordinates exist in Silver.
The 2% sample does not cover every February trip.

The React page is `http://127.0.0.1:5173/ml/anomalies`. The read-only API is
`http://127.0.0.1:8002/v1/anomaly/trips`.
