# UrbanFlow ML Workspace And Training Information Spec

## 1. Outcome

Hoan thien khu vuc Machine Learning thanh ba workspace doc lap, de nguoi dung co
the dung model, hieu model, va kiem tra bang chung huan luyen ma khong nham lan
giua du bao, phat hien bat thuong, va trang thai he thong.

Canonical routes:

- `/ml`: Demand Forecast.
- `/ml/kmeans`: K-means Anomaly.
- `/ml/information`: Training Information.

Legacy routes `/ml/anomalies` va `/ml/evidence` phai tiep tuc mo dung noi dung
tuong ung, nhung navigation moi chi sinh canonical routes.

## 2. Non-goals

- Khong train lai GTQN-Lite hoac K-means.
- Khong thay doi Foundation 1.0.0 contracts.
- Khong goi Spark cho moi HTTP request.
- Khong bien anomaly score thanh fraud probability hoac classification accuracy.
- Khong tu dong mean/mode imputation cho distance, duration hoac fare.
- Khong bien Training Information thanh trang giam sat live Spark cluster.
- Khong khoi phuc Streamlit.

## 3. Existing Truth

### Demand Forecast

- Model: GTQN-Lite, supervised quantile regression; khong phai Transformer.
- Input: 336 half-hour buckets, calendar features, observed mask, fixed/adaptive
  zone graph tuy variant.
- Output: P10, P50, P90 cho pickup count cua zone trong bucket 30 phut tiep theo.
- Distributed runtime: Spark `TorchDistributor` khoi tao hai PyTorch DDP ranks;
  Gloo dong bo gradient va model parameters.
- Durable evidence: SeaweedFS checkpoint, config hash, dataset fingerprint,
  rank/host/batch/checksum evidence.
- Current use status: research only. Weekly-naive baseline hien tot hon GTQN run.

### K-means Anomaly

- Model: Spark MLlib K-means, `k=8`, `maxIter=12`, seed `20260926`.
- Train/calibrate/screen periods: November 2022 / January 2023 / February 2023.
- Features: `log1p(distance)`, `log1p(duration)`, `log1p(fare)`,
  `log1p(average speed)`; Spark StandardScaler mean/std duoc fit tren train sample.
- Score: Euclidean distance toi nearest learned center sau khi standardization.
- Review threshold: calibration score percentile 99.5.
- No labels: precision, recall, fraud probability va accuracy khong ton tai.
- Completed artifact da luu scaler mean/std, cluster centers, threshold, features,
  worker hosts va review sample; API co the score trong memory ma khong chay Spark.

## 4. Product Structure

### 4.1 Demand Forecast

Giu workflow hien tai: chon zone va time bucket, chay historical forecast, so sanh
P10/P50/P90 voi observed value va weekly baseline. Khong mo rong scope trong dot nay.

### 4.2 K-means Anomaly

Khu vuc nay co hai mode noi bo:

1. `Review queue`: giu danh sach February sample va ban do hien tai.
2. `Check a trip`: form de score mot chuyen di moi bang completed artifact.

Input form:

- Pickup zone va drop-off zone: bat buoc de show map/context, nhung phai noi ro
  hai gia tri nay khong nam trong feature vector hien tai.
- Distance miles: finite, `> 0`.
- Duration minutes: finite, `> 0`.
- Fare USD: finite, `>= 0`.
- Average speed: API tu tinh; user khong nhap.

Output:

- `score`, `threshold`, `review_required`.
- `nearest_cluster` dung zero-based cluster index.
- `reason` dua tren standardized feature lech lon nhat.
- Input recap, run ID, method, feature disclosure va research-only warning.
- Map pickup-to-dropoff theo zone centers; khong mo phong GPS route.

The API must return HTTP 422 for invalid numeric inputs and HTTP 503 when a
complete K-means artifact is unavailable. It must never silently fall back to
an arbitrary threshold or centers.

## 5. Training Information

Day la workspace doc lap, khong nam trong K-means. No trinh bay hai ML workflows
song song va phai tach `training-time evidence` khoi `current service status`.

### 5.1 Pipeline diagram

Demand pipeline:

```text
Silver -> quality policy -> zero-inclusive demand tensor -> features/scaler
       -> chronological split -> Spark TorchDistributor
       -> Worker/rank 0 <-> Gloo all-reduce <-> Worker/rank 1
       -> SeaweedFS checkpoint/evidence -> ML API -> Forecast UI
```

Anomaly pipeline:

```text
Silver -> eligible-trip filter -> log features -> StandardScaler
       -> Spark MLlib K-means -> January threshold calibration
       -> February screening/artifact -> ML API -> Review/Check UI
```

Use semantic HTML/CSS and Lucide icons. Do not use a decorative SVG. At mobile
width the diagram becomes a vertical flow without horizontal overflow.

### 5.2 Evidence shown from artifacts

Demand training:

- run ID, use status, benchmark status, parameter count.
- best epoch, training end epoch.
- DDP status, world size, ranks, hosts, batches, distinct host count.
- whether parameter checksums match; show only a short checksum prefix.
- dataset fingerprint/config evidence availability.
- held-out metrics and weekly baseline comparison.

K-means training:

- run ID, `k`, threshold and percentile.
- train/calibration/screen periods and sample counts.
- feature names and worker hosts.
- limitations and artifact availability.

No UI label may say that two workers are currently running. It may say that a
completed run contains evidence from two distinct workers only when artifact
data proves it.

### 5.3 Preprocessing and missing-data policy

Show an explicit table with `Input`, `Policy`, `Why`, and `Evidence`:

- Required Silver fields: invalid/null rows are rejected upstream; no silent
  imputation.
- Demand zero: keep zero when a bucket was observed and a zone had no pickups.
- Source gap: retain zero count plus `observed=false`; model receives the mask.
- Seasonal lag unavailable: numeric lag becomes zero and a missing indicator is
  added.
- GTQN scaling: `log1p`, then robust per-zone median/IQR fit on training split.
- K-means required features: non-positive distance/duration and negative fare
  are ineligible; no mean/mode fill.
- K-means scaling: `log1p`, then StandardScaler fit on November train sample.
- Time splits are chronological to prevent future leakage.

The UI must explain that mean/mode imputation is intentionally not the default:
it would invent trip measurements, distort clusters, and hide source gaps.

### 5.4 Technique glossary

For each technique, use short B1-B2 English copy with these fields:

- `Type`: what kind of technique it is.
- `Owner/ecosystem`: Spark, PyTorch, or UrbanFlow.
- `Purpose`: what problem it solves.
- `How`: one short mechanism statement.
- `Alternative`: nearest comparable method.
- `Limit`: what it does not prove.

Required entries: Spark MLlib K-means, StandardScaler, GTQN-Lite, quantile
regression, DDP, Gloo all-reduce, TorchDistributor, RankPartitionSampler,
AdamW, gradient clipping, gradient accumulation, dropout, early stopping,
ReduceLROnPlateau, checkpoint/resume, observed mask, dataset fingerprint.

## 6. API Contracts

### `POST /v1/anomaly/score`

Request:

```json
{
  "pickup_zone_id": 43,
  "dropoff_zone_id": 161,
  "trip_distance_miles": 4.2,
  "trip_duration_minutes": 18.0,
  "fare_amount_usd": 22.5
}
```

Response:

```json
{
  "run_id": "trip-anomaly-20260926-001",
  "method": "spark_mllib_kmeans",
  "score": 1.23,
  "threshold": 2.4,
  "review_required": false,
  "nearest_cluster": 3,
  "reason": "Fare above learned pattern",
  "derived_speed_mph": 14.0,
  "features_used": ["trip_distance", "trip_duration_minutes", "fare_amount", "average_speed_mph"],
  "use_status": "research_only"
}
```

### `GET /v1/ml/information`

The response has stable top-level keys:

```text
use_status
demand_forecast
kmeans_anomaly
preprocessing
techniques
limitations
```

Each model section has `availability`. Missing anomaly evidence degrades only
`kmeans_anomaly`; it does not remove demand information. Missing demand model
artifact continues to return HTTP 503 because the current ML API cannot start
without it; changing startup architecture is outside this scope.

## 7. UX And Reliability

- Manual refresh only; no polling or timed page reload.
- Keep last cached resource when refresh fails and show a visible warning.
- Every page has loading, empty, degraded and success states.
- No horizontal page overflow at 1440x900 or 390x844.
- Numeric values have units; no bare score is described as a percentage.
- Use English B1-B2 UI labels and short explanations.
- Preserve CARTO/MapLibre local fallback and recenter behavior.
- No secret or `.env` value is exposed in the browser or committed.

## 8. Acceptance Criteria

1. All three canonical routes survive reload and browser Back.
2. Legacy ML URLs still render the correct workspace.
3. A valid trip can be scored without starting a Spark job.
4. Invalid trip values are rejected visibly.
5. K-means UI never claims fraud, accuracy, precision or probability.
6. Information diagram displays artifact-backed two-worker evidence and labels
   it as training-time evidence.
7. Missing-data table matches actual code behavior.
8. Forecast behavior and existing streaming UI do not regress.
9. Python tests, TypeScript build, Vitest and desktop/mobile Playwright pass.
10. README documents routes, meanings, limitations and verification commands.
