# UrbanFlow Distributed Spatio-Temporal Forecast Design

## Decision

UrbanFlow will keep the existing Spark MLlib GBT model as a classical
distributed baseline. The main research model will be **GTQN-Lite**, a Graph
Temporal Quantile Network trained with PyTorch DistributedDataParallel (DDP)
through Spark `TorchDistributor`.

The model solves one primary task: predict taxi pickup demand for every NYC
taxi zone in the next 30-minute bucket. A useful anomaly signal is derived from
the same forecast: an observation is unusual when it falls outside the model's
P10-P90 prediction interval or has a large calibrated residual. It is not a
second unrelated model.

## Why This Is Master-Level Work

This is not made advanced by adding many layers. Its research depth comes from
five connected questions:

1. Does a learned zone graph improve forecasts over independent-zone models?
2. Does a temporal convolution capture daily and weekly demand patterns better
   than hand-built lags alone?
3. Are uncertainty intervals calibrated, not only point forecasts accurate?
4. Does two-worker DDP produce numerically consistent training and a measurable
   distributed execution trace?
5. Do the gains remain on a future, untouched test period and across borough,
   zone-demand decile, peak/off-peak, and sparse-zone slices?

The answer must be supported by baselines, ablations, chronological validation,
reproducible artifacts, and executor/rank evidence. Model complexity alone is
not accepted as evidence.

## Scope

### Included

- Canonical Silver data at `s3a://urbanflow/silver/trips/`.
- A zero-inclusive 30-minute zone demand matrix.
- One-step-ahead forecast for all 265 zones.
- Historical mean, seasonal naive, and Spark MLlib GBT baselines.
- GTQN-Lite point and probabilistic forecasts at P10, P50, and P90.
- Fixed OD-flow graph plus a learned adaptive graph.
- Spark `TorchDistributor` with two non-driver PyTorch DDP ranks.
- Early stopping, regularization, gradient clipping, checkpointing, and fixed
  random seeds.
- Ablations for no graph, fixed graph only, and no quantile interval.
- Quality, calibration, distribution, and reproducibility reports.

### Excluded

- A general-purpose foundation model or language model.
- A large Transformer. The available series is too small and the current CPU
  cluster is too constrained to justify it.
- Real-time online weight updates.
- Claims that waiting time, fuel use, or emissions were reduced. The project
  has no vehicle-operation outcome data to prove those effects.
- Promotion into Foundation 1.0.0 contracts. Outputs remain under
  `s3a://urbanflow/dev/ml/` until change control approves a new contract.

## Data Contract and Splits

All time logic uses `America/New_York`. A sample at bucket `t` consumes only
values available before `t` and predicts the trip count in bucket `t`.

| Period | Purpose | Rule |
|---|---|---|
| 2022-08-01 to 2022-08-07 | history warm-up | feature context only |
| 2022-08-08 to 2022-12-31 | train | optimizer updates |
| 2023-01-01 to 2023-01-31 | validation | early stopping and model selection |
| 2023-02-01 to 2023-02-28 | test | exactly one final evaluation |

The present Silver audit contains only 63 October and 82 December trips. Those
months are incomplete, so the final report must expose both results:

- `current-data` result: reproducible on the available Silver snapshot;
- `complete-data` result: required for a defensible final academic claim after
  full October and December raw data are loaded and Silver is rebuilt.

The implementation may complete on `current-data`, but QUALITY-GO cannot be
called publication-ready while the two months remain incomplete.

## Tensor Dataset

The Spark preparation job writes a compact, ordered artifact:

- `counts`: `[time, zone]`, non-negative float32;
- `observed`: `[time, zone]`, boolean data-presence mask;
- `calendar`: `[time, feature]` for cyclical half-hour, weekday, weekend, and
  month features;
- `zone_ids`: `[zone]`, stable integer zone order;
- `fixed_adjacency`: `[zone, zone]`, row-normalized directed OD-flow graph
  built from training trips only;
- `timestamps`: RFC 3339 bucket starts;
- split boundaries and a dataset fingerprint.

Each training example uses 336 half-hour buckets, exactly seven days of
history. The target is the next bucket for all zones. Workers receive the same
compact matrix and use `DistributedSampler` to process disjoint sample indices.
No 336-step windows are materialized to disk.

## GTQN-Lite Architecture

Input is `[batch, zone, history, feature]`.

1. **Robust scaling**: per-zone `log1p` followed by train-only median/IQR
   scaling. Validation and test statistics never enter the scaler.
2. **Temporal encoder**: a shared gated dilated temporal convolution processes
   each zone. Dilations cover short, daily, and weekly context with residual
   connections and dropout.
3. **Zone embeddings**: two trainable embedding tables create a directed
   adaptive adjacency matrix with `softmax(relu(E1 @ E2^T))`.
4. **Graph fusion**: one diffusion layer combines self information, the fixed
   OD graph, and the adaptive graph. A learned gate controls how much graph
   context each zone uses.
5. **Quantile head**: outputs P10, P50, and P90 for every zone. Softplus deltas
   enforce `P10 <= P50 <= P90` by construction.

Default capacity is deliberately small: hidden size 32, zone embedding size
16, one graph-fusion layer, and dropout 0.2. This is large enough to test the
hypothesis and small enough for CPU DDP.

## Loss and Optimization

- Objective: mean pinball loss over quantiles 0.1, 0.5, and 0.9.
- Extra emphasis: P50 weight 2.0 so the median forecast remains useful.
- Optimizer: AdamW, learning rate `1e-3`, weight decay `1e-4`.
- Scheduler: reduce learning rate on validation pinball plateau.
- Gradient clipping: global norm 1.0.
- Early stopping: patience 8, maximum 60 epochs.
- Seed: 20260917 for Python, NumPy, PyTorch, sampler, and model initialization.
- Selection metric: validation WAPE, with validation pinball as tie-breaker.

## Distributed Training

Spark is the orchestration and data-engine layer. PyTorch is the neural
optimization layer.

`TorchDistributor(num_processes=2, local_mode=False, use_gpu=False)` launches
two executor-side ranks. Every rank owns a full model replica and a disjoint
part of each epoch. During `backward()`, DDP performs gradient all-reduce and
both ranks apply the same optimizer update.

Distribution proof requires all of the following:

- world size equals 2;
- rank metadata contains two different Spark executor hosts;
- every rank processes at least one batch;
- a parameter checksum gathered after training is identical across ranks;
- Spark event logs show successful non-driver tasks on at least two executors;
- the rank-0 checkpoint and run metadata are written successfully.

The proof is about distributed optimization. A local two-process run is only a
smoke test and cannot pass DISTRIBUTION-GO.

## Evaluation

Point metrics use P50 after inverse scaling and non-negative clamping:

- MAE, RMSE, WAPE, signed bias;
- top-k zone recall for operational positioning;
- metrics by borough, zone-demand decile, peak/off-peak, weekday/weekend;
- per-zone error distribution, not only a global average.

Probabilistic metrics:

- mean pinball loss at P10/P50/P90;
- P10-P90 empirical coverage, target 80%;
- mean interval width;
- coverage by demand decile to expose weak sparse-zone calibration.

Anomaly output contains zone, bucket, actual, P10, P50, P90, residual, interval
breach direction, and severity. Without labeled incidents, this is an anomaly
candidate list, not a proven incident detector.

## Baselines and Ablations

The final comparison table contains:

1. Historical mean.
2. Seasonal naive, same zone and same slot one week earlier.
3. Spark MLlib GBT using leakage-safe lag and calendar features.
4. GTQN-Lite without graph fusion.
5. GTQN-Lite with fixed OD graph only.
6. Full GTQN-Lite with fixed plus adaptive graph and quantile head.

The full model is accepted only if it beats seasonal naive and GBT on test WAPE
or provides a material calibrated-uncertainty advantage documented in the
report. An advanced model that does not add measurable value remains a
research result, not a production recommendation.

## Acceptance Gates

| Gate | Pass condition |
|---|---|
| DATA-GO | schema valid, 265-zone grid, split coverage recorded, incomplete months visible |
| TENSOR-GO | ordered artifact validates shapes, no NaN/Inf, non-negative counts, train-only graph/scaler |
| MODEL-GO | forward/backward tests pass, ordered quantiles, finite gradients, checkpoint reload parity |
| DISTRIBUTION-GO | two executor ranks, equal post-train checksum, event-log proof, no failed stage |
| QUALITY-GO | untouched test metrics and baseline/ablation table produced; decision rule applied honestly |
| REPRO-GO | config, Git SHA, dataset fingerprint, seeds, versions, timing, and paths recorded |

## Failure Handling

- OOM: reduce batch size first, then hidden size from 32 to 16; never shorten
  the chronological test period to hide the problem.
- DDP hang: use equal sampler lengths with `drop_last`, fixed batch counts, and
  fail-fast rank diagnostics.
- Sparse/missing months: continue only with a visible data-quality flag and do
  not call the result publication-ready.
- Quantile collapse: inspect interval width and coverage; increase calibration
  penalty only through validation, never through test feedback.
- Full model loses to baselines: report the negative result and recommend the
  best baseline for deployment.
- Worker loss: fail the run; do not publish a checkpoint from a partial world.

## Operational Outputs

Each run uses an immutable run ID under `s3a://urbanflow/dev/ml/runs/<run-id>/`
and records:

- tensor metadata and dataset fingerprint;
- config snapshot and dependency versions;
- best checkpoint and epoch history;
- validation/test predictions;
- baseline and ablation metrics;
- anomaly candidates;
- DDP rank proof and Spark event-log summary;
- model card and final go/no-go decision.

Large generated artifacts, checkpoints, and event logs are ignored by Git.
Only code, tests, configuration examples, and concise Markdown evidence are
committed.
