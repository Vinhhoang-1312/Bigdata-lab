# Black-box audit: Streaming and ML

**Date:** 2026-09-21
**Scope:** user-facing Streaming dashboard/API and the durable distributed-ML
training evidence currently stored in SeaweedFS.

## Method

The browser checks run through the external `AI-QE_AutoTest` Playwright runner
using installed Microsoft Edge. They treat the dashboard as a black box and do
not import application modules. The audit covers desktop, tablet, and mobile;
normal and unavailable-Gold paths; tab persistence; responsive layout; and the
read-only HTTP API.

## Streaming result

| Check | Result | Evidence |
| --- | --- | --- |
| Live Gold dashboard on desktop, tablet, and mobile | PASS | Each viewport loads Integrated Gold and completes all four workflows. |
| No automatic tab reset after 12.5 seconds | PASS | The selected comparison tab remains selected beyond the former five-second refresh interval. |
| Mobile workflow tabs are fully visible | PASS | All four tab boxes stay within the 375 x 812 viewport. |
| Missing Gold is visible to the user | PASS | The dashboard renders a clear Gold-read error rather than a blank result. |
| Configured API overview and health | PASS | HTTP 200 with non-empty live rows. |
| Missing API configuration | PASS | HTTP 503 with an actionable configuration message. |
| Live Gold storage failure | PASS | HTTP 503 with the generic `Gold data is temporarily unavailable` message. |

The three viewport checks must run serially. When desktop, tablet, and mobile
all cold-start simultaneously, their independent S3 reads can exceed the
10-second browser wait. A serial rerun passed for tablet and mobile, so this is
a test-load contention limit, not a reproducible single-user rendering defect.

## Fixes made during the audit

- Gold-reader failures in the Streaming API now become HTTP 503 instead of an
  unhandled HTTP 500.
- The dashboard can read the full Bronze zone lookup through SeaweedFS/S3A,
  so live zones show real names such as `50 Clinton West`.
- On small screens, the four workflow tabs remain inside the viewport.
- The README now includes the required SeaweedFS configuration for the
  dashboard and API.

## ML result

The model artifact is a GTQN-Lite probabilistic demand forecaster trained with
PyTorch DDP/Gloo through Spark `TorchDistributor`; it is not a Transformer.
The actual label is the observed count of pickups in a zone during the next
30-minute bucket. It predicts P10, P50, and P90 forecasts.

| Check | Result | Evidence |
| --- | --- | --- |
| Core GTQN, checkpoint, metrics, and DDP contracts | PASS | 25 tests passed in the production Spark image, including two Gloo ranks with synchronized parameters. |
| Tensor artifact schema and temporal split | PASS | Spark/S3A verification found 10,176 time buckets, 265 zones, calendar features, adjacency, and disjoint train/validation/test ranges. |
| Distributed runtime configuration | PASS | Two Spark executors were allocated for the Spark verification; DDP contract tests cover two synchronized Gloo ranks. |
| Durable training checkpoint | PARTIAL | SeaweedFS contains checkpoints through epoch 13 for `gtqn-quality-20260919-001`. |
| Held-out test metrics and final model selection | NOT COMPLETE | No `best.pt`, `result.json`, or `test_metrics.json` exists for this run. No final accuracy, MAE, WAPE, or calibration claim is valid yet. |

The quality run stopped after Spark removed a worker that had missed heartbeats
for roughly 37 minutes. Checkpoints are durable and the trainer supports
`--resume`, but current orchestration does not automatically relaunch a failed
barrier job. This is the remaining operational issue before a complete ML
result can be claimed.

## Limits

- Browser audit proves observed user workflows, not correctness for every
  possible stream record. Foundation fixtures and Spark contract tests cover
  deterministic data rules separately.
- ML has not passed its held-out test stage. Resuming training and producing
  the final test report is required before presenting model quality to users.
