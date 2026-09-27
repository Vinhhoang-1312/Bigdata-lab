# Assignment 3 Requirement Checkpoint Audit

Checkpoint created before work: Git branch `checkpoint-before-assignment-3` from commit `d2cf72e`.
Working branch: `assignment-3-work`.

| Requirement | Evidence | Status |
|---|---|---|
| Delete old Assignment 2 plan/spec/audit markdown files | Removed old files under `docs/superpowers/plans`, `docs/superpowers/specs`, `reports/REPORT_OUTLINE.md`, `reports/REQUIREMENT_AUDIT.md` | DONE |
| Step 1 richer 20m features | `scripts/05_a3_features_v2.py`, `reports/a3_step1_features_v2_validation.json` | DONE |
| 50,000+ patches and 3+ countries | Step 1: 52,607 rows, 4 countries | DONE |
| Official split, no random split | All scripts use existing `split` column; no `randomSplit`, no CrossValidator/TrainValidationSplit | DONE |
| Train-only preprocessing inside Pipeline | Step 2 and Step 5 use Pipeline; Step 3/4 prep pipelines fit on train only | DONE |
| Step 2 exact-one top frequent class task | `scripts/06_a3_multiclass.py`, counts in `reports/a3_step2_multiclass.json` | DONE |
| Step 2 LR/RF/GBT-OneVsRest grids tuned on validation | `reports/a3_step2_multiclass.json` includes all grid rows and timing | DONE |
| Step 2 test metrics/confusion/per-class metrics | `reports/a3_step2_multiclass.json` | DONE |
| Step 2 compare with A2 baselines | `assignment2_baselines` section in `reports/a3_step2_multiclass.json` | DONE |
| Step 2 RF importances/LR coefficients/top-k retrain | `interpretation` and `top_k_retrain` in Step 2 JSON | DONE |
| Step 3 19 binary label columns | `scripts/07_a3_multilabel.py`, `reports/a3_step3_multilabel.json` | DONE |
| Step 3 binary relevance loop + threshold tuning | `reports/a3_step3_multilabel.json` per-class threshold grid | DONE |
| Step 3 test micro/macro/per-class F1 | `summary` and `per_class` in Step 3 JSON | DONE |
| Step 4 PCA train pipeline + plot | `reports/a3_step4_unsupervised.json`, `reports/figures/a3_pca_scatter.png` | DONE |
| Step 4 KMeans k=3..10 silhouette + contingency | `reports/a3_step4_unsupervised.json` | DONE |
| Step 5 scaling 25/50/100 and two partition counts | `reports/a3_step5_scalability_deployment.json` | DONE |
| Step 5 model persistence reload exact metrics | `reload_reproduces_test_metrics_exactly: true` | DONE |
| Step 5 batch inference held-out country partitioned by country | `hdfs_mirror/bigearthnet/predictions/country=Lithuania`, metrics in Step 5 JSON | DONE |
| Deliverable scripts/config/requirements/report | `scripts/05..09_a3_*.py`, `scripts/a3_config.py`, `requirements.txt`, `reports/a3_final_report.md` | DONE |

Known transparent limitations:

- Partial dataset has 18 of 19 labels and the exact-one single-label subset does not contain 35 usable train classes; scripts request 35 and report available classes/counts.
- Local Windows Spark required Hadoop native helper files in `.hadoop/bin`; `.hadoop/` is ignored from Git.
