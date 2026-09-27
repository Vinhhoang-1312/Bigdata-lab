# SDD ledger - plan: docs/superpowers/plans/2026-09-27-ml-workspace-and-training-information.md

Setup: worktree verified at `C:\Users\DELL\Documents\Big Data\.tmp\team-merge-20260926`; branch `integration/team-merge-20260926`; implementation baseline `24a9638`.

Ruling: Unix `sdd-workspace` and task scripts cannot execute in this Windows environment because Bash/Service/CreateInstance returns `E_ACCESSDENIED`; use the same workspace layout and ledger contract manually with PowerShell, preserving the plan's task order and evidence requirements. Cost if wrong: only scratch bookkeeping differs; implementation and git history remain unchanged.

Pre-flight shared interfaces:

- Task 1 -> Task 2: pure feature/scaling/nearest-center functions are consumed by artifact scoring; names and input dimensions match.
- Task 2 -> Task 3: cached anomaly snapshot and `score_trip` are consumed by information aggregation; no conflict.
- Task 2 -> Task 6: POST score contract is consumed by React form; request and response fields match the spec.
- Task 3 -> Task 7: information response keys are consumed by the React information view; availability and research-only semantics match.
- Task 4 -> Task 8: canonical routes and legacy aliases are consumed by navigation/cache tests; no conflict.
- Task 5 -> Tasks 6-7: typed API contracts are consumed by both UI views; no conflict.
- Task 6 -> Task 8: K-means mode/map behavior is consumed by regression tests; no conflict.

Ruling: Kafka infrastructure is present and verified by existing backend/smoke evidence, but React has no Kafka observability view; this is outside the current ML plan and remains a separate streaming UI gap. Cost if wrong: Kafka UI work is deferred until explicitly scoped.

Ruling: The full Python suite currently has four baseline failures unrelated to Task 1: the locked image lacks `pyspark`/`pytest`, and unchanged source/test contracts lack `regression_metrics` and `inverse_quantiles`; Task 1 focused suite is the valid scoped gate while these are carried to final verification. Cost if wrong: a later full-suite gate may require an environment or baseline repair before release.

Ruling: Task 2 exposed the same baseline serving defects and incomplete test runtime, so the smallest root fixes are included before the full gate: add `inverse_quantiles`, add `regression_metrics`, expose Spark's Python path to `ml-api`, and install the declared pytest dev dependency in the Spark image. Cost if wrong: the image rebuild adds time and size, but without it the repository's own tests cannot execute honestly.

Task 1: complete (commits 0ae7975..af9cde9, tests: focused `test_trip_anomaly_model.py` -> 7/7 pass; full suite baseline blockers recorded)

Task 2: complete (commit 2c1ace4; focused pytest 21 passed; full unittest suite 84 tests OK; baseline runtime/serving contracts repaired)

Task 3: complete (commit 2ba0415; focused builder/endpoint tests pass; full unittest suite 88 tests OK)

Merge: complete (fetched origin/urbanflow-merge; merged new commits 69342ff and dd6fe0b as 4968789; preserved React/no-Streamlit files in conflicts; restored Task 4 work)

Task 4: complete (commit d50f292; TypeScript build pass; focused Playwright route tests 2/2 pass)

Task 5: complete (commit b9633a3; Vitest 8/8 pass; TypeScript build pass)

Task 6: complete (commit 39c22b6; Vitest 8/8, TypeScript/build pass; desktop and mobile Playwright anomaly/K-means suites 3/3 pass each; mobile screenshot reviewed with no horizontal overflow)
