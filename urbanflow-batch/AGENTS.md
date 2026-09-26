# AGENTS.md — `urbanflow-batch`

The root `AGENTS.md` also applies. This file adds Batch-specific guidance.

## Scope and boundaries

Batch owns:

```text
NYC TLC source → Bronze → Silver → Historical Gold
```

It does not own Kafka production, replay pacing, Structured Streaming, Live
Gold, or dashboard behavior. Do not import Replay or Streaming implementation.

Authoritative contracts:

- `../urbanflow-foundation/contracts/silver-trips.yaml`
- `../urbanflow-foundation/contracts/historical-zone-baseline.yaml`
- `../urbanflow-foundation/contracts/time-semantics.md`
- `../urbanflow-foundation/contracts/storage-paths.yaml`
- `../urbanflow-foundation/config/data-periods-reference.yaml`

Frozen real-data periods:

- Historical: `2023-01-01` through `2023-01-31`.
- Replay holdout: `2023-02-01` through `2023-02-28`.

## Contract outputs

- Silver: `s3a://urbanflow/silver/trips/`, partitioned by `year/month`.
- Historical Gold: `s3a://urbanflow/gold/historical_zone_baseline/`.

Fixture and smoke outputs belong under `s3a://urbanflow/dev/batch/`; do not
replace canonical outputs with fixture data.

Silver must preserve the exact 14-field Foundation order/types and derive time
in `America/New_York`. Historical Gold must build per-date counts over the
complete eligible zone/date/hour/slot grid, fill absent counts with zero, and
use Spark `stddev_pop`.

## Code organization

- `batch/io.py` — CSV and mixed-month Parquet adapters.
- `batch/transforms.py` — Bronze → Silver rules and rejection reasons.
- `batch/validation.py` — Silver contract assertions.
- `batch/baseline.py` — zero-inclusive Historical Gold construction.
- `batch/baseline_validation.py` — Historical Gold assertions.
- `batch/source_acquisition.py` — standard-library source download.
- `spark/` — executable Spark jobs only.
- `tests/` — Batch contract and persisted-output tests.
- `config/` — non-secret JSON profiles.
- `runbooks/` and `docs/results/` — reproducible commands and evidence.

Keep transformations reusable and DataFrame-based. RDD exists only for the
required API-equivalence demonstration. Do not add undocumented thresholds or
silent deduplication. Preserve raw rejected values and reason codes.

NYC monthly Parquet schemas can drift (for example, `passenger_count` double vs
INT64). Normalize physical source types per file before unioning; never solve
source drift by changing the Silver contract.

## Required validation

From the repository root:

```bash
docker compose up -d
docker compose run --rm foundation-check

docker compose run --rm spark-submit \
  --master 'local[2]' \
  /workspace/urbanflow-batch/tests/run_fixture_tests.py

docker compose run --rm spark-submit \
  --master 'local[2]' \
  /workspace/urbanflow-batch/tests/run_baseline_tests.py

docker compose run --rm spark-submit \
  --master 'local[2]' \
  /workspace/urbanflow-batch/tests/verify_fixture_s3_output.py

docker compose run --rm spark-submit \
  --master 'local[2]' --conf spark.sql.shuffle.partitions=32 \
  /workspace/urbanflow-batch/tests/verify_integrated_outputs.py
```

Expected terminal markers:

```text
FOUNDATION FIXTURES = PASS
BATCH FIXTURE TESTS = PASS
HISTORICAL BASELINE TESTS = PASS
PERSISTED FIXTURE SILVER = PASS
INTEGRATED BATCH OUTPUTS = PASS
```

Use `runbooks/run_full_batch.md` for acquisition, Bronze landing, canonical
Silver, API equivalence, and Historical Gold commands.

## Completion boundary

Independent Batch is complete, and canonical February Silver → Replay → Kafka
passes without an ad-hoc adapter. Historical Gold → Streaming remains deferred.
When Streaming exists, test that real boundary before declaring integration
complete. Keep `docs/implementation-status.md`, cleaning decisions, measured
results, and runbooks current whenever Batch behavior changes.
