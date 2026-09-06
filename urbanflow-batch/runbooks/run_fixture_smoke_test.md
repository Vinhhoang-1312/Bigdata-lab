# Run the Batch Fixture Smoke Test

From the repository root, start the Docker infrastructure:

```bash
docker compose up -d
```

Run the Batch contract tests:

```bash
docker compose run --rm spark-submit \
  --master 'local[2]' \
  /workspace/urbanflow-batch/tests/run_fixture_tests.py
```

Expected:

```text
BATCH FIXTURE TESTS = PASS
```

Run Bronze → Silver against the Foundation fixtures:

```bash
docker compose run --rm spark-submit \
  --master 'local[2]' \
  /workspace/urbanflow-batch/spark/bronze_to_silver.py \
  --config /workspace/urbanflow-batch/config/settings.fixture.json
```

Expected metrics:

```text
source_rows=15
valid_rows=10
rejected_rows=5
silver_rows=10
silver_readback_rows=10
BRONZE TO SILVER = PASS
```

Verify that persisted S3A Parquet exactly matches the Foundation fixture:

```bash
docker compose run --rm spark-submit \
  --master 'local[2]' \
  /workspace/urbanflow-batch/tests/verify_fixture_s3_output.py
```

Expected:

```text
PERSISTED FIXTURE SILVER = PASS
```

Fixture-mode output is isolated from integrated contract paths:

```text
s3a://urbanflow/dev/batch/silver/trips/
s3a://urbanflow/dev/batch/rejected/trips/
```

Use the integration configuration only after real Bronze data is available.
Credentials come from the Git-ignored root `.env`; never put them in Batch
configuration files.
