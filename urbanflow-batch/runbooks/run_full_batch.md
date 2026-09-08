# Run the Complete Independent Batch Pipeline

Run commands from the repository root.

## 1. Start infrastructure and validate Foundation

```bash
docker compose up -d
docker compose run --rm foundation-check
```

## 2. Acquire the frozen source periods

```bash
docker compose run --rm batch-python
```

This downloads January and February 2023 plus the zone lookup under ignored
`local-data/nyc-tlc/`.

## 3. Land original files in Bronze

```bash
docker compose run --rm spark-submit \
  --master 'local[2]' \
  /workspace/urbanflow-batch/spark/land_bronze.py \
  --period 2023-01 --period 2023-02
```

## 4. Run all fixture contract tests

```bash
docker compose run --rm spark-submit \
  --master 'local[2]' \
  /workspace/urbanflow-batch/tests/run_fixture_tests.py

docker compose run --rm spark-submit \
  --master 'local[2]' \
  /workspace/urbanflow-batch/tests/run_baseline_tests.py
```

## 5. Build canonical Silver on both workers

```bash
docker compose run --rm spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m --total-executor-cores 4 \
  /workspace/urbanflow-batch/spark/bronze_to_silver.py \
  --config /workspace/urbanflow-batch/config/settings.integration.yaml
```

## 6. Demonstrate Spark API equivalence

```bash
docker compose run --rm spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m --total-executor-cores 4 \
  /workspace/urbanflow-batch/spark/spark_api_demo.py
```

## 7. Build canonical Historical Gold

```bash
docker compose run --rm spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m --total-executor-cores 4 \
  --conf spark.sql.shuffle.partitions=48 \
  /workspace/urbanflow-batch/spark/build_gold_baseline.py \
  --start-date 2023-01-01 --end-date 2023-01-31
```

## 8. Verify canonical outputs and data split

```bash
docker compose run --rm spark-submit \
  --master 'local[2]' \
  --conf spark.sql.shuffle.partitions=32 \
  /workspace/urbanflow-batch/tests/verify_integrated_outputs.py
```

Expected final result:

```text
PERIODS NON_OVERLAPPING = PASS
CANONICAL SILVER = PASS
CANONICAL HISTORICAL GOLD = PASS
INTEGRATED BATCH OUTPUTS = PASS
```
