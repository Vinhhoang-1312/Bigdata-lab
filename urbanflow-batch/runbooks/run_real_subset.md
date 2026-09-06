# Run a Small Real NYC TLC Subset

This smoke test uses January 2023 Yellow Taxi data. Downloaded files are stored
under the Git-ignored `local-data/` directory.

```bash
mkdir -p local-data/nyc-tlc
curl -fL --retry 3 -o local-data/nyc-tlc/yellow_tripdata_2023-01.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
curl -fL --retry 3 -o local-data/nyc-tlc/taxi_zone_lookup.csv \
  https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Start infrastructure and submit the job to both Spark workers:

```bash
docker compose up -d
docker compose run --rm spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m \
  --total-executor-cores 4 \
  /workspace/urbanflow-batch/spark/bronze_to_silver.py \
  --config /workspace/urbanflow-batch/config/settings.real-smoke.json
```

The smoke-test output is isolated from the canonical integration path:

```text
s3a://urbanflow/dev/batch/real-smoke/silver/trips/
s3a://urbanflow/dev/batch/real-smoke/rejected/trips/
```

Record observed source, valid, rejected, output row counts and runtime before
adding any new real-data cleaning rule. Do not change Foundation rules merely
to increase the accepted-row count.
