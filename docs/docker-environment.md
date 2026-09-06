# UrbanFlow Docker Environment

The local development infrastructure is managed with Docker Compose so every
subproject can use the same pinned runtime versions and service addresses.

## Host requirements

- Docker Engine or Docker Desktop
- Docker Compose
- Git

Conda, Java, Spark, Python, and AWS CLI are not required on the host.

## First setup

```bash
cp .env.example .env
docker compose up -d --build
docker compose run --rm foundation-check
docker compose run --rm spark-submit
docker compose run --rm aws
```

Expected Spark/S3A output:

```text
WRITE = PASS
READ  = PASS
DATA  = PASS
```

The first `spark-submit` run downloads the pinned S3A dependencies into the
Compose-managed `spark-ivy` named volume. Later runs reuse them.

## Services

| Service | Address |
|---|---|
| Spark master | `spark://spark-master:7077` inside Docker; UI at http://localhost:8080 |
| Spark worker 1 | http://localhost:8081 |
| Spark worker 2 | http://localhost:8082 |
| SeaweedFS S3 | `http://seaweedfs:8333` inside Docker; http://localhost:8333 on host |
| SeaweedFS filer | http://localhost:8888 |
| SeaweedFS master | http://localhost:9333 |

Inside containers, always use Compose service names rather than `localhost`.

## Commands

Start infrastructure:

```bash
docker compose up -d
```

Inspect status and logs:

```bash
docker compose ps
docker compose logs -f spark-master spark-worker-1 spark-worker-2
docker compose logs -f seaweedfs
```

Download the frozen Batch source months with containerized Python 3.13:

```bash
docker compose run --rm batch-python
```

Run local Spark validation:

```bash
docker compose run --rm spark-submit \
  --master 'local[2]' /workspace/tests/infrastructure/spark_local_smoke.py
```

Run a job on the two-worker cluster:

```bash
docker compose run --rm spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m \
  --total-executor-cores 4 \
  /workspace/tests/infrastructure/spark_local_smoke.py
```

Run the Spark/SeaweedFS Parquet round trip:

```bash
docker compose run --rm spark-submit
```

List S3 buckets:

```bash
docker compose run --rm aws
```

Stop services while preserving data:

```bash
docker compose down
```

Delete services and persistent local data:

```bash
docker compose down --volumes
```

## Locked versions

- Foundation: 1.0.0
- Spark/PySpark: 4.2.0
- Java in Spark image: 21
- Scala: 2.13.18
- Hadoop: 3.5.0
- `hadoop-aws`: 3.5.0
- AWS SDK bundle resolved by Hadoop: 2.35.4
- SeaweedFS: 4.45
- Foundation Python: 3.13.11
- AWS CLI: 2.36.40

The Spark image currently supplies Python 3.10.12. This known difference from
the preferred Python 3.13 baseline must be recorded in the final environment
lock or resolved with a custom image before that lock is frozen.

## Credentials

`.env` is ignored by Git. Commit only `.env.example`, and replace its local demo
values for any non-isolated environment. Application code reads credentials
from environment variables; credentials must never be embedded in source code.
