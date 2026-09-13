# UrbanFlow Streaming Environment Lock

Status: not validated yet.

Date: 2026-09-13
Branch: `urbanflow-streaming`

## Candidate Combination

This is the candidate required before integration tests:

```text
Spark image: apache/spark:4.2.0-python3
Spark Kafka connector: org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0
Hadoop AWS: org.apache.hadoop:hadoop-aws:3.5.0
SeaweedFS image: urbanflow/seaweedfs:4.45
Java on local host: OpenJDK 17.0.18 Temurin
Python local venv: Python 3.12.10
```

The previous Streaming candidate used:

```text
org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1
org.apache.hadoop:hadoop-aws:3.4.1
software.amazon.awssdk:bundle:2.29.52
```

That old Kafka connector is not kept because it does not match the Spark 4.2
runtime image.

## Validation Evidence

Completed:

```text
docker-compose config --quiet
exit code: 0
note: command prints a Docker config access warning, but Compose config validates.
```

Blocked:

```text
docker-compose run --rm foundation-check
result: failed before container start
reason: Docker daemon is not reachable at npipe:////./pipe/docker_engine

docker info
result: Docker client is present, server is unavailable
reason: open //./pipe/docker_engine: The system cannot find the file specified.
```

Not run yet:

```text
docker-compose run --rm spark-submit --master local[2] /workspace/docker/spark/local_smoke_test.py
docker-compose run --rm spark-submit --master local[2] /workspace/docker/spark/s3a_smoke_test.py
Kafka connector class-loading smoke test
image digest capture
Spark/Python version capture from inside the Spark image
```

## Decision

This file does not claim a locked runtime. It records the candidate and the
current blocker. The runtime can be called locked only after the Docker daemon is
running and the Spark local, S3A, and Kafka connector smoke tests pass.
