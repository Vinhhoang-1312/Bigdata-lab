# UrbanFlow Streaming Environment Lock

Status: validated for Task 3.

Date: 2026-09-13
Branch: `urbanflow-streaming`

## Locked Combination

```text
Spark image: apache/spark:4.2.0-python3
Spark image digest: sha256:fc64959c04bd87b0ac686be9aaa9008b69cdb1afc695400528279c8b01f43d89
Spark runtime version: 4.2.0
Spark runtime Java: 21.0.11+10-LTS
Spark runtime timezone: America/New_York

Spark Kafka connector: org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0
Spark Kafka token provider: org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.2.0
Kafka client pulled by connector: org.apache.kafka:kafka-clients:3.9.2

Hadoop AWS: org.apache.hadoop:hadoop-aws:3.5.0
AWS SDK bundle pulled by Hadoop AWS: software.amazon.awssdk:bundle:2.35.4
S3 analytics accelerator pulled by Hadoop AWS: software.amazon.s3.analyticsaccelerator:analyticsaccelerator-s3:1.3.1

SeaweedFS image: urbanflow/seaweedfs:4.45
SeaweedFS image digest: sha256:461444cd6354fe84d29c07aa8b17e36bb3a85d5102d9540b77cb32aa882910de

Foundation-check image: python:3.13.11-slim-bookworm
Foundation-check image digest: sha256:20080e807bfc404f8450b185cf0fc95d553462673598549613735f70a5b4d5d0

AWS CLI image: amazon/aws-cli:2.36.40
AWS CLI image digest: sha256:b6aeb95d19d7f5a8cae4eb814cb16739b6b2a4f2f46f427ada6a8c9a20d9881d
```

Local host values observed during validation:

```text
Windows local Python: Python 3.12.10
Windows local Java: OpenJDK 17.0.18 Temurin
Docker Desktop context: desktop-linux
Docker Desktop storage VHDX: D:\Docker\DockerDesktopWSL\disk\docker_data.vhdx
Docker storage VHDX size: 8,150,581,248 bytes
Free disk after validation:
  C: 21,816,348,672 bytes
  D: 143,917,572,096 bytes
```

The previous Streaming candidate used:

```text
org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1
org.apache.hadoop:hadoop-aws:3.4.1
software.amazon.awssdk:bundle:2.29.52
```

That old Kafka connector is not kept because it does not match the Spark 4.2
runtime image.

## Commands Run

Compose validation:

```text
docker-compose config --quiet
result: exit code 0
note: command prints a Docker config access warning in this shell, but Compose config validates.
```

Foundation validator:

```text
docker-compose run --rm foundation-check
result: FOUNDATION FIXTURES = PASS
```

Spark local smoke:

```text
docker-compose run --rm spark-submit --master local[2] /workspace/docker/spark/local_smoke_test.py
result: SPARK = PASS
master: local[2]
timezone: America/New_York
```

S3A smoke:

```text
docker-compose run --rm spark-submit --master local[2] /workspace/docker/spark/s3a_smoke_test.py
result:
  WRITE = PASS
  READ  = PASS
  DATA  = PASS
path: s3a://urbanflow/test/spark-seaweedfs-roundtrip
```

Kafka connector class-loading smoke:

```text
docker-compose run --rm --entrypoint /opt/spark/bin/spark-submit spark-submit \
  --conf spark.jars.ivy=/root/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.hadoop:hadoop-aws:3.5.0 \
  --conf spark.sql.session.timeZone=America/New_York \
  --master local[2] \
  /workspace/docker/spark/kafka_connector_smoke_test.py

result: KAFKA CONNECTOR = PASS
fields: key,value,topic,partition,offset,timestamp,timestampType
timezone: America/New_York
```

Docker usage after validation:

```text
Images: 3.719GB
Containers: 229.4kB
Local volumes: 2.942GB
Build cache: 1.323GB
```

## Decision

This runtime combination is locked for the next Streaming integration steps.
It proves Spark 4.2.0, Hadoop AWS 3.5.0, SeaweedFS S3A, and the Spark Kafka
4.2.0 connector can load together in the project Docker environment.

This does not yet prove Replay Kafka events flow through Streaming. That is Task
4 and must use a real Kafka broker/topic acceptance test.
