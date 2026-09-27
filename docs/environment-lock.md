# UrbanFlow Environment Lock — In Progress

This records validated versions. Streaming entries remain pending until that
subproject is implemented; therefore the final integration lock is not yet
complete.

```text
Foundation:
  urbanflow-foundation 1.0.0

Host:
  Ubuntu 24.04 family, Linux kernel 7.0.0-31-generic, x86_64
  8 CPU cores, 7.6 GiB RAM

Docker Engine:
  29.7.2

Docker Compose:
  5.5.0

Foundation Python image:
  Python 3.13.11 (python:3.13.11-slim-bookworm)

Spark image:
  apache/spark:4.2.0-python3

Spark:
  4.2.0

PySpark:
  4.2.0

Spark image Python:
  3.10.12 (known difference from preferred 3.13 baseline)

Java in Spark image:
  OpenJDK 21.0.11

Scala:
  2.13.18

Hadoop:
  3.5.0

hadoop-aws:
  3.5.0

AWS SDK bundle:
  2.35.4

SeaweedFS:
  4.45

SeaweedFS S3 endpoint:
  http://seaweedfs:8333 inside Compose
  http://localhost:8333 from host

Storage bucket:
  urbanflow

Spark master:
  spark://spark-master:7077

Analytics timezone:
  America/New_York

Kafka image:
  apache/kafka:3.9.1

Kafka mode:
  One broker, KRaft, node/controller ID 1

Kafka bootstrap servers:
  kafka:9092 inside Compose
  localhost:9094 from host

Kafka topic:
  urbanflow.trip-events
  4 partitions, replication factor 1

Replay image:
  urbanflow/replay:python3.13

Replay Python:
  3.13.11

confluent-kafka / librdkafka:
  2.15.0 / 2.15.0

PyArrow:
  25.0.1

Streaming runtime:
  PENDING
```

Validated gates so far: Foundation, Spark master/two workers, SeaweedFS,
Spark↔S3A Parquet round trip, fixture and canonical Batch, Historical Gold,
Kafka broker/topic producer-consumer smoke, fixture Replay, direct Batch →
Replay integration, and Replay Velocity evidence through 5,000 events/sec.
