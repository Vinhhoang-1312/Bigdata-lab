# Kafka to Spark Smoke Result

Date: 2026-09-13
Branch: `urbanflow-streaming`
Result: `Kafka -> Spark = PASS`

## Runtime

```text
Kafka broker image: apache/kafka:3.9.1
Kafka internal address: kafka:9092
Kafka host address: localhost:9094
Spark image: apache/spark:4.2.0-python3
Spark Kafka connector: org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0
Kafka client pulled by connector: org.apache.kafka:kafka-clients:3.9.2
Spark session timezone: America/New_York
```

## Topic Metadata

Canonical topic:

```text
Topic: urbanflow.trip-events
PartitionCount: 4
ReplicationFactor: 1
Partitions: 0, 1, 2, 3
Leader: 1
Replicas: 1
ISR: 1
```

Malformed smoke topic:

```text
Topic: urbanflow.trip-events.smoke-invalid
PartitionCount: 1
ReplicationFactor: 1
Leader: 1
Replicas: 1
ISR: 1
```

## Commands

Start Kafka and create topics:

```powershell
docker-compose --profile integration up -d kafka-topic-init
```

Describe canonical topic:

```powershell
docker-compose --profile integration run --rm kafka-cli --describe --topic urbanflow.trip-events
```

Run Spark smoke:

```powershell
docker-compose --profile integration run --rm spark-submit --master local[2] `
  /workspace/urbanflow-streaming/tests/run_kafka_streaming_smoke.py `
  --bootstrap-servers kafka:9092
```

## Smoke Summary

```json
{
  "result": "Kafka -> Spark = PASS",
  "run_id": "smoke-task4-final-001",
  "started_at": "2026-09-13T15:06:02Z",
  "finished_at": "2026-09-13T15:06:23Z",
  "bootstrap_servers": "kafka:9092",
  "topic": "urbanflow.trip-events",
  "invalid_topic": "urbanflow.trip-events.smoke-invalid",
  "valid": {
    "valid_count": 7,
    "invalid_count": 0,
    "pickup_zone_ids": [161, 236],
    "event_times": [
      "2025-01-06T08:00:30-05:00",
      "2025-01-06T08:01:00-05:00",
      "2025-01-06T08:01:15-05:00",
      "2025-01-06T08:02:00-05:00",
      "2025-01-06T08:02:44-05:00",
      "2025-01-06T08:03:20-05:00",
      "2025-01-06T08:03:55-05:00"
    ]
  },
  "malformed": {
    "invalid_count": 1,
    "raw_value_non_empty": true,
    "validation_error": "missing_required_field"
  }
}
```

## Boundary Notes

- The smoke verifier published the authoritative Foundation Kafka fixture shape.
- The event IDs were suffixed with a unique run marker so old topic data cannot satisfy the check.
- Kafka keys were the decimal string form of `pickup_zone_id`.
- Spark Structured Streaming read from Kafka using the project parser in `spark.event_schema`.
- The explicit UTC offsets in `event_time` strings were preserved.
- The malformed test message remained visible with `raw_value` and `validation_error`.
- This test proves the Kafka to Spark contract boundary.
- It does not yet run the full Replay CLI or the full Live Gold writer.
