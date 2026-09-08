# Run Kafka Smoke and Contract Integration

```bash
docker compose up -d kafka kafka-topic-init
docker compose run --rm kafka-cli
```

The topic description must show:

```text
Topic: urbanflow.trip-events
PartitionCount: 4
ReplicationFactor: 1
```

Publish Foundation fixture-derived events and consume them with an independent
generic client:

```bash
docker compose run --rm replay-python \
  python /workspace/urbanflow-replay/tests/run_kafka_integration.py
```

Expected final marker:

```text
REPLAY KAFKA INTEGRATION = PASS
```

Kafka is reachable as `kafka:9092` inside Compose and `localhost:9094` from the
host. Do not use the host address inside containers.
