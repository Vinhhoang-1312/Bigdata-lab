# Fixture Kafka Smoke Result

Foundation's seven Silver fixture rows mapped exactly to the seven reference
Kafka event objects. A generic Confluent Kafka consumer started at the topic's
end offset before publishing and received all seven new records.

Validated:

```text
KAFKA TOPIC CONTRACT = PASS
GENERIC CONSUMER = PASS
MESSAGE KEYS = PASS
FIXTURE KAFKA EVENTS = PASS
REPLAY KAFKA INTEGRATION = PASS
```

The topic had four partitions and replication factor one. Every message key was
the decimal pickup-zone ID, events for each key used a consistent partition,
and all event values passed strict Foundation validation.
