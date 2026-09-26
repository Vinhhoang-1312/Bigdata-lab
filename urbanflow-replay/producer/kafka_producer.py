"""Small Kafka adapter that makes delivery behavior explicit and testable."""

from __future__ import annotations

from collections.abc import Callable


class KafkaEventProducer:
    def __init__(self, bootstrap_servers: str, client_id: str = "urbanflow-replay"):
        try:
            from confluent_kafka import Producer
        except ImportError as error:
            raise RuntimeError("Kafka publishing requires confluent-kafka") from error

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "lz4",
                "linger.ms": 5,
                "batch.num.messages": 10000,
            }
        )

    def send(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        callback: Callable[[object, object], None],
    ) -> None:
        while True:
            try:
                self._producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    on_delivery=callback,
                )
                break
            except BufferError:
                self._producer.poll(0.05)
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        return self._producer.flush(timeout)
