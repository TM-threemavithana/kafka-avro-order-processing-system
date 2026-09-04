"""Reliable publishing wrapper around the Confluent Kafka client."""

from __future__ import annotations

from collections.abc import Iterable

from confluent_kafka import Producer


class PublishError(RuntimeError):
    """Raised when Kafka does not acknowledge a message."""


class KafkaPublisher:
    """Publish one record and wait for the broker acknowledgement."""

    def __init__(self, bootstrap_servers: str, client_id: str) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "enable.idempotence": True,
                "acks": "all",
            }
        )

    def publish(
        self,
        topic: str,
        value: bytes,
        *,
        key: bytes | str | None = None,
        headers: Iterable[tuple[str, bytes]] | None = None,
        timeout: float = 15.0,
    ) -> None:
        errors: list[str] = []

        def delivered(error: object, _message: object) -> None:
            if error is not None:
                errors.append(str(error))

        while True:
            try:
                self._producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    headers=list(headers or []),
                    on_delivery=delivered,
                )
                break
            except BufferError:
                self._producer.poll(0.5)

        outstanding = self._producer.flush(timeout)
        if outstanding:
            raise PublishError(
                f"Kafka did not acknowledge {outstanding} message(s) within {timeout}s"
            )
        if errors:
            raise PublishError("; ".join(errors))

    def close(self) -> None:
        outstanding = self._producer.flush(15.0)
        if outstanding:
            raise PublishError(f"Unable to flush {outstanding} Kafka message(s)")
