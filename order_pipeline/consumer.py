"""Kafka consumer with aggregation, retry routing, and a dead letter queue."""

from __future__ import annotations

import signal
import time
from types import FrameType

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from order_pipeline.aggregation import RunningAverages, format_snapshot
from order_pipeline.broker import KafkaPublisher
from order_pipeline.config import Settings
from order_pipeline.failures import (
    FailurePolicy,
    PermanentProcessingError,
    TemporaryProcessingError,
)
from order_pipeline.kafka_utils import (
    encode_headers,
    headers_to_dict,
    retry_attempt,
    utc_now,
)
from order_pipeline.serialization import deserialize_order


class OrderConsumerApp:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.running = True
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "group.id": settings.consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.publisher = KafkaPublisher(
            settings.bootstrap_servers, "order-consumer-router"
        )
        self.averages = RunningAverages(settings.state_file)
        self.failure_policy = FailurePolicy(
            temporary_product=settings.temporary_failure_product,
            permanent_product=settings.permanent_failure_product,
            temporary_failures_before_success=(
                settings.temporary_failures_before_success
            ),
        )

    def stop(self, _signum: int, _frame: FrameType | None) -> None:
        self.running = False

    def run(self) -> None:
        self.consumer.subscribe(
            [self.settings.orders_topic, self.settings.retry_topic]
        )
        print(
            "Consumer ready | "
            f"topics={self.settings.orders_topic},{self.settings.retry_topic} "
            f"group={self.settings.consumer_group} "
            f"max_retries={self.settings.max_retries}",
            flush=True,
        )

        try:
            while self.running:
                message = self.consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())
                self._handle(message)
        finally:
            self.consumer.close()
            self.publisher.close()

    def _handle(self, message: Message) -> None:
        headers = headers_to_dict(message.headers())
        try:
            attempt = retry_attempt(headers)
        except ValueError as error:
            self._send_to_dlq(message, headers, 0, "invalid-metadata", error)
            self._commit(message)
            return

        try:
            order = deserialize_order(message.value())
        except Exception as error:
            self._send_to_dlq(message, headers, attempt, "invalid-avro", error)
            self._commit(message)
            return

        try:
            self.failure_policy.check(order, attempt)
            snapshot = self.averages.update(order)
            print(format_snapshot(snapshot), flush=True)
        except TemporaryProcessingError as error:
            if attempt < self.settings.max_retries:
                self._send_to_retry(message, headers, attempt, error)
            else:
                self._send_to_dlq(
                    message, headers, attempt, "retries-exhausted", error
                )
        except PermanentProcessingError as error:
            self._send_to_dlq(message, headers, attempt, "permanent", error)

        self._commit(message)

    def _send_to_retry(
        self,
        message: Message,
        headers: dict[str, str],
        attempt: int,
        error: Exception,
    ) -> None:
        next_attempt = attempt + 1
        delay = self.settings.retry_backoff_seconds * (2**attempt)
        print(
            f"RETRY scheduled orderId={self._message_key(message)} "
            f"attempt={next_attempt}/{self.settings.max_retries} "
            f"backoff={delay:.2f}s reason={error}",
            flush=True,
        )
        if delay:
            time.sleep(delay)

        retry_headers = {
            "retry-attempt": next_attempt,
            "original-topic": headers.get("original-topic", message.topic()),
            "last-error": str(error),
            "retried-at": utc_now(),
        }
        self.publisher.publish(
            self.settings.retry_topic,
            message.value(),
            key=message.key(),
            headers=encode_headers(retry_headers),
        )
        print(
            f"RETRY published orderId={self._message_key(message)} "
            f"attempt={next_attempt}",
            flush=True,
        )

    def _send_to_dlq(
        self,
        message: Message,
        headers: dict[str, str],
        attempt: int,
        failure_type: str,
        error: Exception,
    ) -> None:
        dlq_headers = {
            "retry-attempt": attempt,
            "original-topic": headers.get("original-topic", message.topic()),
            "failure-type": failure_type,
            "error-message": str(error),
            "failed-at": utc_now(),
        }
        self.publisher.publish(
            self.settings.dlq_topic,
            message.value(),
            key=message.key(),
            headers=encode_headers(dlq_headers),
        )
        print(
            f"DLQ published orderId={self._message_key(message)} "
            f"type={failure_type} attempts={attempt} reason={error}",
            flush=True,
        )

    def _commit(self, message: Message) -> None:
        self.consumer.commit(message=message, asynchronous=False)

    @staticmethod
    def _message_key(message: Message) -> str:
        key = message.key()
        if key is None:
            return "<no-key>"
        if isinstance(key, bytes):
            return key.decode("utf-8", errors="replace")
        return str(key)


def main() -> int:
    app = OrderConsumerApp(Settings.from_env())
    signal.signal(signal.SIGINT, app.stop)
    signal.signal(signal.SIGTERM, app.stop)
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
