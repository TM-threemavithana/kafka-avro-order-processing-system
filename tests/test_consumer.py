from collections.abc import Iterable

import pytest

from order_pipeline.aggregation import RunningAverages
from order_pipeline.config import Settings
from order_pipeline.consumer import OrderConsumerApp
from order_pipeline.failures import FailurePolicy, TemporaryProcessingError
from order_pipeline.kafka_utils import headers_to_dict
from order_pipeline.serialization import serialize_order


class FakeConsumer:
    def __init__(self) -> None:
        self.paused: list[object] = []
        self.resumed: list[object] = []
        self.committed: list[object] = []

    def pause(self, partitions: list[object]) -> None:
        self.paused.extend(partitions)

    def resume(self, partitions: list[object]) -> None:
        self.resumed.extend(partitions)

    def commit(self, *, message: object, asynchronous: bool) -> None:
        assert asynchronous is False
        self.committed.append(message)


class FakePublisher:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    def publish(
        self,
        topic: str,
        value: bytes,
        *,
        key: bytes | str | None = None,
        headers: Iterable[tuple[str, bytes]] | None = None,
    ) -> None:
        self.messages.append(
            {"topic": topic, "value": value, "key": key, "headers": headers}
        )


class FakeMessage:
    def __init__(
        self,
        *,
        topic: str = "orders.retry",
        partition: int = 0,
        headers: list[tuple[str, bytes]] | None = None,
        value: bytes = b"avro",
    ) -> None:
        self._topic = topic
        self._partition = partition
        self._headers = headers
        self._value = value

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def headers(self) -> list[tuple[str, bytes]] | None:
        return self._headers

    def key(self) -> bytes:
        return b"order-1"

    def value(self) -> bytes:
        return self._value


def make_app(settings: Settings) -> OrderConsumerApp:
    app = OrderConsumerApp.__new__(OrderConsumerApp)
    app.settings = settings
    app.consumer = FakeConsumer()
    app.publisher = FakePublisher()
    app._deferred_retries = {}
    return app


def test_future_retry_pauses_only_its_partition(settings: Settings) -> None:
    app = make_app(settings)
    message = FakeMessage(headers=[("retry-not-before", b"110")])

    assert app._defer_retry(message, now=100) is True
    assert len(app.consumer.paused) == 1
    assert app._poll_timeout(now=100) == pytest.approx(1.0)

    handled: list[object] = []
    app._handle = handled.append  # type: ignore[method-assign]
    app._process_due_retries(now=109)
    assert handled == []

    app._process_due_retries(now=110)
    assert handled == [message]
    assert len(app.consumer.resumed) == 1
    assert app._deferred_retries == {}


def test_main_topic_message_is_never_deferred(settings: Settings) -> None:
    app = make_app(settings)
    message = FakeMessage(
        topic="orders", headers=[("retry-not-before", b"9999999999")]
    )

    assert app._defer_retry(message, now=100) is False
    assert app.consumer.paused == []


def test_retry_is_published_immediately_with_due_timestamp(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = make_app(settings)
    message = FakeMessage(topic="orders")
    monkeypatch.setattr("order_pipeline.consumer.time.time", lambda: 100.0)

    app._send_to_retry(
        message,
        {},
        0,
        TemporaryProcessingError("temporary"),
    )

    published = app.publisher.messages[0]
    headers = headers_to_dict(published["headers"])
    assert published["topic"] == "orders.retry"
    assert headers["retry-attempt"] == "1"
    assert headers["retry-not-before"] == "101.000000"


def test_conflicting_duplicate_is_routed_to_dlq(settings: Settings) -> None:
    app = make_app(settings)
    app.averages = RunningAverages()
    app.failure_policy = FailurePolicy(
        settings.temporary_failure_product,
        settings.permanent_failure_product,
        settings.temporary_failures_before_success,
    )
    first = FakeMessage(
        topic="orders",
        value=serialize_order(
            {"orderId": "order-1", "product": "Item1", "price": 10.0}
        ),
    )
    conflict = FakeMessage(
        topic="orders",
        value=serialize_order(
            {"orderId": "order-1", "product": "Item2", "price": 10.0}
        ),
    )

    app._handle(first)
    app._handle(conflict)

    assert app.averages.total_count == 1
    assert len(app.consumer.committed) == 2
    dlq_message = app.publisher.messages[-1]
    assert dlq_message["topic"] == "orders.dlq"
    assert headers_to_dict(dlq_message["headers"])["failure-type"] == (
        "duplicate-conflict"
    )
