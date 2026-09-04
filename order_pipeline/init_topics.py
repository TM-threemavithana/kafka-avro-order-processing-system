"""Wait for Kafka and create the assignment topics."""

from __future__ import annotations

import os
import time

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from order_pipeline.config import Settings


def main() -> int:
    settings = Settings.from_env()
    timeout_seconds = float(os.getenv("KAFKA_STARTUP_TIMEOUT_SECONDS", "90"))
    partitions = int(os.getenv("TOPIC_PARTITIONS", "1"))
    if timeout_seconds <= 0 or partitions <= 0:
        raise ValueError("Startup timeout and topic partitions must be positive")

    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    deadline = time.monotonic() + timeout_seconds

    while True:
        try:
            metadata = admin.list_topics(timeout=5.0)
            break
        except KafkaException as error:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Kafka was not ready after {timeout_seconds:.0f} seconds"
                ) from error
            print("Waiting for Kafka broker...", flush=True)
            time.sleep(2.0)

    topic_names = (
        settings.orders_topic,
        settings.retry_topic,
        settings.dlq_topic,
    )
    missing_topics = [name for name in topic_names if name not in metadata.topics]

    if missing_topics:
        futures = admin.create_topics(
            [
                NewTopic(name, num_partitions=partitions, replication_factor=1)
                for name in missing_topics
            ],
            operation_timeout=30.0,
        )
        for name, future in futures.items():
            future.result(timeout=35.0)
            print(f"Created topic {name!r}.", flush=True)

    while True:
        final_metadata = admin.list_topics(timeout=10.0)
        not_visible = [
            name for name in topic_names if name not in final_metadata.topics
        ]
        if not not_visible:
            break
        if time.monotonic() >= deadline:
            raise TimeoutError(
                "Created topics did not become visible before the startup timeout: "
                + ", ".join(not_visible)
            )
        time.sleep(0.5)

    for name in topic_names:
        topic = final_metadata.topics[name]
        print(
            f"Topic {name!r}: partitions={len(topic.partitions)}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
