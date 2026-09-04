"""Read and explain Avro order records from the dead letter queue."""

from __future__ import annotations

import argparse
import json
import time
import uuid
from collections.abc import Sequence

from confluent_kafka import Consumer, KafkaError, KafkaException

from order_pipeline.config import Settings
from order_pipeline.kafka_utils import headers_to_dict
from order_pipeline.serialization import deserialize_order


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect failed orders in the DLQ.")
    parser.add_argument("--max-messages", type=positive_int, default=10)
    parser.add_argument("--timeout", type=positive_float, default=15.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = Settings.from_env()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": f"dlq-viewer-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.dlq_topic])
    deadline = time.monotonic() + args.timeout
    received = 0
    print(f"Reading {settings.dlq_topic!r} from the beginning...", flush=True)

    try:
        while received < args.max_messages and time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            try:
                order: object = deserialize_order(message.value())
            except Exception as error:
                order = {"unreadableAvro": str(error)}
            output = {
                "order": order,
                "failureMetadata": headers_to_dict(message.headers()),
                "partition": message.partition(),
                "offset": message.offset(),
            }
            print(json.dumps(output, indent=2, sort_keys=True), flush=True)
            received += 1
    finally:
        consumer.close()

    print(f"DLQ viewer finished; messages_read={received}.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
