"""Command-line producer for randomized Avro order messages."""

from __future__ import annotations

import argparse
import random
import time
from collections.abc import Sequence

from order_pipeline.broker import KafkaPublisher
from order_pipeline.config import Settings
from order_pipeline.serialization import serialize_order


NORMAL_PRODUCTS = ("Item1", "Item2", "Item3", "Item4")


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be zero or greater")
    return parsed


def generate_orders(
    count: int,
    *,
    seed: int | None,
    demo: bool,
    temporary_failure_product: str,
    permanent_failure_product: str,
) -> list[dict[str, object]]:
    """Generate orders; a supplied seed makes the batch repeatable."""
    if demo and count < 3:
        raise ValueError("Demo mode needs at least three messages")

    generator = random.Random(seed)
    batch_id = int(time.time() * 1000)
    orders = [
        {
            "orderId": f"{batch_id}-{position + 1:04d}",
            "product": generator.choice(NORMAL_PRODUCTS),
            "price": round(generator.uniform(10.0, 500.0), 2),
        }
        for position in range(count)
    ]

    if demo:
        orders[-2]["product"] = temporary_failure_product
        orders[-1]["product"] = permanent_failure_product
    return orders


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Produce randomized Avro order messages to Kafka."
    )
    parser.add_argument("--count", type=positive_int, default=10)
    parser.add_argument("--interval", type=non_negative_float, default=0.35)
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional seed for a repeatable batch (default: system randomness).",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Make the final two orders demonstrate retry and DLQ behavior.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = Settings.from_env()

    try:
        orders = generate_orders(
            args.count,
            seed=args.seed,
            demo=args.demo,
            temporary_failure_product=settings.temporary_failure_product,
            permanent_failure_product=settings.permanent_failure_product,
        )
    except ValueError as error:
        build_parser().error(str(error))

    publisher = KafkaPublisher(settings.bootstrap_servers, "order-producer")
    print(
        f"Producing {len(orders)} Avro orders to {settings.orders_topic!r} "
        f"via {settings.bootstrap_servers}",
        flush=True,
    )

    try:
        for order in orders:
            payload = serialize_order(order)
            publisher.publish(
                settings.orders_topic,
                payload,
                key=str(order["orderId"]),
            )
            print(
                f"PRODUCED orderId={order['orderId']} product={order['product']} "
                f"price={float(order['price']):.2f}",
                flush=True,
            )
            if args.interval:
                time.sleep(args.interval)
    finally:
        publisher.close()

    print("Producer finished successfully.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
