"""Environment-based configuration for all pipeline applications."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _positive_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value < 0:
        raise ValueError(f"{name} must be zero or greater")
    return value


def _positive_float(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if value < 0:
        raise ValueError(f"{name} must be zero or greater")
    return value


@dataclass(frozen=True)
class Settings:
    """Runtime settings shared by the producer, consumer, and DLQ viewer."""

    bootstrap_servers: str
    orders_topic: str
    retry_topic: str
    dlq_topic: str
    consumer_group: str
    max_retries: int
    retry_backoff_seconds: float
    temporary_failure_product: str
    permanent_failure_product: str
    temporary_failures_before_success: int
    state_file: Path

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS", "localhost:9092"),
            orders_topic=os.getenv("ORDERS_TOPIC", "orders"),
            retry_topic=os.getenv("RETRY_TOPIC", "orders.retry"),
            dlq_topic=os.getenv("DLQ_TOPIC", "orders.dlq"),
            consumer_group=os.getenv("CONSUMER_GROUP", "order-processor-v1"),
            max_retries=_positive_int("MAX_RETRIES", 3),
            retry_backoff_seconds=_positive_float("RETRY_BACKOFF_SECONDS", 1.0),
            temporary_failure_product=os.getenv(
                "TEMPORARY_FAILURE_PRODUCT", "TEMPORARY_FAILURE"
            ),
            permanent_failure_product=os.getenv(
                "PERMANENT_FAILURE_PRODUCT", "PERMANENT_FAILURE"
            ),
            temporary_failures_before_success=_positive_int(
                "TEMPORARY_FAILURES_BEFORE_SUCCESS", 2
            ),
            state_file=Path(os.getenv("STATE_FILE", "data/averages.json")),
        )
