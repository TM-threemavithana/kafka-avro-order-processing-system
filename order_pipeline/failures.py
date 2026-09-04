"""Deterministic failure simulation used to demonstrate retries and the DLQ."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


class TemporaryProcessingError(RuntimeError):
    """A recoverable failure that should be retried."""


class PermanentProcessingError(RuntimeError):
    """An unrecoverable failure that should go directly to the DLQ."""


@dataclass(frozen=True)
class FailurePolicy:
    temporary_product: str
    permanent_product: str
    temporary_failures_before_success: int = 2

    def check(self, order: Mapping[str, object], attempt: int) -> None:
        """Raise the failure requested by the order's demonstration product."""
        product = str(order["product"])
        order_id = str(order["orderId"])

        if product == self.permanent_product:
            raise PermanentProcessingError(
                f"Order {order_id} contains permanently rejected product {product}"
            )

        if (
            product == self.temporary_product
            and attempt < self.temporary_failures_before_success
        ):
            raise TemporaryProcessingError(
                f"Simulated temporary failure {attempt + 1}/"
                f"{self.temporary_failures_before_success} for order {order_id}"
            )
