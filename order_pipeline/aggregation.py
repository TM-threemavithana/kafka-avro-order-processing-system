"""Idempotent running-average state for processed orders."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class AverageSnapshot:
    order_id: str
    product: str
    price: float
    total_count: int
    total_average: float
    product_count: int
    product_average: float
    duplicate: bool = False


class RunningAverages:
    """Maintain global and per-product averages and ignore duplicate order IDs."""

    def __init__(self, state_file: Path | None = None) -> None:
        self.state_file = state_file
        self.total_count = 0
        self.total_sum = 0.0
        self.products: dict[str, dict[str, float | int]] = {}
        self.processed_order_ids: set[str] = set()
        if state_file and state_file.exists():
            self._load()

    def update(self, order: Mapping[str, object]) -> AverageSnapshot:
        order_id = str(order["orderId"])
        product = str(order["product"])
        price = float(order["price"])

        if order_id in self.processed_order_ids:
            product_state = self.products[product]
            return AverageSnapshot(
                order_id=order_id,
                product=product,
                price=price,
                total_count=self.total_count,
                total_average=self.total_sum / self.total_count,
                product_count=int(product_state["count"]),
                product_average=float(product_state["sum"])
                / int(product_state["count"]),
                duplicate=True,
            )

        self.processed_order_ids.add(order_id)
        self.total_count += 1
        self.total_sum += price

        product_state = self.products.setdefault(product, {"count": 0, "sum": 0.0})
        product_state["count"] = int(product_state["count"]) + 1
        product_state["sum"] = float(product_state["sum"]) + price

        snapshot = AverageSnapshot(
            order_id=order_id,
            product=product,
            price=price,
            total_count=self.total_count,
            total_average=self.total_sum / self.total_count,
            product_count=int(product_state["count"]),
            product_average=float(product_state["sum"])
            / int(product_state["count"]),
        )
        self._save()
        return snapshot

    def _load(self) -> None:
        if self.state_file is None:
            return
        with self.state_file.open("r", encoding="utf-8") as state_handle:
            state = json.load(state_handle)
        self.total_count = int(state["total_count"])
        self.total_sum = float(state["total_sum"])
        self.products = state["products"]
        self.processed_order_ids = set(state["processed_order_ids"])

    def _save(self) -> None:
        if self.state_file is None:
            return
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        temporary_file = self.state_file.with_suffix(self.state_file.suffix + ".tmp")
        state = {
            "total_count": self.total_count,
            "total_sum": self.total_sum,
            "products": self.products,
            "processed_order_ids": sorted(self.processed_order_ids),
        }
        with temporary_file.open("w", encoding="utf-8") as state_handle:
            json.dump(state, state_handle, indent=2, sort_keys=True)
            state_handle.write("\n")
        os.replace(temporary_file, self.state_file)


def format_snapshot(snapshot: AverageSnapshot) -> str:
    """Create a readable one-line status for the live demonstration."""
    if snapshot.duplicate:
        return f"DUPLICATE skipped orderId={snapshot.order_id}"
    return (
        f"PROCESSED orderId={snapshot.order_id} product={snapshot.product} "
        f"price={snapshot.price:.2f} | overall: count={snapshot.total_count} "
        f"average={snapshot.total_average:.2f} | {snapshot.product}: "
        f"count={snapshot.product_count} average={snapshot.product_average:.2f}"
    )
