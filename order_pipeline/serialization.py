"""Avro serialization for the assignment's order message."""

from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import Any, Mapping

from fastavro import parse_schema, schemaless_reader, schemaless_writer
from fastavro.validation import validate


SCHEMA_PATH = Path(__file__).resolve().parent.parent / "order.avsc"


def load_schema(path: Path = SCHEMA_PATH) -> dict[str, Any]:
    """Load and parse the Avro order schema."""
    with path.open("r", encoding="utf-8") as schema_file:
        return parse_schema(json.load(schema_file))


ORDER_SCHEMA = load_schema()


def validate_order(order: Mapping[str, object]) -> None:
    """Reject values that do not exactly satisfy the assignment contract."""
    expected_fields = {"orderId", "product", "price"}
    if set(order) != expected_fields:
        missing = sorted(expected_fields - set(order))
        extra = sorted(set(order) - expected_fields)
        raise ValueError(f"Invalid order fields; missing={missing}, extra={extra}")
    if not isinstance(order["orderId"], str) or not order["orderId"].strip():
        raise ValueError("orderId must be a non-empty string")
    if not isinstance(order["product"], str) or not order["product"].strip():
        raise ValueError("product must be a non-empty string")
    price = order["price"]
    if isinstance(price, bool) or not isinstance(price, (int, float)):
        raise ValueError("price must be numeric")
    if float(price) < 0:
        raise ValueError("price cannot be negative")
    if not validate(dict(order), ORDER_SCHEMA, raise_errors=False):
        raise ValueError("Order does not conform to order.avsc")


def serialize_order(order: Mapping[str, object]) -> bytes:
    """Serialize one order as a schemaless Avro binary record."""
    validate_order(order)
    buffer = BytesIO()
    schemaless_writer(buffer, ORDER_SCHEMA, dict(order))
    return buffer.getvalue()


def deserialize_order(payload: bytes) -> dict[str, object]:
    """Deserialize and validate one Avro binary order record."""
    if not payload:
        raise ValueError("Order payload is empty")
    buffer = BytesIO(payload)
    order = schemaless_reader(buffer, ORDER_SCHEMA)
    if buffer.read(1):
        raise ValueError("Order payload contains trailing bytes")
    validate_order(order)
    return order
