import pytest

from order_pipeline.serialization import deserialize_order, serialize_order


def test_order_round_trip_uses_avro_binary() -> None:
    order = {"orderId": "1001", "product": "Item1", "price": 125.5}

    payload = serialize_order(order)
    restored = deserialize_order(payload)

    assert isinstance(payload, bytes)
    assert not payload.startswith(b"{")
    assert restored["orderId"] == "1001"
    assert restored["product"] == "Item1"
    assert restored["price"] == pytest.approx(125.5)


@pytest.mark.parametrize(
    "invalid_order",
    [
        {"orderId": "1001", "product": "Item1"},
        {"orderId": "", "product": "Item1", "price": 10.0},
        {"orderId": "1001", "product": "", "price": 10.0},
        {"orderId": "1001", "product": "Item1", "price": "10.0"},
        {"orderId": "1001", "product": "Item1", "price": -1.0},
        {"orderId": "1001", "product": "Item1", "price": float("nan")},
        {"orderId": "1001", "product": "Item1", "price": float("inf")},
        {"orderId": "1001", "product": "Item1", "price": float("-inf")},
        {"orderId": "1001", "product": "Item1", "price": 1e100},
        {"orderId": "1001", "product": "Item1", "price": 10.0, "extra": 1},
    ],
)
def test_invalid_orders_are_rejected(invalid_order: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        serialize_order(invalid_order)


def test_empty_payload_is_rejected() -> None:
    with pytest.raises(ValueError, match="empty"):
        deserialize_order(b"")
