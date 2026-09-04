import json
from pathlib import Path

import pytest

from order_pipeline.aggregation import RunningAverages, format_snapshot


def test_global_and_per_product_running_averages() -> None:
    averages = RunningAverages()

    first = averages.update({"orderId": "1", "product": "Item1", "price": 10.0})
    second = averages.update({"orderId": "2", "product": "Item1", "price": 30.0})
    third = averages.update({"orderId": "3", "product": "Item2", "price": 50.0})

    assert first.total_average == pytest.approx(10.0)
    assert second.total_average == pytest.approx(20.0)
    assert second.product_average == pytest.approx(20.0)
    assert third.total_average == pytest.approx(30.0)
    assert third.product_average == pytest.approx(50.0)
    assert "overall: count=3 average=30.00" in format_snapshot(third)


def test_duplicate_order_is_not_counted_twice() -> None:
    averages = RunningAverages()
    order = {"orderId": "1", "product": "Item1", "price": 10.0}

    averages.update(order)
    duplicate = averages.update(order)

    assert duplicate.duplicate is True
    assert averages.total_count == 1
    assert averages.total_sum == pytest.approx(10.0)


def test_state_survives_restart(tmp_path: Path) -> None:
    state_file = tmp_path / "averages.json"
    first_instance = RunningAverages(state_file)
    first_instance.update({"orderId": "1", "product": "Item1", "price": 12.0})

    second_instance = RunningAverages(state_file)
    snapshot = second_instance.update(
        {"orderId": "2", "product": "Item1", "price": 18.0}
    )

    assert snapshot.total_count == 2
    assert snapshot.total_average == pytest.approx(15.0)
    assert json.loads(state_file.read_text(encoding="utf-8"))["total_count"] == 2
