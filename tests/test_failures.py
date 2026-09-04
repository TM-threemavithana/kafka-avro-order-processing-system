import pytest

from order_pipeline.failures import (
    FailurePolicy,
    PermanentProcessingError,
    TemporaryProcessingError,
)


@pytest.fixture
def policy() -> FailurePolicy:
    return FailurePolicy("TEMPORARY_FAILURE", "PERMANENT_FAILURE", 2)


def test_normal_order_succeeds(policy: FailurePolicy) -> None:
    policy.check({"orderId": "1", "product": "Item1", "price": 10.0}, 0)


def test_temporary_order_fails_twice_then_succeeds(policy: FailurePolicy) -> None:
    order = {"orderId": "2", "product": "TEMPORARY_FAILURE", "price": 10.0}

    with pytest.raises(TemporaryProcessingError):
        policy.check(order, 0)
    with pytest.raises(TemporaryProcessingError):
        policy.check(order, 1)
    policy.check(order, 2)


def test_permanent_order_always_fails(policy: FailurePolicy) -> None:
    order = {"orderId": "3", "product": "PERMANENT_FAILURE", "price": 10.0}

    with pytest.raises(PermanentProcessingError):
        policy.check(order, 0)
    with pytest.raises(PermanentProcessingError):
        policy.check(order, 10)
