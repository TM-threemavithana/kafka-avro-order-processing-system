import pytest

from order_pipeline.kafka_utils import (
    encode_headers,
    headers_to_dict,
    retry_attempt,
)


def test_header_round_trip() -> None:
    encoded = encode_headers({"retry-attempt": 2, "failure-type": "temporary"})
    assert headers_to_dict(encoded) == {
        "retry-attempt": "2",
        "failure-type": "temporary",
    }


def test_retry_attempt_defaults_to_zero() -> None:
    assert retry_attempt({}) == 0


@pytest.mark.parametrize("value", ["invalid", "-1"])
def test_invalid_retry_attempt_is_rejected(value: str) -> None:
    with pytest.raises(ValueError):
        retry_attempt({"retry-attempt": value})
