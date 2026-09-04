from pathlib import Path

import pytest

from order_pipeline.config import Settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Provide isolated pipeline settings for consumer unit tests."""
    return Settings(
        bootstrap_servers="unused:9092",
        orders_topic="orders",
        retry_topic="orders.retry",
        dlq_topic="orders.dlq",
        consumer_group="test-consumer",
        max_retries=3,
        retry_backoff_seconds=1.0,
        temporary_failure_product="TEMPORARY_FAILURE",
        permanent_failure_product="PERMANENT_FAILURE",
        temporary_failures_before_success=2,
        state_file=tmp_path / "averages.json",
    )
