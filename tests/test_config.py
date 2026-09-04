import pytest

from order_pipeline.config import Settings


def test_zero_disables_retries_and_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MAX_RETRIES", "0")
    monkeypatch.setenv("RETRY_BACKOFF_SECONDS", "0")

    settings = Settings.from_env()

    assert settings.max_retries == 0
    assert settings.retry_backoff_seconds == 0


@pytest.mark.parametrize("value", ["-1", "nan", "inf"])
def test_invalid_backoff_is_rejected(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("RETRY_BACKOFF_SECONDS", value)

    with pytest.raises(ValueError):
        Settings.from_env()
