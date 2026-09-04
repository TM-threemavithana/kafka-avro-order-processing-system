"""Small Kafka helpers shared by the pipeline applications."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Mapping


def headers_to_dict(
    headers: Iterable[tuple[str, bytes | None]] | None,
) -> dict[str, str]:
    decoded: dict[str, str] = {}
    for key, value in headers or []:
        decoded[key] = "" if value is None else value.decode("utf-8", errors="replace")
    return decoded


def encode_headers(headers: Mapping[str, object]) -> list[tuple[str, bytes]]:
    return [(key, str(value).encode("utf-8")) for key, value in headers.items()]


def retry_attempt(headers: Mapping[str, str]) -> int:
    raw_attempt = headers.get("retry-attempt", "0")
    try:
        attempt = int(raw_attempt)
    except ValueError as error:
        raise ValueError(f"Invalid retry-attempt header: {raw_attempt!r}") from error
    if attempt < 0:
        raise ValueError("retry-attempt cannot be negative")
    return attempt


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
