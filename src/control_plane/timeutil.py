"""Timestamps: always UTC, millisecond precision, rendered with a `Z` suffix."""

from __future__ import annotations

from datetime import UTC, datetime


def utcnow_ms() -> datetime:
    """Current UTC time truncated to milliseconds.

    Truncating *before* storing means the value we persist is exactly the value we
    render, so a client never sees a timestamp change between two reads.
    """
    now = datetime.now(UTC)
    return now.replace(microsecond=(now.microsecond // 1000) * 1000)


def iso_ms(dt: datetime) -> str:
    """`2026-01-31T12:34:56.789Z`"""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"
