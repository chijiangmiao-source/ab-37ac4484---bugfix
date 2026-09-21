"""Single source of "now" so tests can freeze time via monkeypatching."""

from __future__ import annotations

from datetime import datetime, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
