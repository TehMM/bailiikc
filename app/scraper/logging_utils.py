from __future__ import annotations

from typing import Any

from .utils import log_line


def _scraper_event(phase: str, **fields: Any) -> None:
    """Emit a structured scraper log line.

    `phase` is a short label like 'nav', 'plan', 'table', 'decision', 'box',
    'state', or 'error'. Logging must never interfere with control flow, so all
    errors are swallowed.
    """

    try:
        payload = ", ".join(f"{k}={repr(v)}" for k, v in sorted(fields.items()))
        log_line(f"[SCRAPER][{phase.upper()}] {payload}")
    except Exception:
        # Never let logging break the scraper.
        return


__all__ = ["_scraper_event"]
