from __future__ import annotations

from typing import Any

from .utils import log_line


def _scraper_event(label: str = "", *, phase: str | None = None, **fields: Any) -> None:
    """Emit a structured scraper log line.

    ``label`` controls the bracketed tag (e.g. 'state', 'nav', 'error').
    ``phase`` is an optional payload field to preserve the old 'phase=' semantics
    at call sites without changing the log prefix.
    """

    try:
        phase_label = label or (phase or "")
        if phase and label:
            fields.setdefault("phase", phase)
        payload = ", ".join(f"{k}={repr(v)}" for k, v in sorted(fields.items()))
        log_line(f"[SCRAPER][{phase_label.upper()}] {payload}")
    except Exception:
        # Never let logging break the scraper.
        return


__all__ = ["_scraper_event"]
