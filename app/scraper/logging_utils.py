from __future__ import annotations

from typing import Any

from .utils import log_line


def _scraper_event(label: str = "", *, phase: str | None = None, **fields: Any) -> None:
    """Emit a structured scraper log line.

    ``phase`` may be used as a keyword alias for the label for compatibility
    with existing call sites. When both ``label`` and ``phase`` are provided,
    ``phase`` is emitted as part of the payload so the caller still captures the
    event stage.
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
