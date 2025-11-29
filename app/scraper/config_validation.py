from __future__ import annotations

from typing import Literal

from . import config
from .logging_utils import _scraper_event
from .utils import log_line

Entrypoint = Literal["ui", "cli", "webhook", "replay", "tests"]


def _raise_config_error(
    message: str, *, entrypoint: Entrypoint, error: str, mode: str | None
) -> None:
    _scraper_event(
        "error",
        phase="config",
        context="runtime_validation",
        error=error,
        entrypoint=entrypoint,
        mode=mode,
    )
    mode_fragment = f", mode={mode}" if mode else ""
    log_line(f"[CONFIG] {message} (entrypoint={entrypoint}{mode_fragment})")
    raise ValueError(message)


def validate_runtime_config(entrypoint: Entrypoint, *, mode: str | None = None) -> None:
    """Validate runtime configuration for the given entrypoint.

    Raises ``ValueError`` when a blocking misconfiguration is detected.
    Non-fatal adjustments (e.g., clamping executor knobs) are logged but do not
    raise.
    """

    if config.REPLAY_SKIP_NETWORK and entrypoint not in {"replay", "tests"}:
        _raise_config_error(
            "REPLAY_SKIP_NETWORK must not be enabled for live scrapes; use only in replay/tests.",
            entrypoint=entrypoint,
            error="replay_skip_network_forbidden",
            mode=mode,
        )

    if config.ENABLE_DOWNLOAD_EXECUTOR and config.MAX_PARALLEL_DOWNLOADS < 1:
        adjusted = 1
        _scraper_event(
            "state",
            phase="config",
            context="runtime_validation",
            kind="config_adjustment",
            field="MAX_PARALLEL_DOWNLOADS",
            value=config.MAX_PARALLEL_DOWNLOADS,
            adjusted=adjusted,
            entrypoint=entrypoint,
            mode=mode,
        )
        log_line(
            "[CONFIG] MAX_PARALLEL_DOWNLOADS < 1 when executor enabled; clamping to 1 for safety."
        )
        config.MAX_PARALLEL_DOWNLOADS = adjusted

    if config.ENABLE_DOWNLOAD_EXECUTOR and config.MAX_PENDING_DOWNLOADS < 1:
        adjusted = 1
        _scraper_event(
            "state",
            phase="config",
            context="runtime_validation",
            kind="config_adjustment",
            field="MAX_PENDING_DOWNLOADS",
            value=config.MAX_PENDING_DOWNLOADS,
            adjusted=adjusted,
            entrypoint=entrypoint,
            mode=mode,
        )
        log_line(
            "[CONFIG] MAX_PENDING_DOWNLOADS < 1 when executor enabled; clamping to 1 for safety."
        )
        config.MAX_PENDING_DOWNLOADS = adjusted

    if config.MIN_FREE_MB < 0:
        _raise_config_error(
            "MIN_FREE_MB must be non-negative.",
            entrypoint=entrypoint,
            error="min_free_mb_invalid",
            mode=mode,
        )

    timeout_fields = [
        ("PLAYWRIGHT_NAV_TIMEOUT_SECONDS", config.PLAYWRIGHT_NAV_TIMEOUT_SECONDS),
        ("PLAYWRIGHT_SELECTOR_TIMEOUT_SECONDS", config.PLAYWRIGHT_SELECTOR_TIMEOUT_SECONDS),
        ("PLAYWRIGHT_DOWNLOAD_TIMEOUT_SECONDS", config.PLAYWRIGHT_DOWNLOAD_TIMEOUT_SECONDS),
    ]

    for field_name, value in timeout_fields:
        if value <= 0:
            _raise_config_error(
                f"{field_name} must be greater than zero.",
                entrypoint=entrypoint,
                error="invalid_timeout",
                mode=mode,
            )


__all__ = ["validate_runtime_config", "Entrypoint"]
