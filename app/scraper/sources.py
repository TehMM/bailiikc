from __future__ import annotations

"""Logical sources for the bailiikc scraper.

These values are persisted in the database (cases.source, params_json) and
should be treated as stable identifiers. If you change them, you MUST
provide a migration path for existing DBs.
"""

import logging

LOGGER = logging.getLogger("bailiikc")

UNREPORTED_JUDGMENTS = "unreported_judgments"
PUBLIC_REGISTERS = "public_registers"  # reserved for future use

# Default source for runs created by the current implementation.
DEFAULT_SOURCE = UNREPORTED_JUDGMENTS

# Helper tuple for validation / assertions.
ALL_SOURCES = (UNREPORTED_JUDGMENTS, PUBLIC_REGISTERS)

_DEFAULT_ALIASES = {"default", "unreported_judgments", "unreported-judgments", "uj", "unreported"}
_PUBLIC_REGISTERS_ALIASES = {"public-registers", "public_registers", "pr"}


def normalize_source(value: str | None) -> str:
    """Return a canonical logical source identifier.

    Unknown or empty values fall back to ``DEFAULT_SOURCE`` to keep behaviour
    safe while only ``unreported_judgments`` is actively supported.
    """

    if not value:
        return DEFAULT_SOURCE

    raw = value.strip().lower()
    if raw in ALL_SOURCES:
        return raw
    if raw in _DEFAULT_ALIASES:
        return UNREPORTED_JUDGMENTS
    if raw in _PUBLIC_REGISTERS_ALIASES:
        return PUBLIC_REGISTERS

    return DEFAULT_SOURCE


def coerce_source(raw: str | None) -> str:
    """Normalise a raw source value with basic validation and logging.

    Unknown or empty values fall back to ``DEFAULT_SOURCE`` while emitting a
    concise log line to aid debugging of misconfigured entrypoints.
    """

    if not raw:
        return DEFAULT_SOURCE

    normalized = normalize_source(raw)
    if normalized == DEFAULT_SOURCE and raw.strip().lower() not in _DEFAULT_ALIASES:
        LOGGER.warning("[SOURCES][WARN] Unknown source %r; using default.", raw)
    return normalized
