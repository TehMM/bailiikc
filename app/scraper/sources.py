from __future__ import annotations

"""Logical sources for the bailiikc scraper.

These values are persisted in the database (cases.source, params_json) and
should be treated as stable identifiers. If you change them, you MUST
provide a migration path for existing DBs.
"""

UNREPORTED_JUDGMENTS = "unreported_judgments"
PUBLIC_REGISTERS = "public_registers"  # reserved for future use

# Default source for runs created by the current implementation.
DEFAULT_SOURCE = UNREPORTED_JUDGMENTS

# Helper tuple for validation / assertions.
ALL_SOURCES = (UNREPORTED_JUDGMENTS, PUBLIC_REGISTERS)


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
    if raw in {"uj", "unreported"}:
        return UNREPORTED_JUDGMENTS
    if raw in {"pr", "public-registers", "public_registers"}:
        return PUBLIC_REGISTERS

    return DEFAULT_SOURCE
