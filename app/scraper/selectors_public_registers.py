from __future__ import annotations

"""Selectors and attribute hints for the public registers source."""

from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class PublicRegistersSelectors:
    """Source-specific selector hints for ``public_registers``.

    The page exposes a dedicated table (#public-registers) with one download
    link per row. Tokens are typically carried on the link element itself, but
    some variants surface them on the row, so both sets of attribute probes are
    provided.
    """

    table_selector: str = "#public-registers"
    row_selector: str = "#public-registers tbody tr"
    download_locator: str = "a[data-dl], a[data-download], a:has-text('Download')"
    token_attributes: Tuple[str, ...] = (
        "data-dl",
        "data-reference",
        "data-register-token",
    )
    row_token_attributes: Tuple[str, ...] = (
        "data-register-token",
        "data-reference",
    )
    href_attributes: Tuple[str, ...] = ("data-download", "href")


PUBLIC_REGISTERS_SELECTORS = PublicRegistersSelectors()

__all__ = [
    "PublicRegistersSelectors",
    "PUBLIC_REGISTERS_SELECTORS",
]
