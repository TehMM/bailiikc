from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PublicRegistersSelectors:
    """Selectors for the public registers listing.

    These are intentionally minimal and act as stubs for future refinement
    while keeping ``run.py`` free from public-registers-specific strings.
    """

    table_selector: str = "#public-registers"
    row_selector: str = "#public-registers tbody tr"
    download_button_selector: str = "button"

