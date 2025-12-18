from __future__ import annotations

from app.scraper import selectors_public_registers


def test_public_registers_selectors_defaults() -> None:
    selectors = selectors_public_registers.PUBLIC_REGISTERS_SELECTORS

    assert selectors.table_selector == "#public-registers"
    assert selectors.row_selector.endswith("tbody tr")
    assert "Download" in selectors.download_locator
    assert "data-reference" in selectors.token_attributes
    assert "data-register-token" in selectors.row_token_attributes
    assert "href" in selectors.href_attributes
