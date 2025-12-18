from __future__ import annotations

from app.scraper import run, sources


def test_selectors_for_unreported_judgments() -> None:
    selectors = run._selectors_for_source(sources.UNREPORTED_JUDGMENTS)

    assert selectors.table_selector == "#judgment-registers"
    assert selectors.row_selector.endswith("tbody tr")
    assert "data-dl" in selectors.token_attributes
    assert "data-fname" in selectors.row_token_attributes
    assert "href" in selectors.href_attributes
    assert "button[data-dl]" in selectors.download_locator
