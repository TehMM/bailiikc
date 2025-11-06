"""High-level orchestration for scraping operations."""
from __future__ import annotations

from typing import Any

from . import config
from .downloader import attempt_download_case, cookies_to_requests_session
from .parser import load_cases_from_csv
from .selenium_client import get_nonce_and_cookies, make_driver
from .utils import ensure_dirs, load_metadata, log_line


def run_scrape(
    base_url: str,
    entry_cap: int | None = None,
    page_wait: int | None = None,
    per_delay: float | None = None,
) -> dict[str, Any]:
    """Execute a scraping run and return a summary.

    Args:
        base_url: Base URL of the judgments page.
        entry_cap: Maximum number of cases to process.
        page_wait: Seconds to wait for initial page load.
        per_delay: Delay between downloads.

    Returns:
        Dictionary summary containing totals and counts.
    """
    ensure_dirs()
    entry_cap = entry_cap or config.ENTRY_CAP
    page_wait = page_wait or config.PAGE_WAIT_SECONDS
    per_delay = per_delay if per_delay is not None else config.PER_DOWNLOAD_DELAY

    log_line("=== Starting scraping run ===")
    log_line(f"Target base URL: {base_url}")
    summary = {
        "base_url": base_url,
        "processed": 0,
        "downloaded": 0,
        "failed": 0,
        "skipped": 0,
        "total_cases": 0,
    }

    driver = make_driver()
    try:
        try:
            nonce, cookies = get_nonce_and_cookies(driver, base_url, page_wait)
        except Exception as exc:  # noqa: BLE001
            log_line(f"Failed to initialise Selenium session: {exc}")
            return summary

        session = cookies_to_requests_session(cookies, referer=base_url)
        cases = load_cases_from_csv(config.CSV_URL)
        summary["total_cases"] = len(cases)
        target_cases = cases[: entry_cap]
        log_line(f"Processing {len(target_cases)} cases (cap={entry_cap})")

        metadata = load_metadata()
        for case in target_cases:
            try:
                result = attempt_download_case(
                    driver, session, nonce, case, metadata, per_delay=per_delay
                )
            except Exception as exc:  # noqa: BLE001
                log_line(f"Unexpected error downloading {case.get('fid')}: {exc}")
                summary["processed"] += 1
                summary["failed"] += 1
                continue

            summary["processed"] += 1
            if result["status"] == "downloaded":
                summary["downloaded"] += 1
            elif result["status"] == "failed":
                summary["failed"] += 1
            else:
                summary["skipped"] += 1
    finally:
        driver.quit()
        log_line("Closed Selenium driver")

    log_line(
        "Completed run: processed={processed} downloaded={downloaded} skipped={skipped} failed={failed}".format(
            **summary
        )
    )
    return summary


__all__ = ["run_scrape"]
