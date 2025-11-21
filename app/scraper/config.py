"""Configuration constants for the judicial scraper application."""
from __future__ import annotations

import os
from pathlib import Path

DATA_DIR: Path = Path("/app/data")
PDF_DIR: Path = DATA_DIR / "pdfs"
LOG_DIR: Path = DATA_DIR / "logs"
LOG_FILE: Path = LOG_DIR / "latest.log"
METADATA_FILE: Path = DATA_DIR / "metadata.json"
CONFIG_FILE: Path = DATA_DIR / "config.txt"
CHECKPOINT_PATH: Path = DATA_DIR / "state.json"
RUN_STATE_FILE: Path = DATA_DIR / "run_state.json"
DOWNLOADS_LOG: Path = DATA_DIR / "downloads.jsonl"
SUMMARY_FILE: Path = DATA_DIR / "last_summary.json"
HISTORY_ACTIONS_FILE: Path = DATA_DIR / "history_actions.json"
DB_PATH: Path = DATA_DIR / "bailiikc.db"

DEFAULT_BASE_URL: str = "https://judicial.ky/judgments/unreported-judgments/"
CSV_URL: str = "https://judicial.ky/wp-content/uploads/box_files/judgments.csv"
ZIP_NAME: str = "all_pdfs.zip"

PAGE_WAIT_SECONDS: int = int(os.getenv("PAGE_WAIT_SECONDS", "15"))
PER_DOWNLOAD_DELAY: float = float(os.getenv("PER_DOWNLOAD_DELAY", "1.0"))

SCRAPE_MODE_DEFAULT: str = (
    os.getenv("SCRAPE_MODE", "new").strip().lower() or "new"
)
SCRAPE_NEW_LIMIT: int = int(os.getenv("SCRAPE_NEW_LIMIT", "50"))
SCRAPE_NEW_CONSECUTIVE_LIMIT: int = int(os.getenv("SCRAPE_NEW_CONSECUTIVE_LIMIT", "30"))
SCRAPER_MAX_RETRIES: int = int(os.getenv("SCRAPER_MAX_RETRIES", "3"))
SCRAPE_RESUME_MAX_AGE_HOURS: int = int(os.getenv("SCRAPE_RESUME_MAX_AGE_HOURS", "24"))
SCRAPE_RESUME_DEFAULT: bool = os.getenv("SCRAPE_RESUME", "true").strip().lower() != "false"

MIN_FREE_MB: int = int(os.getenv("MIN_FREE_MB", "400"))
RESUME_ON_CRASH: bool = os.getenv("RESUME_ON_CRASH", "true").strip().lower() != "false"
NEW_ONLY_PAGES: int = int(os.getenv("NEW_ONLY_PAGES", "2"))
DOWNLOAD_TIMEOUT_S: int = int(os.getenv("DOWNLOAD_TIMEOUT_S", "120"))
DOWNLOAD_RETRIES: int = int(os.getenv("DOWNLOAD_RETRIES", "3"))

COMMON_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "X-Requested-With": "XMLHttpRequest",
}


def is_full_mode(mode: str) -> bool:
    """Return ``True`` when ``mode`` represents a full scrape request."""

    return str(mode).strip().lower() == "full"


def is_new_mode(mode: str) -> bool:
    """Return ``True`` when ``mode`` represents a new-only scrape request."""

    return str(mode).strip().lower() == "new"
