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
REPLAY_FIXTURES_DIR: Path = DATA_DIR / "replay_fixtures"
# SQLite database path (new infrastructure); JSON files above remain active
# for the current scraper implementation.
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

def _parse_timeout_seconds(env_var: str, default: int, *, minimum: int = 1) -> int:
    """Parse a timeout value in seconds from the environment with bounds."""

    try:
        value = int(os.getenv(env_var, str(default)))
    except ValueError:
        return default
    return max(minimum, value)


# Playwright timeouts (seconds)
# Navigation timeout for page.goto calls.
PLAYWRIGHT_NAV_TIMEOUT_SECONDS: int = _parse_timeout_seconds(
    "BAILIIKC_NAV_TIMEOUT_SECONDS", 25
)
# Selector waits (e.g., DataTable readiness, click targets).
PLAYWRIGHT_SELECTOR_TIMEOUT_SECONDS: int = _parse_timeout_seconds(
    "BAILIIKC_SELECTOR_TIMEOUT_SECONDS", 20
)
# Download timeout applied to Box/HTTP fetches.
PLAYWRIGHT_DOWNLOAD_TIMEOUT_SECONDS: int = _parse_timeout_seconds(
    "BAILIIKC_DOWNLOAD_TIMEOUT_SECONDS", 120
)
# Click-level timeout remains in milliseconds to match Playwright API expectations.
PLAYWRIGHT_CLICK_TIMEOUT_MS: int = int(os.getenv("PLAYWRIGHT_CLICK_TIMEOUT_MS", "2000"))

# Short sleeps (seconds) for click pacing and retry settle
PLAYWRIGHT_POST_CLICK_SLEEP_SECONDS: float = float(
    os.getenv("PLAYWRIGHT_POST_CLICK_SLEEP_SECONDS", "0.4")
)
PLAYWRIGHT_RETRY_PAGE_SETTLE_SECONDS: float = float(
    os.getenv("PLAYWRIGHT_RETRY_PAGE_SETTLE_SECONDS", "0.3")
)
PLAYWRIGHT_RETRY_AFTER_SWEEP_SECONDS: float = float(
    os.getenv("PLAYWRIGHT_RETRY_AFTER_SWEEP_SECONDS", "2.5")
)

# Concurrency controls
# Max number of Box downloads allowed in-flight at once (bounded executor).
MAX_PARALLEL_DOWNLOADS: int = int(os.getenv("BAILIIKC_MAX_PARALLEL_DOWNLOADS", "1"))
# Max queue depth before falling back to synchronous execution.
MAX_PENDING_DOWNLOADS: int = int(os.getenv("BAILIIKC_MAX_PENDING_DOWNLOADS", "100"))
# Global enable/disable for the download executor abstraction.
ENABLE_DOWNLOAD_EXECUTOR: bool = os.getenv(
    "BAILIIKC_ENABLE_DOWNLOAD_EXECUTOR", "1"
).strip().lower() not in {"0", "false"}

# Replay + offline controls
REPLAY_SKIP_NETWORK: bool = os.getenv("BAILIIKC_REPLAY_SKIP_NETWORK", "0").strip().lower() not in {
    "0",
    "false",
}
RECORD_REPLAY_FIXTURES: bool = os.getenv("BAILIIKC_RECORD_REPLAY_FIXTURES", "0").strip().lower() not in {
    "0",
    "false",
}

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


def use_db_worklist_for_new() -> bool:
    """Return True if new-mode worklists should come from SQLite."""

    return os.getenv("BAILIIKC_USE_DB_WORKLIST_FOR_NEW", "1") == "1"


def use_db_worklist_for_full() -> bool:
    """Return True if full-mode worklists should come from SQLite."""

    return os.getenv("BAILIIKC_USE_DB_WORKLIST_FOR_FULL", "1") == "1"


def use_db_worklist_for_resume() -> bool:
    """Return True if resume-mode worklists should come from SQLite."""

    return os.getenv("BAILIIKC_USE_DB_WORKLIST_FOR_RESUME", "1") == "1"


def use_db_reporting() -> bool:
    """Return True if DB-backed reporting should be preferred."""

    return os.getenv("BAILIIKC_USE_DB_REPORTING", "1") == "1"


def use_db_cases() -> bool:
    """Return True if DB-backed case indices should be preferred."""

    return os.getenv("BAILIIKC_USE_DB_CASES", "1") == "1"
