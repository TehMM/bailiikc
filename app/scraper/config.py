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

DEFAULT_BASE_URL: str = "https://judicial.ky/judgments/unreported-judgments/"
CSV_URL: str = "https://judicial.ky/wp-content/uploads/box_files/judgments.csv"
ZIP_NAME: str = "all_pdfs.zip"

PAGE_WAIT_SECONDS: int = int(os.getenv("PAGE_WAIT_SECONDS", "15"))
ENTRY_CAP: int = int(os.getenv("ENTRY_CAP", "25"))
PER_DOWNLOAD_DELAY: float = float(os.getenv("PER_DOWNLOAD_DELAY", "1.0"))

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
