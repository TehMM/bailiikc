# app/scraper/run.py
from pathlib import Path
import time
from typing import Optional, Callable

from app.scraper.utils import ensure_dirs, paths
from app.scraper.playwright_downloader import download_all

def _file_logger() -> Callable[[str], None]:
    log_file = paths().scrape_log
    log_file.parent.mkdir(parents=True, exist_ok=True)
    def _log(msg: str):
        ts = time.strftime("[%Y-%m-%d %H:%M:%S]")
        line = f"{ts} {msg}"
        print(line, flush=True)
        with log_file.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    return _log

def run_scrape(filter_pred: Optional[Callable[[str, str], bool]] = None) -> dict:
    """
    Kick off the Playwright-based download. Returns a summary dict.
    `filter_pred` can be used to restrict which (fid, fname) to download.
    """
    ensure_dirs()
    out_dir = paths().pdf_dir
    logger = _file_logger()
    logger("=== New scrape session (Playwright) ===")
    result = download_all(
        out_dir=out_dir,
        headless=True,
        max_loadmore=200,
        delay_sec=0.6,
        filter_pred=filter_pred,
        logger=logger,
    )
    logger(f"Summary: {result}")
    return result
