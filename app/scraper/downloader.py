"""Download coordination utilities for the judicial scraper."""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from selenium.webdriver.remote.webdriver import WebDriver

from . import config
from .selenium_client import selenium_ajax_get_box_url
from .utils import (
    build_pdf_path,
    ensure_dirs,
    find_metadata_entry,
    hashed_fallback_path,
    is_duplicate,
    log_line,
    record_result,
)


def cookies_to_requests_session(cookies: dict[str, str], referer: str | None = None) -> requests.Session:
    """Build a requests session hydrated with Selenium cookies.

    Args:
        cookies: Cookie dictionary captured from Selenium.
        referer: Optional referer header to attach to the session.

    Returns:
        Configured requests session instance.
    """
    session = requests.Session()
    session.headers.update(config.COMMON_HEADERS)
    if referer:
        session.headers["Referer"] = referer
    for name, value in cookies.items():
        session.cookies.set(name, value)
    return session


def stream_pdf(session: requests.Session, url: str, out_path: Path) -> tuple[bool, str | None]:
    """Stream a PDF file to disk with validation.

    Args:
        session: Requests session carrying cookies/headers.
        url: Box download URL.
        out_path: Destination path on disk.

    Returns:
        Tuple of success flag and optional error message.
    """
    log_line(f"Streaming PDF from {url}")
    try:
        with session.get(url, stream=True, timeout=120) as response:
            if response.status_code >= 400:
                return False, f"HTTP {response.status_code}"
            with out_path.open("wb") as handle:
                first_chunk = True
                for chunk in response.iter_content(chunk_size=8192):
                    if not chunk:
                        continue
                    if first_chunk and not chunk.startswith(b"%PDF"):
                        handle.close()
                        out_path.unlink(missing_ok=True)
                        return False, "Response is not a PDF"
                    first_chunk = False
                    handle.write(chunk)
        if out_path.stat().st_size == 0:
            out_path.unlink(missing_ok=True)
            return False, "Empty file"
    except Exception as exc:  # noqa: BLE001
        out_path.unlink(missing_ok=True)
        return False, str(exc)
    return True, None


def attempt_download_case(
    driver: WebDriver,
    session: requests.Session,
    nonce: str,
    case: dict[str, Any],
    meta: dict[str, Any],
    per_delay: float | None = None,
) -> dict[str, Any]:
    """Attempt to download a single case PDF using Selenium and requests.

    Args:
        driver: Selenium WebDriver instance.
        session: Prepared requests session.
        nonce: AJAX security nonce.
        case: Case metadata dictionary.
        meta: Metadata dictionary (will be mutated on success).
        per_delay: Optional delay between downloads.

    Returns:
        Dictionary describing the attempt outcome.
    """
    ensure_dirs()
    fid = case["fid"]
    slug = case.get("fname") or fid
    log_line(f"Processing case {fid}: {case.get('title', 'Untitled')}")
    title = case.get("title") or ""
    cause_number = case.get("cause_number") or None

    out_path = build_pdf_path(
        config.PDF_DIR,
        title,
        action=slug,
        cause_number=cause_number,
    )

    entry, _ = find_metadata_entry(meta, slug=slug, fid=fid, filename=out_path.name)
    if entry and (entry.get("local_filename") or entry.get("filename")):
        existing_name = entry.get("local_filename") or entry.get("filename")
        if isinstance(existing_name, str) and existing_name.strip():
            out_path = config.PDF_DIR / existing_name

    if is_duplicate(fid, out_path.name, meta, slug=slug):
        log_line(
            f"Skipping {fid} because a verified download already exists"
        )
        return {
            "status": "skipped",
            "reason": "duplicate",
            "fid": fid,
            "filename": out_path.name,
        }

    box_url = selenium_ajax_get_box_url(driver, fid, case["fname"], nonce)
    if not box_url:
        fallback_url = f"https://judicial.ky/wp-content/uploads/box_files/{fid}.pdf"
        log_line(f"Falling back to direct URL {fallback_url}")
        box_url = fallback_url

    if not box_url.lower().startswith("http"):
        box_url = urljoin(config.DEFAULT_BASE_URL, box_url)

    log_line(f"Downloading using URL: {box_url}")
    success, error = stream_pdf(session, box_url, out_path)
    if success:
        size_kb = out_path.stat().st_size / 1024
        log_line(f"Saved {out_path.name} ({size_kb:.1f} KiB)")
        record_result(
            meta,
            slug=slug,
            fid=fid,
            title=case.get("title") or slug,
            local_filename=out_path.name,
            source_url=box_url,
            size_bytes=out_path.stat().st_size,
            category=case.get("category"),
            judgment_date=case.get("judgment_date"),
            court=case.get("court"),
            cause_number=case.get("cause_number"),
            subject=case.get("subject") or case.get("title"),
            local_path=str(out_path.resolve()),
        )
        result = {"status": "downloaded", "fid": fid, "filename": out_path.name}
    else:
        filename_error = str(error or "").lower()
        if "file name too long" in filename_error or "errno 36" in filename_error:
            fallback_path = hashed_fallback_path(config.PDF_DIR, title)
            log_line(
                f"Filename too long for {title!r}; retrying with fallback {fallback_path.name}"
            )
            success, retry_error = stream_pdf(session, box_url, fallback_path)
            if success:
                size_kb = fallback_path.stat().st_size / 1024
                log_line(f"Saved {fallback_path.name} ({size_kb:.1f} KiB) after fallback")
                record_result(
                    meta,
                    slug=slug,
                    fid=fid,
                    title=case.get("title") or slug,
                    local_filename=fallback_path.name,
                    source_url=box_url,
                    size_bytes=fallback_path.stat().st_size,
                    category=case.get("category"),
                    judgment_date=case.get("judgment_date"),
                    court=case.get("court"),
                    cause_number=case.get("cause_number"),
                    subject=case.get("subject") or case.get("title"),
                    local_path=str(fallback_path.resolve()),
                )
                result = {
                    "status": "downloaded",
                    "fid": fid,
                    "filename": fallback_path.name,
                }
            else:
                log_line(f"Failed to download {fid}: {retry_error}")
                fallback_path.unlink(missing_ok=True)
                result = {
                    "status": "failed",
                    "fid": fid,
                    "filename": fallback_path.name,
                    "error": retry_error,
                }
        else:
            log_line(f"Failed to download {fid}: {error}")
            out_path.unlink(missing_ok=True)
            result = {
                "status": "failed",
                "fid": fid,
                "filename": out_path.name,
                "error": error,
            }

    time.sleep(per_delay if per_delay is not None else config.PER_DOWNLOAD_DELAY)
    return result


__all__ = ["cookies_to_requests_session", "stream_pdf", "attempt_download_case"]
