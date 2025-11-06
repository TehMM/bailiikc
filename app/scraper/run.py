"""High-level orchestration for scraping operations (Playwright-based)."""

from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright

from . import config
from .parser import load_cases_from_csv
from .utils import (
    ensure_dirs,
    load_metadata,
    log_line,
    is_duplicate,
    record_result,
    sanitize_filename,
)

ADMIN_AJAX = "https://judicial.ky/wp-admin/admin-ajax.php"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/142.0.0.0 Safari/537.36"
)


# ---------- Playwright helpers ----------

_NONCE_PATTERNS = [
    re.compile(r"['\"](?:_?nonce|security)['\"]\s*[:=]\s*['\"]([A-Za-z0-9]{8,})['\"]"),
    re.compile(r"dl_bfile[^A-Za-z0-9]+['\"]([A-Za-z0-9]{8,})['\"]", re.I),
]


def _extract_nonce(page_html: str) -> str:
    """Attempt to locate the AJAX security nonce within rendered HTML."""

    for pattern in _NONCE_PATTERNS:
        match = pattern.search(page_html)
        if match:
            return match.group(1)

    raise RuntimeError("Failed to locate AJAX security nonce on the page")


def _fetch_box_url(api, fid: str, fname: str, security: str, referer: str) -> str:
    """Call the same dl_bfile AJAX endpoint the site uses to get a Box URL."""
    res = api.post(
        ADMIN_AJAX,
        form={
            "action": "dl_bfile",
            "fid": fid,
            "fname": fname,
            "security": security,
        },
        headers={
            "Accept": "*/*",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://judicial.ky",
            "Referer": referer,
            "X-Requested-With": "XMLHttpRequest",
        },
        timeout=30_000,
    )

    if res.status != 200:
        raise RuntimeError(f"AJAX HTTP {res.status}")

    try:
        payload = res.json()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"AJAX non-JSON response: {exc}") from exc

    if payload in (-1, "-1"):
        raise RuntimeError("AJAX returned -1 (invalid nonce)")

    if (
        not isinstance(payload, dict)
        or not payload.get("success")
        or "data" not in payload
        or "fid" not in payload["data"]
    ):
        raise RuntimeError(f"AJAX payload malformed: {payload}")

    box_url = str(payload["data"]["fid"]).replace("\\/", "/")
    return box_url


def _stream_pdf(api, url: str, out_path: Path) -> None:
    """Download the PDF via Playwright's request context and save to disk."""
    res = api.get(url, timeout=120_000)

    if res.status not in (200, 206):
        raise RuntimeError(f"Download status {res.status}")

    body = res.body()
    if not body.startswith(b"%PDF"):
        raise RuntimeError("Response is not a PDF")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(body)

    if out_path.stat().st_size == 0:
        out_path.unlink(missing_ok=True)
        raise RuntimeError("Empty PDF")


# ---------- Public API ----------

def run_scrape(
    base_url: str | None = None,
    entry_cap: int | None = None,
    page_wait: int | None = None,
    per_delay: float | None = None,
) -> Dict[str, Any]:
    """
    Execute a scraping run using Playwright + dl_bfile AJAX.

    Called by the /scrape route. All params optional; sensible defaults
    pulled from config.
    """
    ensure_dirs()

    base_url = (base_url or config.DEFAULT_BASE_URL).strip()
    entry_cap = entry_cap or config.ENTRY_CAP
    page_wait = page_wait or config.PAGE_WAIT_SECONDS
    per_delay = per_delay if per_delay is not None else config.PER_DOWNLOAD_DELAY

    log_line("=== Starting scraping run (Playwright) ===")
    log_line(f"Target base URL: {base_url}")
    log_line(
        f"Params: entry_cap={entry_cap} page_wait={page_wait}s "
        f"per_download_delay={per_delay}s"
    )

    # Load CSV cases (non-criminal only) for metadata & filtering
    cases = load_cases_from_csv(config.CSV_URL)
    meta = load_metadata()

    summary: Dict[str, Any] = {
        "base_url": base_url,
        "processed": 0,
        "downloaded": 0,
        "failed": 0,
        "skipped": 0,
        "total_cases": len(cases),
        "error": None,
    }

    if not cases:
        log_line("No cases parsed from CSV (non-criminal set is empty). Aborting.")
        return summary

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=UA, locale="en-US")

        try:
            page = context.new_page()

            log_line("Opening judgments page in Playwright...")
            page.goto(base_url, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")
            if page_wait:
                page.wait_for_timeout(page_wait * 1000)

            page_html = page.content()
            try:
                nonce = _extract_nonce(page_html)
            except Exception as exc:  # noqa: BLE001
                log_line(f"Failed to extract AJAX nonce: {exc}")
                summary["error"] = f"nonce_error: {exc}"
                log_line("Aborting scrape due to nonce extraction failure")
                return summary

            log_line(f"Using AJAX security nonce: {nonce}")

            api = context.request

            for case in cases:
                if summary["processed"] >= entry_cap:
                    break

                fid = case.get("fid")
                fname = case.get("fname") or fid
                if not fid or not fname:
                    log_line(
                        "Skipping case with missing fid/fname: "
                        f"{case.get('title', 'Untitled')}"
                    )
                    summary["skipped"] += 1
                    continue

                sanitized = sanitize_filename(fname)
                filename = (
                    sanitized if sanitized.lower().endswith(".pdf") else f"{sanitized}.pdf"
                )
                out_path = config.PDF_DIR / filename

                if out_path.exists() or is_duplicate(fid, filename, meta):
                    log_line(f"Skipping {fid} ({filename}) – already downloaded/recorded")
                    summary["skipped"] += 1
                    continue

                summary["processed"] += 1

                box_url = None

                try:
                    log_line(
                        f"Requesting Box URL for fid={fid} fname={fname} via dl_bfile"
                    )
                    box_url = _fetch_box_url(api, fid, fname, nonce, referer=base_url)
                except Exception as exc:  # noqa: BLE001
                    log_line(f"AJAX lookup failed for fid={fid}: {exc}")

                if not box_url:
                    fallback_url = (
                        f"https://judicial.ky/wp-content/uploads/box_files/{fid}.pdf"
                    )
                    log_line(f"Falling back to direct URL {fallback_url}")
                    box_url = fallback_url

                if not box_url.lower().startswith("http"):
                    box_url = urljoin(base_url, box_url)

                try:
                    log_line(f"Streaming PDF from {box_url}")
                    _stream_pdf(api, box_url, out_path)

                    size_bytes = out_path.stat().st_size

                    record_result(
                        meta,
                        fid=fid,
                        filename=filename,
                        fields={
                            "title": case.get("title"),
                            "category": case.get("category"),
                            "judgment_date": case.get("judgment_date"),
                            "source_url": box_url,
                            "size_bytes": size_bytes,
                        },
                    )

                    log_line(
                        f"Saved {filename} ({size_bytes / 1024:.1f} KiB) "
                        f"for case '{case.get('title', '').strip()}'"
                    )
                    summary["downloaded"] += 1

                except Exception as exc:  # noqa: BLE001
                    log_line(f"Failed fid={fid} ({filename}): {exc}")
                    if out_path.exists():
                        out_path.unlink(missing_ok=True)
                    summary["failed"] += 1

                time.sleep(per_delay)
        finally:
            try:
                context.close()
            finally:
                browser.close()

    log_line(
        "Completed run: "
        "processed={processed} downloaded={downloaded} "
        "skipped={skipped} failed={failed}".format(**summary)
    )
    return summary


__all__ = ["run_scrape"]
