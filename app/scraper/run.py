"""High-level orchestration for scraping operations (Playwright-based)."""

from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

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

def _load_all_results(page, max_loadmore: int) -> None:
    """Click 'Load more' and scroll to reveal as many rows as possible."""
    page.wait_for_load_state("networkidle")

    selectors = [
        "button.pt-cv-loadmore",
        ".pt-cv-loadmore button",
        ".pt-cv-loadmore a",
        "button:has-text('Load more')",
        "button:has-text('Load More')",
    ]

    clicks = 0
    while clicks < max_loadmore:
        found = None
        for sel in selectors:
            try:
                loc = page.locator(sel)
                if loc.count() and loc.first.is_visible():
                    found = loc.first
                    break
            except Exception:
                continue

        if not found:
            break

        try:
            found.click()
            clicks += 1
            log_line(f"Clicked 'Load more' ({clicks})")
            page.wait_for_load_state("networkidle")
            time.sleep(0.4)
        except Exception as exc:  # noqa: BLE001
            log_line(f"'Load more' click failed: {exc}")
            break

    # Infinite scroll safety: nudge a few times
    last_h = 0
    for _ in range(20):
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(0.4)
            page.wait_for_load_state("networkidle")
            h = page.evaluate("document.body.scrollHeight")
        except Exception:
            break

        if h == last_h:
            break
        last_h = h


def _collect_buttons(page) -> List[Tuple[str, str, str]]:
    """
    Collect (fid, fname, security) triples from any elements that look like
    the live "Download" controls.

    We intentionally:
      * DO NOT hard-fail if a specific selector isn't found.
      * Accept any element with data-fid, and then read data-fname / data-s.
    """

    # Give the JS app a moment to render after all our scrolling / load-more
    try:
        page.wait_for_timeout(3000)
    except Exception:
        pass

    # Grab every element that advertises a fid; this is the most stable hook
    elements = page.query_selector_all("[data-fid]")
    log_line(f"Found {len(elements)} raw [data-fid] elements on page")

    if not elements:
        # Nothing matched at all; let the caller log a friendly message.
        return []

    # Try to discover a reusable nonce as fallback
    nonce_fallback = None

    # 1) Check any element with data-s
    for n in page.query_selector_all("[data-s]"):
        val = (n.get_attribute("data-s") or "").strip()
        if val and re.fullmatch(r"[A-Za-z0-9]+", val):
            nonce_fallback = val
            break

    # 2) Fallback: look inside inline scripts for dl_bfile / security token
    if not nonce_fallback:
        for s in page.query_selector_all("script"):
            txt = s.text_content() or ""
            m = re.search(
                r"dl_bfile[^A-Za-z0-9]+security[^A-Za-z0-9]+([A-Za-z0-9]{6,})",
                txt,
                flags=re.S | re.I,
            )
            if m:
                nonce_fallback = m.group(1)
                break

    if nonce_fallback:
        log_line(f"Using fallback nonce candidate: {nonce_fallback}")

    seen = set()
    out: List[Tuple[str, str, str]] = []

    for el in elements:
        fid = (el.get_attribute("data-fid") or "").strip()
        fname = (el.get_attribute("data-fname") or "").strip()
        sec = (el.get_attribute("data-s") or "" or nonce_fallback or "").strip()

        # Must have a fid and some security token to try dl_bfile
        if not fid or not sec:
            continue

        # Prefer fname from attribute; if missing, derive something minimal
        if not fname:
            # some implementations embed fname in data attributes or text; keep this cheap
            txt = (el.text_content() or "").strip().replace(" ", "")
            if txt:
                fname = txt
        if not fname:
            continue

        # Protect against old-style non-numeric IDs (we only want new AJAX ones)
        if not re.fullmatch(r"\d{5,}", fid):
            continue

        key = fid + "|" + fname
        if key in seen:
            continue
        seen.add(key)

        out.append((fid, fname, sec))

    log_line(f"Collected {len(out)} candidate download buttons after filtering")
    return out

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

    # ...leave the rest of the function body exactly as you have it now...

    # Load CSV cases (non-criminal only) for metadata & filtering
    cases = load_cases_from_csv(config.CSV_URL)
    meta = load_metadata()

    # Index by sanitized fname so we can join DOM buttons ↔ CSV rows
    cases_by_fname: Dict[str, Dict[str, Any]] = {}
    for c in cases:
        fname = c.get("fname")
        if not fname:
            continue
        cases_by_fname[fname] = c

    summary: Dict[str, Any] = {
        "base_url": base_url,
        "processed": 0,
        "downloaded": 0,
        "failed": 0,
        "skipped": 0,
        "total_cases": len(cases),
    }

    if not cases:
        log_line("No cases parsed from CSV (non-criminal set is empty). Aborting.")
        return summary

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=UA, locale="en-US")
        page = context.new_page()

        log_line("Opening judgments page in Playwright...")
        page.goto(base_url, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")
        if page_wait:
            page.wait_for_timeout(page_wait * 1000)

        # Reveal as many entries as possible
        _load_all_results(page, max_loadmore=200)

        buttons = _collect_buttons(page)
        if not buttons:
            log_line("No valid download buttons discovered on page.")
            browser.close()
            return summary

        api = context.request

        for fid, raw_fname, sec in buttons:
            if summary["processed"] >= entry_cap:
                break

            # The site's fname is already a clean key; normalise via sanitize
            sanitized = sanitize_filename(raw_fname)
            filename = (
                sanitized if sanitized.lower().endswith(".pdf") else f"{sanitized}.pdf"
            )
            out_path = config.PDF_DIR / filename

            # Only keep entries that exist in the non-criminal CSV
            case = cases_by_fname.get(sanitized) or cases_by_fname.get(raw_fname)
            if not case:
                log_line(
                    f"Skipping fid={fid} fname={raw_fname} "
                    f"(not found in non-criminal CSV)"
                )
                summary["skipped"] += 1
                continue

            summary["processed"] += 1

            if out_path.exists() or is_duplicate(fid, filename, meta):
                log_line(
                    f"Skipping fid={fid} ({filename}) – already downloaded/recorded"
                )
                summary["skipped"] += 1
                continue

            try:
                log_line(
                    f"Requesting Box URL for fid={fid} fname={raw_fname} via dl_bfile"
                )
                box_url = _fetch_box_url(api, fid, raw_fname, sec, referer=base_url)

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
                log_line(f"Failed fid={fid} ({raw_fname}): {exc}")
                if out_path.exists():
                    out_path.unlink(missing_ok=True)
                summary["failed"] += 1

            time.sleep(per_delay)

        browser.close()

    log_line(
        "Completed run: "
        "processed={processed} downloaded={downloaded} "
        "skipped={skipped} failed={failed}".format(**summary)
    )
    return summary


__all__ = ["run_scrape"]
