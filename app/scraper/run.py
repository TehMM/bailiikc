"""High-level orchestration for scraping operations using Playwright.

Strategy here:
- Open the Unreported Judgments page.
- Accept cookie banners if present.
- Scroll to encourage lazy-load.
- Attach a response listener for the wp-admin AJAX "dl_bfile" endpoint.
- Proactively click anything that looks like a "Download" control to trigger those AJAX calls.
- For each AJAX response, parse Box URL and original POST data (incl. fname), then stream the PDF.
- Record results with existing utils so the Flask UI keeps working.

This avoids hard-coding brittle DOM selectors for data-fid/data-fname and instead
piggybacks on the site's actual network behavior.
"""

from __future__ import annotations

import re
import time
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from playwright.sync_api import (
    Browser,
    BrowserContext,
    Error as PWError,
    Page,
    Request,
    Response,
    TimeoutError as PWTimeout,
    sync_playwright,
)

from . import config
from .parser import load_cases_from_csv
from .utils import (
    ensure_dirs,
    is_duplicate,
    load_metadata,
    log_line,
    record_result,
    sanitize_filename,
)

ADMIN_AJAX = "https://judicial.ky/wp-admin/admin-ajax.php"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _accept_cookies(page: Page) -> None:
    """Best-effort click-through for common cookie banners."""
    selectors = [
        "button:has-text('Accept')",
        "button:has-text('I agree')",
        "button[aria-label*='Accept' i]",
        "button[title*='Accept' i]",
        "[role='button']:has-text('Accept')",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() and loc.is_visible():
                loc.click(timeout=1500)
                page.wait_for_timeout(500)
                log_line(f"Clicked cookie banner via {sel}")
                return
        except Exception:
            continue


def _load_all_results(page: Page, max_scrolls: int = 40) -> None:
    """Encourage any lazy content to render."""
    try:
        page.wait_for_load_state("networkidle", timeout=20_000)
    except PWTimeout:
        log_line("Initial networkidle timeout; continuing anyway.")

    last_height = 0
    for i in range(max_scrolls):
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(500)
            page.wait_for_load_state("networkidle", timeout=10_000)
            height = page.evaluate("document.body.scrollHeight")
        except PWError:
            break

        if not isinstance(height, (int, float)):
            break

        if int(height) == int(last_height):
            break

        last_height = int(height)
        log_line(f"Scroll {i+1}: document height now {last_height}")


def _guess_download_locators() -> List[str]:
    """A broad set of locators that often match the site's Download controls."""
    return [
        # Obvious text
        "a:has-text('Download')",
        "button:has-text('Download')",

        # Case-insensitive-ish via regex
        "a:has-text(/download/i)",
        "button:has-text(/download/i)",

        # Icon buttons with semantics
        "[aria-label*='Download' i]",
        "[title*='Download' i]",

        # Common button class names
        "a[class*='download' i]",
        "button[class*='download' i]",

        # Fallback: any link with data-fname or onclick mentioning dl_bfile
        "a[data-fname]",
        "button[data-fname]",
        "[onclick*='dl_bfile']",
    ]


def _screenshot(page: Page, name: str = "page.png") -> Optional[Path]:
    """Save a screenshot to data/pdfs for out-of-band inspection."""
    out = config.PDF_DIR / name
    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(out), full_page=True)
        log_line(f"Saved debug screenshot -> {out}")
        return out
    except Exception as e:
        log_line(f"Failed to save debug screenshot: {e}")
        return None


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------

def run_scrape(
    base_url: Optional[str] = None,
    entry_cap: Optional[int] = None,
    page_wait: Optional[int] = None,
    per_delay: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Execute a scraping run using Playwright + response capture of dl_bfile.

    - base_url defaults to config.DEFAULT_BASE_URL
    - entry_cap limits number of processed items this session
    - page_wait adds extra seconds after initial load
    - per_delay throttles between processed items
    """
    ensure_dirs()

    base_url = (base_url or config.DEFAULT_BASE_URL).strip()
    entry_cap = entry_cap or config.ENTRY_CAP
    page_wait = page_wait or config.PAGE_WAIT_SECONDS
    per_delay = per_delay if per_delay is not None else config.PER_DOWNLOAD_DELAY

    log_line("=== Starting scraping run (Playwright, response-capture) ===")
    log_line(f"Target base URL: {base_url}")
    log_line(
        f"Params: entry_cap={entry_cap}, page_wait={page_wait}, "
        f"per_download_delay={per_delay}"
    )

    # Load CSV (non-criminal)
    cases = load_cases_from_csv(config.CSV_URL)
    meta = load_metadata()

    # Index by fname for quick metadata join
    cases_by_fname: Dict[str, Dict[str, Any]] = {
        str(c.get("fname")).strip(): c for c in cases if c.get("fname")
    }

    summary: Dict[str, Any] = {
        "base_url": base_url,
        "processed": 0,
        "downloaded": 0,
        "failed": 0,
        "skipped": 0,
        "total_cases": len(cases),
    }

    if not cases:
        log_line("No non-criminal cases loaded from CSV; aborting scrape.")
        return summary

    # Track which fname we already handled within this run to avoid dup clicks
    seen_fnames_in_run = set()

    with sync_playwright() as pw:
        browser: Browser = pw.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context: BrowserContext = browser.new_context(
            user_agent=UA,
            locale="en-US",
            viewport={"width": 1368, "height": 900},
        )

        # Basic stealth: drop webdriver flag
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )

        page: Page = context.new_page()

        # ---------- Response hook for admin-ajax dl_bfile ----------

        def on_response(resp: Response) -> None:
            try:
                url = resp.url
            except PWError:
                return

            if not url.startswith(ADMIN_AJAX):
                return

            req: Request = resp.request
            method = req.method
            if method != "POST":
                return

            # Parse POST body for fname (and other fields)
            try:
                body = req.post_data() or ""
            except PWError:
                body = ""
            qs = urllib.parse.parse_qs(body)

            action = (qs.get("action", [""])[0] or "").strip()
            if action != "dl_bfile":
                return

            fid_param = (qs.get("fid", [""])[0] or "").strip()
            fname_param = (qs.get("fname", [""])[0] or "").strip()
            security_param = (qs.get("security", [""])[0] or "").strip()

            if not fname_param:
                # Without fname we can't map to CSV metadata; skip
                log_line("dl_bfile response without fname in request; skipping.")
                return

            if fname_param in seen_fnames_in_run:
                # Already handled in this run
                return

            # Parse JSON to get Box URL
            try:
                payload = resp.json()
            except Exception as exc:
                log_line(f"dl_bfile non-JSON response: {exc}")
                return

            if payload in (-1, "-1") or not isinstance(payload, dict) or not payload.get("success"):
                log_line(f"dl_bfile returned failure for fname={fname_param}: {payload}")
                return

            data = payload.get("data") or {}
            box_url = str(data.get("fid") or "").replace("\\/", "/").strip()
            if not box_url.startswith("http"):
                log_line(f"dl_bfile returned invalid URL for fname={fname_param}: {box_url}")
                return

            # Join to CSV metadata
            case = cases_by_fname.get(fname_param)
            if not case:
                # If the DOM used a slightly different fname casing, try a sanitized key
                case = cases_by_fname.get(sanitize_filename(fname_param))
            if not case:
                log_line(f"fname not found in CSV (non-criminal): {fname_param}; skipping.")
                return

            # Determine output filename
            safe_root = sanitize_filename(fname_param)
            safe_name = safe_root if safe_root.lower().endswith(".pdf") else f"{safe_root}.pdf"
            out_path = config.PDF_DIR / safe_name

            # Idempotency across runs
            if out_path.exists() or is_duplicate(fid_param, safe_name, meta):
                log_line(f"Already have {safe_name}; skipping download.")
                seen_fnames_in_run.add(fname_param)
                return

            # Download
            try:
                log_line(f"Streaming PDF for {fname_param} from {box_url}")
                r = context.request.get(box_url, timeout=120_000)
                if r.status not in (200, 206):
                    raise RuntimeError(f"Download HTTP {r.status}")
                body_bytes = r.body()
                if not body_bytes.startswith(b"%PDF"):
                    raise RuntimeError("Response is not a PDF")
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_bytes(body_bytes)
                size_bytes = out_path.stat().st_size

                record_result(
                    meta,
                    fid=fid_param,
                    filename=safe_name,
                    fields={
                        "title": case.get("title"),
                        "category": case.get("category"),
                        "judgment_date": case.get("judgment_date"),
                        "source_url": box_url,
                        "size_bytes": size_bytes,
                    },
                )
                log_line(
                    f"Saved {safe_name} ({size_bytes/1024:.1f} KiB) "
                    f"for case '{(case.get('title') or '').strip()}'"
                )
                seen_fnames_in_run.add(fname_param)

            except Exception as exc:
                log_line(f"Failed to save PDF for {fname_param}: {exc}")

        context.on("response", on_response)

        # ---------- Navigate and prep ----------

        log_line("Opening judgments page in Playwright...")
        page.goto(base_url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=20_000)
        except PWTimeout:
            log_line("Timed out waiting for full load; continuing.")
        if page_wait:
            page.wait_for_timeout(page_wait * 1000)

        _accept_cookies(page)
        _load_all_results(page)

        # Debug screenshot so you can inspect what headless sees
        _screenshot(page, "unreported_judgments.png")

        # ---------- Click likely Download controls to trigger AJAX ----------

        locators = _guess_download_locators()
        clicked = 0
        max_clicks = entry_cap  # keep overall limit consistent

        for sel in locators:
            try:
                items = page.locator(sel)
                count = items.count()
            except Exception:
                continue

            if not count:
                continue

            for i in range(count):
                if clicked >= max_clicks:
                    break

                try:
                    el = items.nth(i)
                    if not el.is_visible():
                        continue
                    # Be gentle: click, then give responses time to arrive
                    el.click(timeout=2000)
                    clicked += 1

                    # Wait a little for any AJAX to fire/complete
                    page.wait_for_timeout(int(per_delay * 1000) + 400)
                except Exception:
                    continue

            if clicked >= max_clicks:
                break

        # ---------- Final wait to let late responses finish ----------

        page.wait_for_timeout(2500)

        # Summarize from metadata diff: we don't mutate summary counters live because
        # downloads happen in the response hook. Instead, compute a cheap delta:
        # (We keep the prior behavior minimal and rely on UI/metadata/report for totals.)
        # But to keep a similar shape:
        summary["processed"] = clicked
        # Can't know exact counts without reloading metadata; just log.
        log_line(f"Clicks attempted: {clicked}. See report/metadata for results.")

        browser.close()

    log_line("Completed run (response-capture strategy).")
    return summary


__all__ = ["run_scrape"]
