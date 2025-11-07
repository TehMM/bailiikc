"""Playwright-based scraper for Cayman Unreported Judgments.

Workflow:

- Load judgments.csv (non-criminal cases).
- Open https://judicial.ky/judgments/unreported-judgments/ in Playwright.
- Scroll to trigger the JS widget.
- Click the real download buttons:

      <button class="btn p-2 btn-outline-primary lh-1"
              data-dl="FSD0151202511062025ATPLIFESCIENCE">
          <i class="icon-dl fs-6 lh-1"></i>
      </button>

- Listen for wp-admin/admin-ajax.php POSTs (dl_bfile).
- For each successful response, extract the Box download URL, stream the PDF,
  and record metadata.

This is wired to /scrape via run_scrape().
"""

from __future__ import annotations

import time
import urllib.parse
from typing import Any, Dict, List, Optional, Set

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
    """Best-effort click-through for cookie banners."""
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
            if loc.count():
                loc.click(timeout=1500)
                page.wait_for_timeout(400)
                log_line(f"Clicked cookie banner via {sel}")
                return
        except Exception:
            continue


def _load_all_results(page: Page, max_scrolls: int = 40) -> None:
    """Scroll a few times to trigger lazy-loading."""
    try:
        page.wait_for_load_state("networkidle", timeout=20_000)
    except PWTimeout:
        log_line("Initial networkidle timeout; continuing.")

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
    """
    Locators that should match the real Unreported Judgments download buttons.
    """
    return [
        # Primary: explicit data-dl buttons (what we see on the page)
        "button[data-dl]",
        "[data-dl]",

        # Icon-based: anything containing the download icon
        "button:has(i.icon-dl)",
        "a:has(i.icon-dl)",

        # Fallbacks for possible future HTML tweaks
        "a:has-text('Download')",
        "button:has-text('Download')",
        "a:has-text(/download/i)",
        "button:has-text(/download/i)",
        "[aria-label*='Download' i]",
        "[title*='Download' i]",
        "a[class*='download' i]",
        "button[class*='download' i]",
        "[onclick*='dl_bfile']",
    ]


def _screenshot(page: Page, name: str = "unreported_judgments.png") -> None:
    """Save a screenshot under PDF_DIR for debugging."""
    path = config.PDF_DIR / name
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(path), full_page=True)
        log_line(f"Saved debug screenshot -> {path}")
    except Exception as exc:
        log_line(f"Failed to save debug screenshot: {exc}")


def _extract_box_url(data: Dict[str, Any]) -> Optional[str]:
    """
    Extract Box download URL from dl_bfile payload.
    Different plugins/sites sometimes rename the key; be flexible.
    """
    candidates = [
        data.get("fid"),
        data.get("url"),
        data.get("download_url"),
        data.get("link"),
    ]
    for raw in candidates:
        if not raw:
            continue
        url = str(raw).replace("\\/", "/").strip()
        if url.startswith("http"):
            return url
    return None


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------

def run_scrape(
    base_url: Optional[str] = None,
    entry_cap: Optional[int] = None,
    page_wait: Optional[int] = None,
    per_delay: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Execute a scraping run using Playwright + captured dl_bfile AJAX responses.
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

    # Load CSV (non-criminal only)
    cases = load_cases_from_csv(config.CSV_URL)
    meta = load_metadata()

    cases_by_fname: Dict[str, Dict[str, Any]] = {
        str(c.get("fname")).strip(): c
        for c in cases
        if c.get("fname")
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
        log_line("No non-criminal cases loaded from CSV; aborting.")
        return summary

    seen_fnames_in_run: Set[str] = set()

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

        # Mild stealth
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )

        page: Page = context.new_page()

        # ---------- RESPONSE HOOK ----------

        def on_response(resp: Response) -> None:
            try:
                url = resp.url
            except Exception as exc:
                log_line(f"[AJAX] error reading url: {exc}")
                return

            if "admin-ajax.php" in url:
                try:
                    method = resp.request.method
                except Exception as exc:
                    method = f"? ({exc})"
                log_line(
                    f"[AJAX] url={url} status={resp.status} method={method}"
                )

            if not url.startswith(ADMIN_AJAX):
                return

            try:
                req: Request = resp.request
                if req.method != "POST":
                    return

                body = req.post_data or ""
                log_line(f"[AJAX] raw POST body={body!r}")

                qs = urllib.parse.parse_qs(body)
                action = (qs.get("action", [""])[0] or "").strip()
                fid_param = (qs.get("fid", [""])[0] or "").strip()
                fname_param = (qs.get("fname", [""])[0] or "").strip()
                security = (qs.get("security", [""])[0] or "").strip()

                log_line(
                    f"[AJAX] parsed action={action!r} fid={fid_param!r} "
                    f"fname={fname_param!r} security={security!r}"
                )

                if action != "dl_bfile":
                    # Other admin-ajax actions are noise for us
                    return

                # Some implementations may omit fname; if so, try to recover later.
                if not fname_param:
                    log_line("[AJAX] dl_bfile without fname; skipping.")
                    return

                if fname_param in seen_fnames_in_run:
                    log_line(
                        f"[AJAX] fname {fname_param} already processed this run."
                    )
                    return

                # Parse JSON payload
                try:
                    payload = resp.json()
                except Exception as exc:
                    log_line(f"[AJAX] dl_bfile non-JSON response: {exc}")
                    return

                log_line(f"[AJAX] dl_bfile payload={payload!r}")

                if (
                    payload in (-1, "-1")
                    or not isinstance(payload, dict)
                    or not payload.get("success")
                ):
                    log_line(
                        f"[AJAX] dl_bfile failure for fname={fname_param}: {payload}"
                    )
                    return

                data = payload.get("data") or {}
                box_url = _extract_box_url(data)
                if not box_url:
                    log_line(
                        f"[AJAX] Could not find Box URL in payload for "
                        f"fname={fname_param}: {data!r}"
                    )
                    return

                # Map fname -> CSV row
                case = (
                    cases_by_fname.get(fname_param)
                    or cases_by_fname.get(sanitize_filename(fname_param))
                )
                if not case:
                    log_line(
                        f"[AJAX] fname not found in CSV: {fname_param}; skipping."
                    )
                    return

                safe_root = sanitize_filename(fname_param)
                safe_name = (
                    safe_root
                    if safe_root.lower().endswith(".pdf")
                    else f"{safe_root}.pdf"
                )
                out_path = config.PDF_DIR / safe_name

                if out_path.exists() or is_duplicate(fid_param, safe_name, meta):
                    log_line(
                        f"[AJAX] Already have {safe_name}; skipping download."
                    )
                    seen_fnames_in_run.add(fname_param)
                    summary["skipped"] += 1
                    return

                # Download PDF
                try:
                    log_line(
                        f"[AJAX] Streaming PDF for {fname_param} from {box_url}"
                    )
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
                        fid=fid_param or fname_param,
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
                        f"[AJAX] Saved {safe_name} ({size_bytes/1024:.1f} KiB) "
                        f"for case '{(case.get('title') or '').strip()}'"
                    )
                    seen_fnames_in_run.add(fname_param)
                    summary["downloaded"] += 1

                except Exception as exc:
                    log_line(
                        f"[AJAX] Failed to save PDF for {fname_param}: {exc}"
                    )
                    if out_path.exists():
                        out_path.unlink(missing_ok=True)
                    summary["failed"] += 1

            except Exception as exc:
                # Catch any bug in our handler so it never kills Playwright
                log_line(f"[AJAX] handler error: {exc}")

        context.on("response", on_response)

        # ---------- Navigate & initialise ----------

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
        _screenshot(page)

        # ---------- Click download buttons to trigger dl_bfile ----------

        locators = _guess_download_locators()
        clicked = 0
        max_clicks = entry_cap

        for sel in locators:
            if clicked >= max_clicks:
                break

            try:
                items = page.locator(sel)
                count = items.count()
            except Exception as exc:
                log_line(f"Locator {sel!r} lookup failed: {exc}")
                continue

            if not count:
                continue

            log_line(f"Locator {sel!r} matched {count} elements")

            for i in range(count):
                if clicked >= max_clicks:
                    break

                try:
                    el = items.nth(i)

                    # Try normal click; if Playwright internals are broken, fall back to JS.
                    try:
                        el.click(timeout=2000)
                    except Exception as exc_click:
                        log_line(
                            f"Playwright click failed for {sel!r} index {i}: "
                            f"{exc_click}; trying JS click."
                        )
                        try:
                            el.evaluate("el => el.click()")
                        except Exception as exc_js:
                            log_line(
                                f"JS click also failed for {sel!r} index {i}: "
                                f"{exc_js}"
                            )
                            continue

                    clicked += 1
                    summary["processed"] = clicked
                    log_line(f"Clicked element {i} for selector {sel!r}")

                    # Wait a bit so dl_bfile AJAX + handler can run.
                    time.sleep(per_delay + 0.4)

                except Exception as exc:
                    log_line(
                        f"Unexpected error for selector {sel!r} index {i}: {exc}"
                    )
                    continue

        # Final grace period for late AJAX responses
        time.sleep(2.5)

        log_line(
            f"Clicks attempted: {clicked}. "
            f"Downloads={summary['downloaded']}, "
            f"Failed={summary['failed']}, "
            f"Skipped={summary['skipped']}"
        )

        browser.close()

    log_line("Completed run (response-capture strategy).")
    return summary


__all__ = ["run_scrape"]
