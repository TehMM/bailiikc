"""High-level orchestration for scraping operations using Playwright.

This module is wired to the /scrape endpoint and is responsible for:

- Loading the Unreported Judgments page.
- Discovering judgment download controls (including inside iframes).
- Calling the wp-admin/admin-ajax.php dl_bfile endpoint.
- Streaming PDFs from Box.com to data/pdfs.
- Recording results in metadata.json for the UI/report.
"""

from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from playwright.sync_api import (
    Browser,
    BrowserContext,
    Error as PWError,
    Page,
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
# Frame helpers
# ---------------------------------------------------------------------------


def _same_origin_frames(page: Page) -> Iterable[Page]:
    """Yield the main page and all same-origin iframes."""
    # Main page
    yield page

    # Any same-origin iframes (where the JS app may live)
    for frame in page.frames:
        try:
            url = frame.url or ""
        except PWError:
            continue

        if url.startswith("https://judicial.ky"):
            yield frame


# ---------------------------------------------------------------------------
# Discovery helpers
# ---------------------------------------------------------------------------


def _discover_nonce(frame: Page) -> Optional[str]:
    """Try to discover a security nonce within a frame."""
    # Direct data-s attributes
    try:
        nodes = frame.query_selector_all("[data-s]")
    except PWError:
        nodes = []

    for n in nodes:
        try:
            val = (n.get_attribute("data-s") or "").strip()
        except PWError:
            continue
        if val and re.fullmatch(r"[A-Za-z0-9]+", val):
            return val

    # Inline scripts mentioning dl_bfile & security
    try:
        scripts = frame.query_selector_all("script")
    except PWError:
        scripts = []

    for s in scripts:
        try:
            txt = s.text_content() or ""
        except PWError:
            continue

        m = re.search(
            r"dl_bfile[^A-Za-z0-9]+security[^A-Za-z0-9]+([A-Za-z0-9]{6,})",
            txt,
            flags=re.I | re.S,
        )
        if m:
            return m.group(1)

    return None


def _collect_candidates_from_frame(
    frame: Page,
    fallback_nonce: Optional[str],
) -> List[Tuple[str, str, str]]:
    """
    Collect (fid, fname, security) triples from a single frame.

    Strategy:
      - Look for any element with data-fid or data-fname.
      - Gather attributes from the element and its ancestors.
      - Require:
          * fid: numeric (Box / internal id used by dl_bfile)
          * fname: non-empty
          * security: from data-s or fallback_nonce
    """
    candidates: List[Tuple[str, str, str]] = []
    seen_keys = set()

    try:
        elements = frame.query_selector_all("[data-fid], [data-fname]")
    except PWError:
        elements = []

    if not elements:
        return candidates

    nonce = fallback_nonce or _discover_nonce(frame)

    for el in elements:
        try:
            fid = (el.get_attribute("data-fid") or "").strip()
            fname = (el.get_attribute("data-fname") or "").strip()
            sec = (el.get_attribute("data-s") or "").strip()
        except PWError:
            continue

        # If missing security on this element, use frame/global nonce
        if not sec and nonce:
            sec = nonce

        # Some implementations put attributes on ancestors; climb a little
        if not (fid and fname and sec):
            parent = el
            for _ in range(4):
                try:
                    parent = parent.evaluate_handle("node => node.parentElement")
                except PWError:
                    parent = None
                if not parent:
                    break

                try:
                    pfid = (parent.get_attribute("data-fid") or "").strip()
                    pfname = (parent.get_attribute("data-fname") or "").strip()
                    psec = (parent.get_attribute("data-s") or "").strip()
                except PWError:
                    continue

                if not fid and pfid:
                    fid = pfid
                if not fname and pfname:
                    fname = pfname
                if not sec and psec:
                    sec = psec
                if fid and fname and sec:
                    break

        if not fid or not fname or not sec:
            continue

        # Only keep numeric fid; this matches live dl_bfile usage
        if not re.fullmatch(r"\d{5,}", fid):
            continue

        key = f"{fid}|{fname}"
        if key in seen_keys:
            continue
        seen_keys.add(key)

        candidates.append((fid, fname, sec))

    return candidates


def _collect_buttons(page: Page) -> List[Tuple[str, str, str]]:
    """
    Collect all (fid, fname, security) candidates from the page and same-origin frames.
    """
    all_candidates: List[Tuple[str, str, str]] = []

    for frame in _same_origin_frames(page):
        try:
            furl = frame.url
        except PWError:
            furl = ""

        frame_nonce = _discover_nonce(frame)
        frame_candidates = _collect_candidates_from_frame(frame, frame_nonce)

        # FIXED: don't call frame.name as a function
        try:
            frame_name = getattr(frame, "name", "") or ""
        except PWError:
            frame_name = ""

        log_line(
            f"Frame name={frame_name!r} url={furl} -> {len(frame_candidates)} candidates"
        )

        all_candidates.extend(frame_candidates)

    # Deduplicate overall
    deduped: List[Tuple[str, str, str]] = []
    seen = set()
    for fid, fname, sec in all_candidates:
        key = f"{fid}|{fname}|{sec}"
        if key not in seen:
            seen.add(key)
            deduped.append((fid, fname, sec))

    log_line(f"Found {len(deduped)} download candidates on page (all frames).")
    return deduped


# ---------------------------------------------------------------------------
# AJAX + download helpers
# ---------------------------------------------------------------------------


def _fetch_box_url(
    api,
    fid: str,
    fname: str,
    security: str,
    referer: str,
) -> str:
    """Call the dl_bfile AJAX endpoint to obtain the Box download URL."""
    res: Response = api.post(
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

    # -1 is the plugin's "invalid nonce" / failure signal
    if payload in (-1, "-1"):
        raise RuntimeError("AJAX returned -1 (invalid security nonce)")

    if (
        not isinstance(payload, dict)
        or not payload.get("success")
        or "data" not in payload
    ):
        raise RuntimeError(f"AJAX malformed payload: {payload}")

    data = payload["data"]

    box_url = str(data.get("fid")).replace("\\/", "/").strip()
    if not box_url.startswith("http"):
        raise RuntimeError(f"AJAX did not return a valid download URL: {box_url}")

    return box_url


def _stream_pdf(api, url: str, out_path: Path) -> None:
    """Download the PDF from Box (or direct URL) and save to out_path."""
    res: Response = api.get(url, timeout=120_000)

    if res.status not in (200, 206):
        raise RuntimeError(f"Download HTTP {res.status}")

    body = res.body()
    if not body.startswith(b"%PDF"):
        raise RuntimeError("Response is not a PDF")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(body)

    if out_path.stat().st_size == 0:
        out_path.unlink(missing_ok=True)
        raise RuntimeError("Downloaded PDF is empty")


# ---------------------------------------------------------------------------
# Page load helpers
# ---------------------------------------------------------------------------


def _load_all_results(page: Page, max_scrolls: int = 40) -> None:
    """
    Best-effort to let the Unreported Judgments widget fully initialise.

    - Waits for initial network idle.
    - Scrolls to bottom repeatedly to trigger any lazy load.
    """
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
    Execute a scraping run using Playwright + dl_bfile AJAX.

    This is called by the /scrape route. All parameters are optional; sensible
    defaults are pulled from config.
    """
    ensure_dirs()

    base_url = (base_url or config.DEFAULT_BASE_URL).strip()
    entry_cap = entry_cap or config.ENTRY_CAP
    page_wait = page_wait or config.PAGE_WAIT_SECONDS
    per_delay = per_delay if per_delay is not None else config.PER_DOWNLOAD_DELAY

    log_line("=== Starting scraping run (Playwright) ===")
    log_line(f"Target base URL: {base_url}")
    log_line(
        f"Params: entry_cap={entry_cap}, page_wait={page_wait}, "
        f"per_download_delay={per_delay}"
    )

    cases = load_cases_from_csv(config.CSV_URL)
    meta = load_metadata()

    cases_by_fname = {str(c.get("fname")).strip(): c for c in cases if c.get("fname")}
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

    with sync_playwright() as pw:
        browser: Browser = pw.chromium.launch(headless=True)
        context: BrowserContext = browser.new_context(
            user_agent=UA,
            locale="en-US",
        )
        page: Page = context.new_page()

        log_line("Opening judgments page in Playwright...")
        page.goto(base_url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=20_000)
        except PWTimeout:
            log_line("Timed out waiting for full load; continuing.")

        if page_wait:
            page.wait_for_timeout(page_wait * 1000)

        _load_all_results(page)

        candidates = _collect_buttons(page)

        if not candidates:
            log_line("No valid download buttons discovered on page.")
            # Extra diagnostics: list frames & a snippet of HTML
            try:
                for f in page.frames:
                    try:
                        log_line(f"Frame debug: name={f.name} url={f.url}")
                    except PWError:
                        continue
            except PWError:
                pass

            try:
                html = page.content()
                snippet = html[:2000].replace("\n", " ")
                log_line(f"HTML snippet: {snippet}")
            except PWError:
                log_line("Unable to capture HTML snippet for diagnostics.")

            browser.close()
            return summary

        api = context.request

        for fid, raw_fname, sec in candidates:
            if summary["processed"] >= entry_cap:
                break

            safe_root = sanitize_filename(raw_fname)
            safe_name = (
                f"{safe_root}.pdf"
                if not safe_root.lower().endswith(".pdf")
                else safe_root
            )
            out_path = config.PDF_DIR / safe_name

            case = cases_by_fname.get(raw_fname) or cases_by_fname.get(safe_root)
            if not case:
                log_line(
                    f"Skipping fid={fid} fname={raw_fname}: "
                    "no matching non-criminal case in CSV."
                )
                summary["skipped"] += 1
                summary["processed"] += 1
                continue

            if out_path.exists() or is_duplicate(fid, safe_name, meta):
                log_line(
                    f"Skipping fid={fid} ({safe_name}): already downloaded/recorded."
                )
                summary["skipped"] += 1
                summary["processed"] += 1
                continue

            try:
                log_line(
                    f"Requesting Box URL via dl_bfile for fid={fid} fname={raw_fname}"
                )
                box_url = _fetch_box_url(
                    api,
                    fid=fid,
                    fname=raw_fname,
                    security=sec,
                    referer=base_url,
                )

                log_line(f"Streaming PDF from {box_url}")
                _stream_pdf(api, box_url, out_path)

                size_bytes = out_path.stat().st_size

                record_result(
                    meta,
                    fid=fid,
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
                summary["downloaded"] += 1

            except Exception as exc:  # noqa: BLE001
                log_line(f"Failed download for fid={fid} fname={raw_fname}: {exc}")
                if out_path.exists():
                    out_path.unlink(missing_ok=True)
                summary["failed"] += 1

            summary["processed"] += 1
            time.sleep(per_delay)

        browser.close()

    log_line(
        "Completed run: "
        "processed={processed} downloaded={downloaded} "
        "skipped={skipped} failed={failed}".format(**summary)
    )

    return summary


__all__ = ["run_scrape"]
