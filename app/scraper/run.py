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

import re
import time
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import requests

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
from .cases_index import (
    CASES_BY_ACTION,
    find_case_by_fname,
    load_cases_from_csv as load_cases_index,
)
from .utils import ensure_dirs, find_metadata_entry, load_metadata, log_line, record_result

ADMIN_AJAX = "https://judicial.ky/wp-admin/admin-ajax.php"

_ONCLICK_FNAME_RE = re.compile(r"dl_bfile[^'\"]*['\"]([A-Za-z0-9]+)['\"]", re.IGNORECASE)
_FORM_FIELD_SPLIT_RE = re.compile(r"&(?=[A-Za-z0-9_]+=)")

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def wait_seconds(page: Optional[Page], seconds: float) -> None:
    """Wait safely for ``seconds`` only if *page* remains open."""

    if page is None:
        return

    if seconds is None or seconds <= 0:
        return

    if not page.is_closed():
        page.wait_for_timeout(int(seconds * 1000))


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
                wait_seconds(page, 0.4)
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
            wait_seconds(page, 0.5)
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


def _extract_form_value(body: str, field: str) -> str:
    """Extract *field* from an ``application/x-www-form-urlencoded`` body."""

    if not body or not field:
        return ""

    marker = f"{field}="
    if marker not in body:
        return ""

    tail = body.split(marker, 1)[1]
    parts = _FORM_FIELD_SPLIT_RE.split(tail, 1)
    value = parts[0]
    return value


# ---------------------------------------------------------------------------
# Download + AJAX helpers
# ---------------------------------------------------------------------------

def queue_or_download_file(
    url: str,
    dest_path: Path,
    *,
    http_client: Optional[Any] = None,
    max_retries: int = 3,
    timeout: int = 120,
) -> tuple[bool, Optional[str]]:
    """Download ``url`` to ``dest_path`` with retries and validation."""

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    last_error: Optional[str] = None

    for attempt in range(1, max_retries + 1):
        try:
            if http_client is not None:
                response = http_client(url, timeout=timeout)
                status = getattr(response, "status", None)
                if status is None:
                    status = getattr(response, "status_code", None)
                if status is not None and int(status) >= 400:
                    raise RuntimeError(f"HTTP {status}")

                body = response.body() if hasattr(response, "body") else response.content
                if isinstance(body, str):
                    body_bytes = body.encode("utf-8")
                elif isinstance(body, (bytes, bytearray)):
                    body_bytes = bytes(body)
                else:
                    body_bytes = bytes(body)

                if not body_bytes.startswith(b"%PDF"):
                    raise RuntimeError("Response is not a PDF")

                dest_path.write_bytes(body_bytes)
                return True, None

            with requests.get(url, stream=True, timeout=timeout) as resp:
                resp.raise_for_status()
                with dest_path.open("wb") as handle:
                    first_chunk = True
                    for chunk in resp.iter_content(chunk_size=8192):
                        if not chunk:
                            continue
                        if first_chunk:
                            if not chunk.startswith(b"%PDF"):
                                raise RuntimeError("Response is not a PDF")
                            first_chunk = False
                        handle.write(chunk)

            if dest_path.stat().st_size <= 0:
                raise RuntimeError("Empty download")

            return True, None

        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            log_line(
                f"[AJAX] Download attempt {attempt} for {url} failed: {exc}"
            )
            dest_path.unlink(missing_ok=True)
            time.sleep(min(2 ** attempt, 5))

    return False, last_error


def _sanitize_title_for_filename(title: str) -> str:
    """Return a stable, filesystem-safe representation of *title*."""

    cleaned = re.sub(r"[\r\n]+", " ", (title or "").strip())
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"[\\/:*?\"<>|]+", "_", cleaned)
    cleaned = cleaned.strip(" _")
    return cleaned or "Document"


def _compute_output_filename(fname: str, case: Optional["CaseRow"]) -> str:
    """Return a deterministic PDF filename for the provided case/fname."""

    token = (fname or "").strip()
    title = case.title if case and case.title else token or "Document"
    safe_title = _sanitize_title_for_filename(title)

    if not token:
        token = safe_title

    filename = f"{token} - {safe_title}"
    filename = re.sub(r"\s+", " ", filename).strip()
    filename = re.sub(r"[\\/:*?\"<>|]+", "_", filename)

    if not filename.lower().endswith(".pdf"):
        filename = f"{filename}.pdf"

    return filename


def _label_for_entry(
    fname: str,
    case: Optional["CaseRow"],
    entry: Optional[Dict[str, Any]],
) -> str:
    """Build a human-readable label for logging."""

    if entry:
        title = entry.get("title") or entry.get("slug")
        if isinstance(title, str) and title.strip():
            return title.strip()

    if case and case.title:
        return case.title

    return fname


def is_already_downloaded(dest_path: Path, case_meta: Optional[Dict[str, Any]]) -> bool:
    """Return True when the file for this case already exists locally."""

    try:
        if dest_path is None:
            return False
        if not dest_path.is_file():
            return False
        if dest_path.stat().st_size < 1024:
            return False
    except OSError:
        return False

    if case_meta is not None:
        downloaded_flag = case_meta.get("downloaded") or case_meta.get("downloaded_pdf")
        if bool(downloaded_flag):
            return True

    return True


def handle_dl_bfile_from_ajax(
    post_body: str,
    payload: Any,
    downloads_dir: Path,
    *,
    meta: Optional[Dict[str, Any]] = None,
    seen: Optional[Set[str]] = None,
    http_client: Optional[Any] = None,
    pending: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """Process a dl_bfile AJAX response and download the corresponding PDF."""

    params = {
        k: v for k, v in urllib.parse.parse_qsl(post_body or "", keep_blank_values=True)
    }
    action_param = (params.get("action") or "").strip()
    fname_raw = (params.get("fname") or "").strip()
    if not fname_raw:
        fname_raw = _extract_form_value(post_body or "", "fname").strip()
    fid_param = (params.get("fid") or "").strip()

    if action_param and action_param != "dl_bfile":
        return "ignored"

    if not fname_raw:
        log_line(f"[AJAX] dl_bfile without fname; params={params!r}")
        return "ignored"

    fname_value = urllib.parse.unquote_plus(fname_raw).strip()
    fname_key = fname_value.upper() or fname_raw.upper()
    fname_display = fname_value or fname_key

    if seen is not None and fname_key in seen:
        log_line(f"[AJAX] fname {fname_display} already processed this run.")
        if pending is not None:
            pending.pop(fname_key, None)
        return "duplicate"

    if not isinstance(payload, dict):
        log_line(
            f"[AJAX][WARN] dl_bfile payload is not a dict for fname={fname_key}: {payload!r}"
        )
        if seen is not None:
            seen.add(fname_key)
        if pending is not None:
            pending.pop(fname_key, None)
        return "failed"

    if payload in (-1, "-1") or not payload.get("success"):
        log_line(f"[AJAX][WARN] dl_bfile failure for fname={fname_key}: {payload!r}")
        if seen is not None:
            seen.add(fname_key)
        if pending is not None:
            pending.pop(fname_key, None)
        return "failed"

    data = payload.get("data") or {}
    box_url = _extract_box_url(data)
    if not box_url:
        log_line(
            f"[AJAX] Could not find Box URL in payload for fname={fname_key}: {data!r}"
        )
        if seen is not None:
            seen.add(fname_key)
        if pending is not None:
            pending.pop(fname_key, None)
        return "failed"

    case_context: Optional[Dict[str, Any]] = None
    if pending is not None:
        case_context = pending.get(fname_key)

    case = case_context.get("case") if case_context else None
    slug: str
    if case_context and case_context.get("slug"):
        slug = case_context["slug"]
    else:
        slug = fname_key

    entry: Optional[Dict[str, Any]] = None
    if case_context:
        entry = case_context.get("metadata_entry")

    if case is None:
        case = find_case_by_fname(fname_key, strict=True)
        if case is not None:
            slug = case.action

    if case is None:
        log_line(
            f"[AJAX][WARN] No case mapping found for fname={fname_display}; "
            "not skipping based on other cases."
        )
    else:
        log_line(
            f'[AJAX] dl_bfile request fname={fname_display} → matched case="{case.title}" '
            f'(action={case.action})'
        )
        slug = case.action

    filename = _compute_output_filename(fname_key, case)
    dest_path = Path(downloads_dir) / filename

    raw_fid_value = data.get("fid") or box_url
    if isinstance(raw_fid_value, str):
        fid_for_log = raw_fid_value.strip()
    else:
        fid_for_log = str(raw_fid_value)
    if fid_for_log:
        short_fid = fid_for_log
        if len(short_fid) > 96:
            short_fid = f"{short_fid[:93]}..."
        log_line(
            f"[AJAX] dl_bfile fname={fname_display} using download fid={short_fid}"
        )

    entry_candidate = entry
    if meta is not None:
        meta_entry, _ = find_metadata_entry(meta, slug=slug, filename=dest_path.name)
        if meta_entry is not None:
            entry_candidate = meta_entry

    candidates: List[Dict[str, Any]] = []
    if entry_candidate:
        candidates.append(entry_candidate)
    context_entry = case_context.get("metadata_entry") if case_context else None
    if context_entry and context_entry not in candidates:
        candidates.append(context_entry)

    entry = None
    for candidate in candidates:
        if not candidate:
            continue
        stored_name = (candidate.get("local_filename") or candidate.get("filename") or "").strip()
        if stored_name and stored_name != dest_path.name:
            log_line(
                f"[AJAX][WARN] Metadata filename mismatch for fname={fname_display}: "
                f"stored={stored_name!r} expected={dest_path.name!r}; ignoring stored path."
            )
            continue
        entry = candidate
        break

    log_line(
        f"[AJAX] dl_bfile fname={fname_display} expected_path='{dest_path}'"
    )

    label = _label_for_entry(fname_key, case, entry)

    if is_already_downloaded(dest_path, entry):
        log_line(
            f"[AJAX] Local file exists for fname={fname_display}, case=\"{label}\"; skipping download."
        )
        if seen is not None:
            seen.add(fname_key)
        if pending is not None:
            pending.pop(fname_key, None)
        return "duplicate"

    success, error = queue_or_download_file(
        box_url,
        dest_path,
        http_client=http_client,
    )
    if not success:
        log_line(f"[AJAX] Failed to save {fname_key} to {dest_path}: {error}")
        if seen is not None:
            seen.add(fname_key)
        if pending is not None:
            pending.pop(fname_key, None)
        dest_path.unlink(missing_ok=True)
        return "failed"

    size_bytes = dest_path.stat().st_size
    log_line(
        f"[AJAX] Saved fname={fname_display} -> {dest_path} ({size_bytes/1024:.1f} KiB)"
    )

    if meta is not None:
        record_result(
            meta,
            slug=slug,
            fid=fid_param or fname_key,
            title=label,
            local_filename=dest_path.name,
            source_url=box_url,
            size_bytes=size_bytes,
            category=(case.extra.get("Category") if case else None),
            judgment_date=(
                case.extra.get("Judgment Date") if case else None
            )
            or (case.extra.get("Date") if case else None),
            local_path=str(dest_path.resolve()),
        )

    if seen is not None:
        seen.add(fname_key)
    if pending is not None:
        pending.pop(fname_key, None)

    return "downloaded"


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

    # Load case index from CSV once per run
    load_cases_index("data/judgments.csv")
    meta = load_metadata()

    summary: Dict[str, Any] = {
        "base_url": base_url,
        "processed": 0,
        "downloaded": 0,
        "failed": 0,
        "skipped": 0,
        "total_cases": len(CASES_BY_ACTION),
    }

    if not CASES_BY_ACTION:
        log_line("No cases loaded from judgments CSV; aborting.")
        return summary

    seen_fnames_in_run: Set[str] = set()
    pending_by_fname: Dict[str, Dict[str, Any]] = {}

    active = True
    browser: Optional[Browser] = None
    context: Optional[BrowserContext] = None
    page: Optional[Page] = None

    with sync_playwright() as pw:
        try:
            browser = pw.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            context = browser.new_context(
                user_agent=UA,
                locale="en-US",
                viewport={"width": 1368, "height": 900},
            )

            # Mild stealth
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            )

            page = context.new_page()
            if page is None:
                raise RuntimeError("Failed to create Playwright page")

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

                if not active:
                    return

                page_ref = page
                if page_ref is not None and page_ref.is_closed():
                    return

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

                    payload: Any
                    try:
                        payload = resp.json()
                    except Exception as exc:
                        log_line(f"[AJAX][WARN] dl_bfile non-JSON response: {exc}")
                        payload = None

                    log_line(f"[AJAX] dl_bfile payload={payload!r}")

                    if not active:
                        return

                    http_fetcher = (
                        lambda download_url, timeout=120: context.request.get(
                            download_url,
                            timeout=timeout * 1000,
                        )
                    )

                    result = handle_dl_bfile_from_ajax(
                        post_body=body,
                        payload=payload,
                        downloads_dir=config.PDF_DIR,
                        meta=meta,
                        seen=seen_fnames_in_run,
                        http_client=http_fetcher,
                        pending=pending_by_fname,
                    )

                    if result == "downloaded":
                        summary["downloaded"] += 1
                    elif result == "failed":
                        summary["failed"] += 1
                    elif result in {"duplicate", "unknown", "ignored"}:
                        summary["skipped"] += 1

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
                wait_seconds(page, float(page_wait))

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

                        # Resolve fname so the AJAX handler can associate responses precisely.
                        fname_token: Optional[str] = None
                        for attr_name in ("data-dl", "data-fname", "data-filename", "data-target"):
                            try:
                                raw_value = el.get_attribute(attr_name)
                            except Exception:
                                raw_value = None
                            if raw_value and raw_value.strip():
                                fname_token = raw_value.strip()
                                break

                        if not fname_token:
                            try:
                                onclick_raw = el.get_attribute("onclick") or ""
                            except Exception:
                                onclick_raw = ""
                            if onclick_raw:
                                match = _ONCLICK_FNAME_RE.search(onclick_raw)
                                if match:
                                    fname_token = match.group(1).strip()

                        if fname_token:
                            fname_token_key = fname_token.strip().upper()
                            case_for_fname = find_case_by_fname(fname_token_key, strict=True)
                            if case_for_fname is not None:
                                slug = case_for_fname.action
                            else:
                                slug = fname_token_key
                            entry_for_fname: Optional[Dict[str, Any]] = None
                            if meta is not None:
                                entry_for_fname, _ = find_metadata_entry(
                                    meta,
                                    slug=slug,
                                    filename=None,
                                )
                            pending_by_fname[fname_token_key] = {
                                "case": case_for_fname,
                                "metadata_entry": entry_for_fname,
                                "slug": slug,
                                "raw": fname_token,
                            }

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
        finally:
            active = False
            for closable in (page, context, browser):
                try:
                    if closable is None:
                        continue
                    closable.close()
                except Exception as exc:
                    name = type(closable).__name__ if closable is not None else "Unknown"
                    log_line(f"Error closing Playwright object {name}: {exc}")

    log_line("Completed run (response-capture strategy).")
    return summary




if __name__ == "__main__":  # pragma: no cover - quick manual verification
    from tempfile import TemporaryDirectory

    from .cases_index import AJAX_FNAME_INDEX, CASES_ALL, CaseRow

    CASES_BY_ACTION.clear()
    AJAX_FNAME_INDEX.clear()
    CASES_ALL.clear()

    sample_case = CaseRow(
        action="FSD0151202511062025ATPLIFESCIENCE",
        code="FSD0151202511062025",
        suffix="ATPLIFESCIENCE",
        title="Re ATP Life Science Ventures LP - Judgment",
        extra={"Category": "Grand Court", "Judgment Date": "2025-Nov-06"},
    )
    CASES_BY_ACTION[sample_case.action] = sample_case
    AJAX_FNAME_INDEX[sample_case.action] = sample_case

    embedded_case = CaseRow(
        action="1J1CB5JDVWQJ1DE60AG13020A37E6E68EADE88BE7AE51E57A648",
        code="1J1CB5JDVWQJ1DE60AG13020",
        suffix="A37E6E68EADE88BE7AE51E57A648",
        title="Embedded Token Fixture",
        extra={"Category": "Example", "Judgment Date": "2024-Jun-01"},
    )
    CASES_BY_ACTION[embedded_case.action] = embedded_case
    AJAX_FNAME_INDEX[embedded_case.action] = embedded_case

    CASES_ALL.extend(CASES_BY_ACTION.values())

    print(
        "[Demo] Exact lookup:",
        find_case_by_fname("FSD0151202511062025ATPLIFESCIENCE", strict=True),
    )
    print("[Demo] Partial lookup:", find_case_by_fname("AG13020"))

    class _DummyResponse:
        status = 200

        def __init__(self, payload: bytes) -> None:
            self._payload = payload

        def body(self) -> bytes:
            return self._payload

    class _DummyClient:
        def __call__(self, url: str, timeout: int = 120) -> _DummyResponse:  # noqa: D401
            return _DummyResponse(b"%PDF-1.4\n%%EOF\n")

    sample_body = (
        "action=dl_bfile&fid=999&fname=FSD0151202511062025ATPLIFESCIENCE&security=dummy"
    )
    sample_payload = {
        "success": True,
        "data": {"fid": "https://example.org/dummy.pdf"},
    }

    with TemporaryDirectory() as tmpdir:
        result = handle_dl_bfile_from_ajax(
            sample_body,
            sample_payload,
            Path(tmpdir),
            meta={},
            seen=set(),
            http_client=_DummyClient(),
            pending={},
        )
        print("[Demo] Handler result:", result)

__all__ = ["run_scrape"]

