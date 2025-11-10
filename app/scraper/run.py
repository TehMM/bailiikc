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

import json
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
    normalize_action_token,
)
from .utils import (
    build_pdf_path,
    ensure_dirs,
    find_metadata_entry,
    hashed_fallback_path,
    has_local_pdf,
    load_metadata,
    log_line,
    record_result,
    setup_run_logger,
)

ADMIN_AJAX = "https://judicial.ky/wp-admin/admin-ajax.php"

_ONCLICK_FNAME_RE = re.compile(r"dl_bfile[^'\"]*['\"]([A-Za-z0-9]+)['\"]", re.IGNORECASE)
_FORM_FIELD_SPLIT_RE = re.compile(r"&(?=[A-Za-z0-9_]+=)")

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


MAX_RESTART_ATTEMPTS = 3
RESTART_BACKOFF_SECONDS = [5, 15, 45]


class Checkpoint:
    """Persist and restore scraper progress between browser restarts."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data: Dict[str, Any] = {
            "last_page_index": 0,
            "last_row_index": -1,
            "processed_tokens": [],
            "completed_downloads": [],
        }

        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    for key in self.data:
                        if key in loaded:
                            self.data[key] = loaded[key]
            except Exception as exc:  # noqa: BLE001
                log_line(f"[CHECKPOINT] Failed to read checkpoint: {exc}")

        tokens = self.data.get("processed_tokens") or []
        self._processed_tokens: Set[str] = {
            normalize_action_token(token)
            for token in tokens
            if isinstance(token, str) and normalize_action_token(token)
        }
        self.data["processed_tokens"] = sorted(self._processed_tokens)

        downloads = self.data.get("completed_downloads") or []
        self._completed_downloads: Set[str] = {
            str(name)
            for name in downloads
            if isinstance(name, str) and name.strip()
        }
        self.data["completed_downloads"] = sorted(self._completed_downloads)

    @property
    def page_index(self) -> int:
        try:
            return int(self.data.get("last_page_index", 0))
        except (TypeError, ValueError):
            return 0

    @property
    def row_index(self) -> int:
        try:
            return int(self.data.get("last_row_index", -1))
        except (TypeError, ValueError):
            return -1

    @property
    def processed_tokens(self) -> Set[str]:
        return set(self._processed_tokens)

    def save(self) -> None:
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(self.path)

    def mark_page(self, page_index: int, *, reset_row: bool = True) -> None:
        self.data["last_page_index"] = int(max(0, page_index))
        if reset_row:
            self.data["last_row_index"] = -1
        self.save()

    def mark_row(self, row_index: int) -> None:
        self.data["last_row_index"] = int(max(-1, row_index))
        self.save()

    def mark_position(self, page_index: int, row_index: int) -> None:
        self.data["last_page_index"] = int(max(0, page_index))
        self.data["last_row_index"] = int(max(-1, row_index))
        self.save()

    def record_download(self, token: str, filename: str) -> None:
        norm = normalize_action_token(token)
        if norm:
            if norm not in self._processed_tokens:
                self._processed_tokens.add(norm)
                tokens = self.data.setdefault("processed_tokens", [])
                tokens.append(norm)
        if filename:
            if filename not in self._completed_downloads:
                self._completed_downloads.add(filename)
                downloads = self.data.setdefault("completed_downloads", [])
                downloads.append(filename)
        self.save()


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


def _set_datatable_page(page: Page, page_index: int) -> bool:
    """Attempt to switch the DataTable to *page_index* (0-based)."""

    if page_index <= 0:
        return True

    try:
        switched = page.evaluate(
            """
            (idx) => {
                const $ = window.jQuery;
                if (!$ || !$.fn || !$.fn.dataTable) {
                    return false;
                }
                const table = $('#judgment-registers').DataTable();
                if (!table) {
                    return false;
                }
                const info = table.page.info();
                if (!info || idx < 0 || idx >= info.pages) {
                    return false;
                }
                table.page(idx).draw('page');
                return true;
            }
            """,
            page_index,
        )
        return bool(switched)
    except Exception as exc:  # noqa: BLE001
        log_line(f"[RUN] Failed to set DataTable page via JS: {exc}")
        return False


def _refresh_datatable_page(page: Page, page_index: int) -> None:
    """Force the current DataTable page to redraw."""

    try:
        page.evaluate(
            """
            (idx) => {
                const $ = window.jQuery;
                if (!$ || !$.fn || !$.fn.dataTable) {
                    return false;
                }
                const table = $('#judgment-registers').DataTable();
                if (!table) {
                    return false;
                }
                table.page(idx).draw(false);
                return true;
            }
            """,
            page_index,
        )
    except Exception:
        # Silently ignore; best effort.
        pass


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


def _is_target_closed_error(exc: Exception) -> bool:
    """Return ``True`` if *exc* indicates the Playwright target is gone."""

    message = str(exc)
    return any(
        marker in message
        for marker in (
            "Target closed",
            "Target crashed",
            "has been closed",
            "Execution context was destroyed",
        )
    )


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


def handle_dl_bfile_from_ajax(
    post_body: str,
    payload: Any,
    downloads_dir: Path,
    *,
    meta: Optional[Dict[str, Any]] = None,
    seen: Optional[Set[str]] = None,
    http_client: Optional[Any] = None,
    pending: Optional[Dict[str, Dict[str, Any]]] = None,
    checkpoint: Optional[Checkpoint] = None,
    processed_tokens: Optional[Set[str]] = None,
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
    fname_key = normalize_action_token(fname_value or fname_raw)
    fname_display = fname_value or fname_raw or fname_key

    if not fname_key:
        log_line(
            f"[AJAX][WARN] Unable to normalise fname token '{fname_display}'; skipping."
        )
        return "failed"

    def _finalise(
        result: str,
        final_path: Optional[Path] = None,
        *,
        mark_token: bool = True,
        mark_seen: bool = True,
    ) -> str:
        context = pending.get(fname_key) if pending is not None else None
        if checkpoint is not None and context:
            page_idx = context.get("page_index")
            row_idx = context.get("row_index")
            if page_idx is not None and row_idx is not None:
                try:
                    checkpoint.mark_position(int(page_idx), int(row_idx))
                except Exception as exc:  # noqa: BLE001
                    log_line(f"[CHECKPOINT] Failed to record position: {exc}")
        if mark_token and result in {"downloaded", "duplicate"}:
            if processed_tokens is not None:
                processed_tokens.add(fname_key)
            if checkpoint is not None:
                checkpoint.record_download(
                    fname_key,
                    final_path.name if final_path is not None else "",
                )
        if mark_seen and seen is not None:
            seen.add(fname_key)
        if pending is not None:
            pending.pop(fname_key, None)
        return result

    if processed_tokens is not None and fname_key in processed_tokens:
        log_line(f"[AJAX] fname {fname_display} already persisted in checkpoint.")
        return _finalise("duplicate", mark_token=False)

    if seen is not None and fname_key in seen:
        log_line(f"[AJAX] fname {fname_display} already processed this run.")
        return _finalise("duplicate", mark_token=False)

    if not isinstance(payload, dict):
        log_line(
            f"[AJAX][WARN] dl_bfile payload is not a dict for fname={fname_key}: {payload!r}"
        )
        return _finalise("failed", mark_token=False)

    if payload in (-1, "-1") or not payload.get("success"):
        log_line(f"[AJAX][WARN] dl_bfile failure for fname={fname_key}: {payload!r}")
        return _finalise("failed", mark_token=False)

    data = payload.get("data") or {}
    box_url = _extract_box_url(data)
    if not box_url:
        log_line(
            f"[AJAX] Could not find Box URL in payload for fname={fname_key}: {data!r}"
        )
        return _finalise("failed", mark_token=False)

    case_context: Optional[Dict[str, Any]] = None
    if pending is not None:
        case_context = pending.get(fname_key)

    case = case_context.get("case") if case_context else None
    slug = case_context.get("slug") if case_context and case_context.get("slug") else fname_key
    meta_entry: Optional[Dict[str, Any]] = None
    if case_context:
        meta_entry = case_context.get("metadata_entry")

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
        case_subject = case.subject or case.title or fname_key
        log_line(
            f"[AJAX] dl_bfile fname={fname_display} resolved to case='{case_subject}'"
        )
        slug = case.action

    downloads_dir = Path(downloads_dir).resolve()
    title_source = (
        (case.title if case and case.title else None)
        or (case.subject if case and case.subject else None)
        or fname_display
    )
    cause_number: Optional[str] = None
    if case is not None:
        cause_number = case.cause_number or case.extra.get("Cause Number")
    dest_path = build_pdf_path(downloads_dir, title_source, cause_number=cause_number)

    raw_fid_value = data.get("fid") or box_url
    if isinstance(raw_fid_value, str):
        fid_for_log = raw_fid_value.strip()
    else:
        fid_for_log = str(raw_fid_value)
    if fid_for_log:
        short_fid = fid_for_log if len(fid_for_log) <= 96 else f"{fid_for_log[:93]}..."
        log_line(
            f"[AJAX] dl_bfile fname={fname_display} using download fid={short_fid}"
        )

    resolved_entry = meta_entry
    if meta is not None:
        lookup_entry, _ = find_metadata_entry(meta, slug=slug, filename=dest_path.name)
        if lookup_entry is None and slug != fname_key:
            lookup_entry, _ = find_metadata_entry(
                meta, slug=fname_key, filename=dest_path.name
            )
        if lookup_entry is not None:
            resolved_entry = lookup_entry

    log_line(
        f"[AJAX] dl_bfile fname={fname_display} expected_path='{dest_path}'"
    )

    if resolved_entry and has_local_pdf(resolved_entry):
        label = resolved_entry.get("title") or resolved_entry.get("slug") or fname_display
        log_line(
            f"[AJAX] Metadata and local file confirm {label}; skipping download."
        )
        return _finalise("duplicate", dest_path)

    if resolved_entry:
        stored_name = (resolved_entry.get("local_filename") or "").strip()
        if stored_name and stored_name != dest_path.name:
            log_line(
                f"[AJAX][WARN] Metadata filename mismatch for fname={fname_display}: "
                f"stored={stored_name!r} expected={dest_path.name!r}; proceeding to download."
            )

    if not resolved_entry:
        try:
            if dest_path.exists() and dest_path.is_file() and dest_path.stat().st_size > 0:
                log_line(
                    f"[AJAX] Existing file detected for fname={fname_display}; skipping download."
                )
                if meta is not None:
                    subject_value = (
                        (case.title if case and case.title else None)
                        or (case.subject if case and case.subject else None)
                        or fname_display
                    )
                    category_value = None
                    judgment_value = None
                    court_value = None
                    cause_value = None
                    if case is not None:
                        category_value = case.category or case.extra.get("Category")
                        judgment_value = (
                            case.judgment_date
                            or case.extra.get("Judgment Date")
                            or case.extra.get("Date")
                        )
                        court_value = (
                            case.court
                            or case.extra.get("Court")
                            or case.extra.get("Court file")
                        )
                        cause_value = case.cause_number or case.extra.get("Cause Number")
                    record_result(
                        meta,
                        slug=slug,
                        fid=fid_param or fname_key,
                        title=subject_value or fname_display,
                        local_filename=dest_path.name,
                        source_url=box_url,
                        size_bytes=dest_path.stat().st_size,
                        category=category_value,
                        judgment_date=judgment_value,
                        court=court_value,
                        cause_number=cause_value,
                        subject=subject_value,
                        local_path=str(dest_path.resolve()),
                    )
                return _finalise("duplicate", dest_path)
        except OSError:
            pass

    success = False
    error: Optional[str] = None
    final_path = dest_path

    try:
        success, error = queue_or_download_file(
            box_url,
            dest_path,
            http_client=http_client,
        )
    except OSError as exc:
        error = str(exc)
        success = False

    def _filename_error_matches(message: Optional[str]) -> bool:
        if not message:
            return False
        lowered = message.lower()
        return "file name too long" in lowered or "errno 36" in lowered or "errno 63" in lowered

    if not success and _filename_error_matches(error):
        fallback_path = hashed_fallback_path(downloads_dir, title_source)
        if fallback_path != dest_path:
            log_line(
                f"[AJAX] Retrying save for fname={fname_display} with fallback {fallback_path.name}"
            )
            final_path = fallback_path
            try:
                success, error = queue_or_download_file(
                    box_url,
                    fallback_path,
                    http_client=http_client,
                )
            except OSError as exc:
                error = str(exc)
                success = False

    if not success:
        log_line(f"[AJAX] Failed to save {fname_key} to {final_path}: {error}")
        final_path.unlink(missing_ok=True)
        return _finalise("failed", mark_token=False)

    size_bytes = final_path.stat().st_size
    log_line(
        f"[AJAX] Saved fname={fname_display} -> {final_path} ({size_bytes/1024:.1f} KiB)"
    )

    if meta is not None:
        subject_value = (
            (case.title if case and case.title else None)
            or (case.subject if case and case.subject else None)
            or fname_display
        )
        category_value = None
        judgment_value = None
        court_value = None
        cause_value = None
        if case is not None:
            category_value = case.category or case.extra.get("Category")
            judgment_value = (
                case.judgment_date
                or case.extra.get("Judgment Date")
                or case.extra.get("Date")
            )
            court_value = case.court or case.extra.get("Court") or case.extra.get("Court file")
            cause_value = case.cause_number or case.extra.get("Cause Number")
        elif resolved_entry:
            category_value = resolved_entry.get("category")
            judgment_value = resolved_entry.get("judgment_date")
            court_value = resolved_entry.get("court")
            cause_value = resolved_entry.get("cause_number")

        record_result(
            meta,
            slug=slug,
            fid=fid_param or fname_key,
            title=subject_value or fname_key,
            local_filename=final_path.name,
            source_url=box_url,
            size_bytes=size_bytes,
            category=category_value,
            judgment_date=judgment_value,
            court=court_value,
            cause_number=cause_value,
            subject=subject_value,
            local_path=str(final_path.resolve()),
        )

    return _finalise("downloaded", final_path)


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------



def run_scrape(
    base_url: Optional[str] = None,
    entry_cap: Optional[int] = None,
    page_wait: Optional[int] = None,
    per_delay: Optional[float] = None,
    start_message: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute a scraping run with automatic restart/resume support."""

    ensure_dirs()

    log_path = setup_run_logger()
    if start_message:
        log_line(start_message)

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

    load_cases_index("data/judgments.csv")
    meta = load_metadata()
    checkpoint = Checkpoint(config.DATA_DIR / "state.json")

    downloaded_index: Dict[str, Dict[str, Any]] = {}
    for entry in meta.get("downloads", []):
        if not isinstance(entry, dict):
            continue
        slug_value = normalize_action_token(entry.get("slug") or entry.get("fid") or "")
        if not slug_value:
            continue
        if entry.get("downloaded") and has_local_pdf(entry):
            downloaded_index[slug_value] = entry

    processed_tokens: Set[str] = set(checkpoint.processed_tokens)
    processed_tokens.update(downloaded_index.keys())

    summary: Dict[str, Any] = {
        "base_url": base_url,
        "processed": 0,
        "downloaded": 0,
        "failed": 0,
        "skipped": 0,
        "total_cases": len(CASES_BY_ACTION),
        "log_file": str(log_path),
    }

    if not CASES_BY_ACTION:
        log_line("No cases loaded from judgments CSV; aborting.")
        return summary

    attempt = 0
    while attempt <= MAX_RESTART_ATTEMPTS:
        seen_fnames_in_run: Set[str] = set(processed_tokens)
        pending_by_fname: Dict[str, Dict[str, Any]] = {}
        active = True
        browser: Optional[Browser] = None
        context: Optional[BrowserContext] = None
        page: Optional[Page] = None
        resume_page_index = max(0, checkpoint.page_index)
        resume_row_index = checkpoint.row_index
        crash_stop = False

        log_line(
            f"[RUN] Resume checkpoint -> page_index={resume_page_index}, row_index={resume_row_index}"
        )

        try:
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

                    context.add_init_script(
                        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                    )

                    page = context.new_page()
                    if page is None:
                        raise RuntimeError("Failed to create Playwright page")

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
                                checkpoint=checkpoint,
                                processed_tokens=processed_tokens,
                            )

                            if result == "downloaded":
                                summary["downloaded"] += 1
                            elif result == "failed":
                                summary["failed"] += 1
                            elif result in {"duplicate", "unknown", "ignored"}:
                                summary["skipped"] += 1

                        except Exception as exc:
                            log_line(f"[AJAX] handler error: {exc}")

                    context.on("response", on_response)

                    log_line("Opening judgments page in Playwright...")
                    page.goto(base_url, wait_until="domcontentloaded")
                    try:
                        page.wait_for_load_state("networkidle", timeout=20_000)
                    except PWTimeout:
                        log_line("Initial networkidle timeout; continuing.")

                    if page_wait:
                        wait_seconds(page, float(page_wait))

                    _accept_cookies(page)
                    _load_all_results(page)
                    _screenshot(page)

                    total_clicks = summary.get("processed", 0)
                    entry_cap_reached = False
                    resume_consumed = False
                    page_number = max(1, resume_page_index + 1)

                    try:
                        page.wait_for_selector("button:has(i.icon-dl)", timeout=25_000)
                    except PWTimeout:
                        log_line("No download buttons detected within timeout; aborting run.")
                    except PWError as exc:
                        if _is_target_closed_error(exc):
                            log_line(
                                "[RUN] Browser target crashed while waiting for download buttons; stopping scrape gracefully."
                            )
                            crash_stop = True
                            active = False
                        else:
                            log_line(f"Failed waiting for download buttons: {exc}")
                    else:
                        if resume_page_index > 0:
                            if _set_datatable_page(page, resume_page_index):
                                log_line(
                                    f"[RUN] Jumped to DataTable page index {resume_page_index}"
                                )
                                page_number = resume_page_index + 1
                            else:
                                log_line(
                                    "[RUN] Unable to set DataTable page via JS; advancing manually."
                                )
                                for step in range(resume_page_index):
                                    try:
                                        next_button = page.locator(
                                            "ul.pagination li.dt-paging-button button.page-link.next"
                                        )
                                        if next_button.count() == 0:
                                            break
                                        next_button.first.click(timeout=2000)
                                        wait_seconds(page, 0.4)
                                        page.wait_for_load_state("networkidle", timeout=10_000)
                                    except Exception as exc:
                                        log_line(
                                            f"[RUN][WARN] Manual pagination advance failed on step {step+1}: {exc}"
                                        )
                                        break
                                page_number = resume_page_index + 1

                        while not crash_stop:
                            page_index_zero = max(0, page_number - 1)
                            checkpoint.mark_page(page_index_zero, reset_row=False)
                            try:
                                buttons = page.locator("button:has(i.icon-dl)")
                                count = buttons.count()
                            except PWError as exc:
                                if _is_target_closed_error(exc):
                                    log_line(
                                        "[RUN] Browser target crashed while enumerating download buttons; stopping scrape gracefully."
                                    )
                                    crash_stop = True
                                    active = False
                                    break
                                log_line(f"Failed to enumerate download buttons: {exc}")
                                break
                            except Exception as exc:
                                log_line(f"Failed to enumerate download buttons: {exc}")
                                break

                            if crash_stop:
                                break

                            if count == 0:
                                log_line(
                                    f"No download buttons found on page {page_number}; stopping."
                                )
                                break

                            log_line(
                                f"Processing pagination page {page_number}: {count} download button(s)"
                            )

                            start_row = 0
                            if (
                                not resume_consumed
                                and page_index_zero == resume_page_index
                                and resume_row_index >= -1
                            ):
                                start_row = max(0, resume_row_index + 1)
                                if start_row > 0:
                                    log_line(
                                        f"[RUN] Resuming from row index {start_row} on page {page_number}"
                                    )
                            resume_consumed = True

                            for i in range(start_row, count):
                                if crash_stop:
                                    break

                                if entry_cap is not None and total_clicks >= entry_cap:
                                    log_line("Entry cap reached; stopping pagination loop.")
                                    entry_cap_reached = True
                                    break

                                try:
                                    el = page.locator("button:has(i.icon-dl)").nth(i)
                                except PWError as exc:
                                    if _is_target_closed_error(exc):
                                        log_line(
                                            "[RUN] Browser target crashed while accessing a download button; stopping scrape gracefully."
                                        )
                                        crash_stop = True
                                        active = False
                                        break
                                    log_line(f"Failed to access button index {i}: {exc}")
                                    continue
                                except Exception as exc:
                                    log_line(f"Failed to access button index {i}: {exc}")
                                    continue

                                fname_token: Optional[str] = None
                                for attr_name in (
                                    "data-dl",
                                    "data-fname",
                                    "data-filename",
                                    "data-target",
                                ):
                                    try:
                                        raw_value = el.get_attribute(attr_name)
                                    except PWError as exc_attr:
                                        if _is_target_closed_error(exc_attr):
                                            log_line(
                                                "[RUN] Browser target crashed while reading button attributes; stopping scrape gracefully."
                                            )
                                            crash_stop = True
                                            active = False
                                            break
                                        raw_value = None
                                    except Exception:
                                        raw_value = None
                                    if raw_value and raw_value.strip():
                                        fname_token = raw_value.strip()
                                        break
                                if crash_stop:
                                    break

                                if not fname_token:
                                    try:
                                        onclick_raw = el.get_attribute("onclick") or ""
                                    except PWError as exc_attr:
                                        if _is_target_closed_error(exc_attr):
                                            log_line(
                                                "[RUN] Browser target crashed while reading onclick attribute; stopping scrape gracefully."
                                            )
                                            crash_stop = True
                                            active = False
                                            onclick_raw = ""
                                        else:
                                            onclick_raw = ""
                                    except Exception:
                                        onclick_raw = ""
                                    if crash_stop:
                                        break
                                    if onclick_raw:
                                        match = _ONCLICK_FNAME_RE.search(onclick_raw)
                                        if match:
                                            fname_token = match.group(1).strip()

                                if crash_stop:
                                    break

                                fname_key = normalize_action_token(fname_token or "")
                                if not fname_key:
                                    log_line(
                                        f"[AJAX][WARN] Skipping button index {i} on page {page_number}: unable to normalise fname token."
                                    )
                                    checkpoint.mark_position(page_index_zero, i)
                                    continue

                                if fname_key in processed_tokens:
                                    log_line(
                                        f"[SKIP] fname={fname_token} already processed according to checkpoint; skipping click."
                                    )
                                    checkpoint.mark_position(page_index_zero, i)
                                    summary["skipped"] += 1
                                    seen_fnames_in_run.add(fname_key)
                                    continue

                                metadata_entry = downloaded_index.get(fname_key)
                                if metadata_entry and has_local_pdf(metadata_entry):
                                    label = metadata_entry.get("title") or fname_key
                                    log_line(
                                        f"[SKIP] fname={fname_token} already downloaded as {label}; skipping click."
                                    )
                                    processed_tokens.add(fname_key)
                                    checkpoint.record_download(
                                        fname_key,
                                        metadata_entry.get("local_filename")
                                        or metadata_entry.get("filename")
                                        or "",
                                    )
                                    checkpoint.mark_position(page_index_zero, i)
                                    summary["skipped"] += 1
                                    seen_fnames_in_run.add(fname_key)
                                    continue

                                case_for_fname = CASES_BY_ACTION.get(fname_key)
                                slug_value = (
                                    case_for_fname.action if case_for_fname else fname_key
                                )

                                pending_by_fname[fname_key] = {
                                    "case": case_for_fname,
                                    "metadata_entry": metadata_entry,
                                    "slug": slug_value,
                                    "raw": fname_token,
                                    "page_index": page_index_zero,
                                    "row_index": i,
                                }

                                click_success = False
                                for attempt_idx in range(3):
                                    try:
                                        el.click(timeout=2000)
                                        click_success = True
                                        break
                                    except PWError as exc_click:
                                        if _is_target_closed_error(exc_click):
                                            log_line(
                                                "[RUN] Target crashed during click; stopping scrape gracefully."
                                            )
                                            crash_stop = True
                                            active = False
                                            break
                                        log_line(
                                            f"Playwright click failed for button index {i} on page {page_number}: {exc_click}; trying JS click."
                                        )
                                        try:
                                            el.evaluate("el => el.click()")
                                            click_success = True
                                            break
                                        except PWError as exc_js:
                                            if _is_target_closed_error(exc_js):
                                                log_line(
                                                    "[RUN] Target crashed during JS click; stopping scrape gracefully."
                                                )
                                                crash_stop = True
                                                active = False
                                                break
                                            log_line(
                                                f"JS click also failed for button index {i} on page {page_number}: {exc_js}"
                                            )
                                        except Exception as exc_js:
                                            log_line(
                                                f"JS click also failed for button index {i} on page {page_number}: {exc_js}"
                                            )
                                    except Exception as exc_click:
                                        log_line(
                                            f"Playwright click failed for button index {i} on page {page_number}: {exc_click}; trying JS click."
                                        )
                                        try:
                                            el.evaluate("el => el.click()")
                                            click_success = True
                                            break
                                        except Exception as exc_js:
                                            log_line(
                                                f"JS click also failed for button index {i} on page {page_number}: {exc_js}"
                                            )
                                    if crash_stop:
                                        break
                                    if not click_success and attempt_idx < 2:
                                        _refresh_datatable_page(page, page_index_zero)
                                        wait_seconds(page, 0.3)

                                if crash_stop:
                                    break

                                if not click_success:
                                    log_line(
                                        f"[RUN][WARN] Unable to click download button index {i} on page {page_number} after retries; marking as failed."
                                    )
                                    pending_by_fname.pop(fname_key, None)
                                    summary["failed"] += 1
                                    checkpoint.mark_position(page_index_zero, i)
                                    continue

                                seen_fnames_in_run.add(fname_key)
                                total_clicks += 1
                                summary["processed"] = total_clicks
                                log_line(
                                    f"Clicked download button index {i} on page {page_number} (fname={fname_token})."
                                )

                                time.sleep((per_delay or 0) + 0.4)

                            if crash_stop or entry_cap_reached:
                                break

                            try:
                                next_button = page.locator(
                                    "ul.pagination li.dt-paging-button button.page-link.next"
                                )
                                next_count = next_button.count()
                            except PWError as exc:
                                if _is_target_closed_error(exc):
                                    log_line(
                                        "[RUN] Browser target crashed while locating pagination controls; stopping scrape gracefully."
                                    )
                                    crash_stop = True
                                    active = False
                                    break
                                log_line(f"Failed to locate pagination controls: {exc}")
                                break
                            except Exception as exc:
                                log_line(f"Failed to locate pagination controls: {exc}")
                                break

                            if crash_stop or entry_cap_reached:
                                break

                            if next_count == 0:
                                log_line("No pagination 'next' button found; ending traversal.")
                                break

                            try:
                                next_button.first.click(timeout=2000)
                            except PWError as exc:
                                if _is_target_closed_error(exc):
                                    log_line(
                                        "[RUN] Target crashed during pagination; stopping scrape gracefully."
                                    )
                                    crash_stop = True
                                    active = False
                                    break
                                log_line(f"Pagination click failed: {exc}")
                                break
                            except Exception as exc:
                                log_line(f"Pagination click failed: {exc}")
                                break

                            try:
                                page.wait_for_load_state("networkidle", timeout=20_000)
                            except PWError as exc:
                                if _is_target_closed_error(exc):
                                    log_line(
                                        "[RUN] Target crashed while waiting after pagination; stopping scrape gracefully."
                                    )
                                    crash_stop = True
                                    active = False
                                    break
                                log_line(f"Pagination load state wait failed: {exc}")
                                break

                            checkpoint.mark_page(page_index_zero + 1)
                            page_number += 1
                            wait_seconds(page, 0.5)

                        if crash_stop:
                            log_line(
                                "[RUN] Pagination loop terminated early due to browser crash; collected downloads will be preserved."
                            )

                    time.sleep(2.5)

                    summary["processed"] = total_clicks
                    log_line(
                        f"Clicks attempted: {total_clicks}. "
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
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            attempt += 1
            log_line(f"[RUN] Top-level scrape error: {exc}")
            if attempt > MAX_RESTART_ATTEMPTS:
                log_line("[RUN] Exhausted restart attempts; aborting scrape.")
                raise
            delay = RESTART_BACKOFF_SECONDS[min(
                attempt - 1,
                len(RESTART_BACKOFF_SECONDS) - 1,
            )]
            log_line(
                f"[RUN] Restarting browser in {delay}s (attempt {attempt}/{MAX_RESTART_ATTEMPTS})"
            )
            time.sleep(delay)
            continue
        else:
            break

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
        subject="Re ATP Life Science Ventures LP - Judgment",
        court="Grand Court",
        category="Financial Services",
        judgment_date="2025-Nov-06",
        extra={"Category": "Financial Services", "Judgment Date": "2025-Nov-06"},
    )
    CASES_BY_ACTION[sample_case.action] = sample_case
    AJAX_FNAME_INDEX[sample_case.action] = sample_case

    embedded_case = CaseRow(
        action="1J1CB5JDVWQJ1DE60AG13020A37E6E68EADE88BE7AE51E57A648",
        code="1J1CB5JDVWQJ1DE60AG13020",
        suffix="A37E6E68EADE88BE7AE51E57A648",
        title="Embedded Token Fixture",
        subject="Embedded Token Fixture",
        court="Example Court",
        category="Example",
        judgment_date="2024-Jun-01",
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

