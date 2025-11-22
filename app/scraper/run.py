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

import argparse
import json
import re
import time
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

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

from . import config, csv_sync, db
from .config import is_full_mode, is_new_mode
from .cases_index import (
    CASES_BY_ACTION,
    load_cases_from_csv as load_cases_index,
    normalize_action_token,
)
from .csv_sync import normalize_action_token as normalize_action_token_db
from .state import clear_checkpoint, derive_checkpoint_from_logs, load_checkpoint, save_checkpoint
from .telemetry import RunTelemetry
from .utils import (
    append_json_line,
    build_pdf_path,
    canon_fname,
    disk_has_room,
    ensure_dirs,
    find_metadata_entry,
    hashed_fallback_path,
    has_local_pdf,
    load_json_file,
    load_json_lines,
    load_metadata,
    log_line,
    record_result,
    save_json_file,
    setup_run_logger,
)

ADMIN_AJAX = "https://judicial.ky/wp-admin/admin-ajax.php"

_ONCLICK_FNAME_RE = re.compile(r"dl_bfile[^'\"]*['\"]([A-Za-z0-9]+)['\"]", re.IGNORECASE)
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


RESTART_BACKOFF_SECONDS = [5, 15, 45]


def _now_iso() -> str:
    """Return the current UTC time formatted for DB logging."""

    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _short_error_message(exc: Exception, max_length: int = 200) -> str:
    """Return a truncated string representation of ``exc`` for DB logging."""

    message = str(exc)
    if len(message) > max_length:
        return message[: max_length - 3] + "..."
    return message


class Checkpoint:
    """Persist and restore scraper progress between browser restarts."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data: Dict[str, Any] = {
            "mode": "",
            "current_page": 0,
            "last_button_index": -1,
            "processed_count": 0,
            "processed_tokens": [],
            "completed_downloads": [],
            "updated_at": None,
        }
        self._dirty = False

        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    self.data.update({k: loaded.get(k, v) for k, v in self.data.items()})
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

    # ------------------------------------------------------------------
    # Properties + helpers
    # ------------------------------------------------------------------

    @property
    def page_index(self) -> int:
        try:
            return int(self.data.get("current_page", 0))
        except (TypeError, ValueError):
            return 0

    @property
    def row_index(self) -> int:
        try:
            return int(self.data.get("last_button_index", -1))
        except (TypeError, ValueError):
            return -1

    @property
    def processed_tokens(self) -> Set[str]:
        return set(self._processed_tokens)

    @property
    def processed_count(self) -> int:
        try:
            return int(self.data.get("processed_count", 0))
        except (TypeError, ValueError):
            return 0

    def _touch(self) -> None:
        self.data["updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    def save(self, *, force: bool = False) -> None:
        if not force and not self._dirty:
            return
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(self.path)
        self._dirty = False

    def flush(self) -> None:
        self.save(force=True)

    def should_resume(self, mode: str, *, max_age_hours: int) -> bool:
        stored_mode = (self.data.get("mode") or "").strip().lower()
        if not stored_mode or stored_mode != mode.strip().lower():
            return False
        updated_raw = self.data.get("updated_at")
        if not updated_raw:
            return False
        try:
            updated = datetime.fromisoformat(updated_raw.replace("Z", "+00:00"))
        except ValueError:
            return False
        age = datetime.utcnow() - updated.replace(tzinfo=None)
        return age <= timedelta(hours=max_age_hours)

    # ------------------------------------------------------------------
    # Mutation helpers
    # ------------------------------------------------------------------

    def _update_position(
        self,
        page_index: int,
        row_index: int,
        *,
        mode: str,
    ) -> None:
        self.data["mode"] = mode
        self.data["current_page"] = int(max(0, page_index))
        self.data["last_button_index"] = int(max(-1, row_index))
        self._touch()
        self._dirty = True

    def mark_page(self, page_index: int, *, reset_row: bool = True, mode: str = "") -> None:
        row_index = -1 if reset_row else self.row_index
        self._update_position(page_index, row_index, mode=mode or (self.data.get("mode") or ""))

    def mark_row(self, row_index: int, *, mode: str = "") -> None:
        self._update_position(self.page_index, row_index, mode=mode or (self.data.get("mode") or ""))

    def mark_position(self, page_index: int, row_index: int, *, mode: str) -> None:
        self._update_position(page_index, row_index, mode=mode)

    def record_download(
        self,
        token: str,
        filename: str,
        *,
        mode: str,
        page_index: Optional[int] = None,
        row_index: Optional[int] = None,
    ) -> None:
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

        new_count = self.processed_count + 1
        self.data["processed_count"] = new_count
        if page_index is not None or row_index is not None:
            target_page = self.page_index if page_index is None else page_index
            target_row = self.row_index if row_index is None else row_index
            self._update_position(target_page, target_row, mode=mode)
        else:
            self.data["mode"] = mode
            self._touch()
            self._dirty = True

        if new_count % 10 == 0:
            self.save(force=True)


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


def _get_total_pages(page: Page) -> int:
    """Return the total number of DataTable pages available."""

    try:
        buttons = page.locator(
            "ul.pagination li.dt-paging-button button.page-link:not(.previous):not(.next)"
        )
        count = buttons.count()
    except PWError as exc:
        if _is_target_closed_error(exc):
            raise
        log_line(f"[RUN] Unable to enumerate pagination buttons: {exc}")
        return 1
    except Exception as exc:
        log_line(f"[RUN] Unable to enumerate pagination buttons: {exc}")
        return 1

    max_index = -1
    for idx in range(count):
        try:
            attr = buttons.nth(idx).get_attribute("data-dt-idx") or ""
        except PWError as exc:
            if _is_target_closed_error(exc):
                raise
            log_line(f"[RUN] Failed reading pagination attribute: {exc}")
            continue
        except Exception as exc:
            log_line(f"[RUN] Failed reading pagination attribute: {exc}")
            continue

        if not attr:
            continue
        try:
            value = int(attr)
        except ValueError:
            continue
        max_index = max(max_index, value)

    return max_index + 1 if max_index >= 0 else 1


def _goto_datatable_page(page: Page, page_index: int) -> bool:
    """Click the DataTable pagination button for ``page_index``."""

    if page_index <= 0:
        return True

    try:
        page.evaluate(
            """
            (idx) => {
              const dt = window.jQuery && window.jQuery.fn && window.jQuery.fn.dataTable ?
                         window.jQuery('#judgment-registers').DataTable() : null;
              if (dt) { dt.page(idx).draw('page'); }
            }
            """,
            page_index,
        )
        page.wait_for_selector(
            f"li.dt-paging-button.active:has(button[data-dt-idx=\"{page_index}\"])",
            timeout=20_000,
        )
        return True
    except PWError as exc:
        if _is_target_closed_error(exc):
            raise
        log_line(f"[RUN] Pagination button click failed: {exc}")
        return False
    except Exception as exc:
        log_line(f"[RUN] Pagination button click failed: {exc}")
        return False


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
    *,
    mode: str,
    fname: str,
    box_url: str,
    downloads_dir: Path,
    cases_by_action: Dict[str, Any],
    processed_this_run: Optional[Set[str]] = None,
    checkpoint: Optional[Checkpoint] = None,
    metadata: Optional[Dict[str, Any]] = None,
    http_client: Optional[Any] = None,
    case_context: Optional[Dict[str, Any]] = None,
    fid: Optional[str] = None,
) -> tuple[str, Dict[str, Any]]:
    """Process a dl_bfile AJAX response using explicit mode and dedupe semantics.

    Returns a tuple of ``(result, details)`` where ``result`` is the legacy
    status string (e.g. ``"downloaded"``, ``"existing_file"``, ``"failed"``)
    and ``details`` is a dictionary containing keys such as ``slug``,
    ``box_url``, ``file_path``, ``file_size_bytes``, and ``error_message``.
    """

    norm_fname = normalize_action_token(fname)
    display_name = fname or norm_fname
    canonical_token = canon_fname(fname)
    download_details: Dict[str, Any] = {
        "slug": None,
        "box_url": box_url,
        "file_path": None,
        "file_size_bytes": None,
        "error_message": None,
    }
    if not norm_fname:
        log_line(
            f"[AJAX][WARN] Unable to normalise fname token '{display_name}'; skipping."
        )
        return "failed", {**download_details, "error_message": "invalid_token"}

    if processed_this_run is not None and canonical_token:
        if canonical_token in processed_this_run:
            log_line(
                f"[AJAX] fname {display_name} already processed earlier in this run; ignoring duplicate response."
            )
            return "duplicate_in_run", {**download_details, "slug": norm_fname}

    case_row = case_context.get("case") if case_context else None
    if case_row is None:
        case_row = cases_by_action.get(norm_fname)

    slug = norm_fname
    if case_row is not None and getattr(case_row, "action", None):
        slug = normalize_action_token(case_row.action) or norm_fname

    page_index_hint = (case_context or {}).get("page_index")
    row_index_hint = (case_context or {}).get("row_index")

    if case_row is not None:
        subject_label = case_row.title or case_row.subject or display_name
        log_line(
            f"[AJAX] dl_bfile fname={display_name} resolved to case='{subject_label}'"
        )
    else:
        subject_label = display_name
        log_line(
            f"[AJAX][WARN] No case mapping found for fname={display_name}; proceeding generically."
        )

    download_details["slug"] = slug
    downloads_dir = Path(downloads_dir).resolve()
    downloads_dir.mkdir(parents=True, exist_ok=True)

    cause_number = None
    judgment_date = None
    court = None
    category = None
    if case_row is not None:
        cause_number = getattr(case_row, "cause_number", None) or case_row.extra.get(
            "Cause Number"
        )
        judgment_date = getattr(case_row, "judgment_date", None) or case_row.extra.get(
            "Judgment Date"
        )
        category = getattr(case_row, "category", None) or case_row.extra.get("Category")
        court = getattr(case_row, "court", None) or case_row.extra.get("Court")

    title_label = case_row.title if case_row else subject_label
    pdf_path = build_pdf_path(
        downloads_dir,
        title_label,
        default_stem=slug,
    )
    log_line(
        f"[AJAX] dl_bfile fname={display_name} expected_path='{pdf_path.name}'"
    )

    final_path = pdf_path
    if final_path.exists():
        try:
            size_bytes = final_path.stat().st_size
        except OSError:
            size_bytes = 0
        log_line(
            f"[AJAX] Local file exists for fname={display_name}; counted as processed."
        )
        if metadata is not None:
            record_result(
                metadata,
                slug=slug,
                fid=fid or slug,
                title=title_label,
                local_filename=final_path.name,
                source_url=box_url,
                size_bytes=size_bytes,
                category=category,
                judgment_date=judgment_date,
                court=court,
                cause_number=cause_number,
                subject=subject_label,
                local_path=str(final_path.resolve()),
            )
        if checkpoint is not None:
            checkpoint.record_download(
                slug,
                str(final_path.name),
                mode=mode,
                page_index=page_index_hint,
                row_index=row_index_hint,
            )
        return "existing_file", {**download_details, "file_path": str(final_path.name)}

    if is_new_mode(mode) and checkpoint is not None:
        processed_tokens = checkpoint.processed_tokens
        if norm_fname in processed_tokens:
            log_line(
                f"[AJAX] {display_name} previously completed; skip in NEW mode."
            )
            return "checkpoint_skip", download_details

    meta = metadata or {}
    meta_entry: Optional[Dict[str, Any]] = None
    if case_context and case_context.get("metadata_entry"):
        meta_entry = case_context["metadata_entry"]

    if meta_entry is None and meta:
        meta_entry, _ = find_metadata_entry(meta, slug=slug, filename=pdf_path.name)
        if meta_entry is None and slug != norm_fname:
            meta_entry, _ = find_metadata_entry(
                meta, slug=norm_fname, filename=pdf_path.name
            )
        if meta_entry is None:
            meta_entry, _ = find_metadata_entry(meta, filename=pdf_path.name)

    if meta_entry and has_local_pdf(meta_entry):
        label = meta_entry.get("title") or meta_entry.get("slug") or subject_label
        log_line(
            f"[AJAX] Metadata and local file confirm {label}; skipping download."
        )
        if checkpoint is not None:
            checkpoint.record_download(
                slug,
                str(pdf_path.name),
                mode=mode,
                page_index=page_index_hint,
                row_index=row_index_hint,
            )
        return "existing_file", {**download_details, "file_path": str(pdf_path.name)}

    try:
        if pdf_path.exists() and pdf_path.stat().st_size > 0:
            log_line(
                f"[AJAX] Local file {pdf_path.name} already exists; skipping download."
            )
            if meta_entry is None and meta is not None:
                record_result(
                    meta,
                    slug=slug,
                    fid=fid or slug,
                    title=title_label,
                    local_filename=pdf_path.name,
                    source_url=box_url,
                    size_bytes=pdf_path.stat().st_size,
                    category=category,
                    judgment_date=judgment_date,
                    court=court,
                    cause_number=cause_number,
                    subject=subject_label,
                    local_path=str(pdf_path.resolve()),
                )
            if checkpoint is not None:
                checkpoint.record_download(
                    slug,
                    str(pdf_path.name),
                    mode=mode,
                    page_index=page_index_hint,
                    row_index=row_index_hint,
                )
            return "existing_file", {**download_details, "file_path": str(pdf_path.name)}
    except OSError:
        pass

    if not disk_has_room(config.MIN_FREE_MB, downloads_dir):
        log_line(
            f"[AJAX][STOP] Insufficient disk space (<{config.MIN_FREE_MB} MB free); aborting before download."
        )
        return "disk_full", {**download_details, "error_message": "disk_full"}

    success = False
    error: Optional[str] = None
    try:
        success, error = queue_or_download_file(
            box_url,
            pdf_path,
            http_client=http_client,
        )
    except OSError as exc:
        error = str(exc)
        success = False

    if not success and error:
        lowered = error.lower()
        if "file name too long" in lowered or "errno 36" in lowered or "errno 63" in lowered:
            fallback_path = hashed_fallback_path(downloads_dir, subject_label)
            if fallback_path != pdf_path:
                log_line(
                    f"[AJAX] Retrying save for {display_name} with fallback {fallback_path.name}"
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
        log_line(
            f"[AJAX] Download failed for {display_name} -> {final_path.name}: {error}"
        )
        final_path.unlink(missing_ok=True)
        return "failed", {**download_details, "error_message": error or "unknown"}

    size_bytes = final_path.stat().st_size
    log_line(
        f"[AJAX] Saved fname={display_name} -> {final_path} ({size_bytes/1024:.1f} KiB)"
    )

    if processed_this_run is not None and canonical_token:
        processed_this_run.add(canonical_token)

    if meta is not None:
        record_result(
            meta,
            slug=slug,
            fid=fid or slug,
            title=title_label,
            local_filename=final_path.name,
            source_url=box_url,
            size_bytes=size_bytes,
            category=category,
            judgment_date=judgment_date,
            court=court,
            cause_number=cause_number,
            subject=subject_label,
            local_path=str(final_path.resolve()),
        )

    try:
        relative_path = final_path.relative_to(config.PDF_DIR)
        saved_path_value: str = str(relative_path)
    except ValueError:
        saved_path_value = final_path.name

    append_json_line(
        config.DOWNLOADS_LOG,
        {
            "actions_token": slug,
            "title": title_label,
            "subject": subject_label,
            "court": court,
            "category": category,
            "cause_number": cause_number,
            "judgment_date": judgment_date,
            "saved_path": saved_path_value,
            "bytes": size_bytes,
            "downloaded_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        },
    )

    if checkpoint is not None:
        checkpoint.record_download(
            slug,
            str(final_path.name),
            mode=mode,
            page_index=page_index_hint,
            row_index=row_index_hint,
        )

    return "downloaded", {
        **download_details,
        "file_path": saved_path_value,
        "file_size_bytes": size_bytes,
        "box_url": box_url,
    }


# ---------------------------------------------------------------------------
# Retry helpers
# ---------------------------------------------------------------------------


def retry_failed_downloads(
    *,
    page: Optional[Page],
    failed_items: List[Dict[str, Any]],
    scrape_mode: str,
    pending_by_fname: Dict[str, Dict[str, Any]],
    processed_this_run: Set[str],
    checkpoint: Optional[Checkpoint],
) -> None:
    """Retry download clicks for items that previously failed."""

    if page is None or page.is_closed():
        return

    if not failed_items:
        return

    log_line(f"[RETRY] Attempting {len(failed_items)} failed download retries.")

    for item in list(failed_items):
        if page.is_closed():
            log_line("[RETRY] Page closed before retry could complete; aborting remaining retries.")
            break

        fname = item.get("fname")
        if not fname:
            continue

        page_index = int(item.get("page_index", 0))
        button_index = int(item.get("button_index", 0))

        try:
            if not _set_datatable_page(page, page_index):
                log_line(
                    f"[RETRY] Unable to navigate to DataTable page {page_index + 1} for {fname}; skipping."
                )
                continue
        except PWError as exc:
            if _is_target_closed_error(exc):
                log_line("[RETRY] Target closed while navigating for retry; aborting retries.")
                break
            log_line(f"[RETRY] Error setting page for retry: {exc}")
            continue

        wait_seconds(page, 0.3)

        locator = None
        for selector in _guess_download_locators():
            try:
                candidates = page.locator(selector)
                if candidates.count() > button_index:
                    locator = candidates.nth(button_index)
                    break
            except PWError as exc:
                if _is_target_closed_error(exc):
                    log_line("[RETRY] Target closed while locating button; aborting retries.")
                    locator = None
                    break
                log_line(f"[RETRY] Locator error for selector {selector!r}: {exc}")
            except Exception as exc:
                log_line(f"[RETRY] Unexpected locator error for selector {selector!r}: {exc}")

        if locator is None:
            log_line(
                f"[RETRY] Could not locate button index {button_index} on page {page_index + 1} for {fname}; skipping."
            )
            continue

        pending_by_fname[fname] = {
            "case": item.get("case"),
            "metadata_entry": item.get("metadata_entry"),
            "slug": item.get("slug") or fname,
            "raw": item.get("raw") or fname,
            "page_index": page_index,
            "row_index": button_index,
            "fid": item.get("fid"),
        }

        try:
            locator.click(timeout=2000)
            processed_this_run.discard(canon_fname(fname))
            wait_seconds(page, 0.4)
        except PWError as exc:
            if _is_target_closed_error(exc):
                log_line("[RETRY] Target closed during retry click; aborting remaining retries.")
                break
            log_line(f"[RETRY] Click failed for {fname}: {exc}")
            continue
        except Exception as exc:
            log_line(f"[RETRY] Click failed for {fname}: {exc}")
            continue

        if checkpoint is not None:
            checkpoint.mark_position(page_index, button_index, mode=scrape_mode)

# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------



def run_with_retries(
    run_callable: Callable[[], Dict[str, Any]],
    *,
    max_retries: int,
) -> Dict[str, Any]:
    """Execute ``run_callable`` with retries for Playwright crashes."""

    effective_retries = max(1, max_retries)
    for attempt in range(1, effective_retries + 1):
        try:
            log_line(
                f"[RUN] Starting scraping run attempt {attempt}/{effective_retries}..."
            )
            result = run_callable()
            log_line("[RUN] Scraping run completed successfully.")
            return result
        except PWError as exc:
            log_line(f"[RUN] Playwright error on attempt {attempt}: {exc}")
            if attempt >= effective_retries:
                log_line("[RUN] Max retries reached; aborting after repeated crashes.")
                raise
            delay = RESTART_BACKOFF_SECONDS[
                min(attempt - 1, len(RESTART_BACKOFF_SECONDS) - 1)
            ]
            log_line(f"[RUN] Retrying in {delay}s after Playwright error...")
            time.sleep(delay)
        except Exception:
            log_line("[RUN] Unexpected non-Playwright error; aborting without retry.")
            raise

    raise RuntimeError("run_with_retries exhausted without returning a result")


def _run_scrape_attempt(
    base_url: Optional[str] = None,
    page_wait: Optional[int] = None,
    per_delay: Optional[float] = None,
    start_message: Optional[str] = None,
    *,
    scrape_mode: str,
    new_limit: Optional[int],
    log_path: Path,
    max_retries: int,
    checkpoint: Optional[Checkpoint],
    resume_enabled: bool,
    resume_state: Optional[Dict[str, Any]] = None,
    limit_pages: Optional[List[int]] = None,
    row_limit_override: Optional[int] = None,
    run_id: Optional[int] = None,
    csv_source: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute a scraping run with automatic restart/resume support."""

    if start_message:
        log_line(start_message)

    base_url = (base_url or config.DEFAULT_BASE_URL).strip()
    page_wait = page_wait or config.PAGE_WAIT_SECONDS
    per_delay = per_delay if per_delay is not None else config.PER_DOWNLOAD_DELAY
    row_limit = (
        row_limit_override
        if row_limit_override is not None
        else new_limit
        if new_limit is not None
        else config.SCRAPE_NEW_LIMIT
    )

    telemetry = RunTelemetry(scrape_mode)

    log_line("=== Starting scraping run (Playwright, response-capture) ===")
    log_line(f"Target base URL: {base_url}")
    log_line(
        "Params: scrape_mode=%s, row_limit=%s, page_wait=%s, per_download_delay=%s"
        % (scrape_mode, row_limit if scrape_mode == "new" else "all", page_wait, per_delay)
    )

    log_line(
        f"[CASES_INDEX] using {'db' if cases_index.should_use_db_index() else 'csv'} backend"
    )
    effective_csv_source = csv_source or config.CSV_URL
    load_cases_index(effective_csv_source)
    meta = load_metadata()
    downloaded_index: Dict[str, Dict[str, Any]] = {}
    for entry in meta.get("downloads", []):
        if not isinstance(entry, dict):
            continue
        slug_value = normalize_action_token(entry.get("slug") or entry.get("fid") or "")
        if not slug_value:
            continue
        if entry.get("downloaded") and has_local_pdf(entry):
            downloaded_index[slug_value] = entry

    summary: Dict[str, Any] = {
        "base_url": base_url,
        "processed": 0,
        "downloaded": 0,
        "failed": 0,
        "skipped": 0,
        "inspected_rows": 0,
        "total_cases": len(CASES_BY_ACTION),
        "log_file": str(log_path),
        "scrape_mode": scrape_mode,
        "skip_reasons": {},
        "fail_reasons": {},
    }

    def _lookup_case_id(token_norm: str) -> Optional[int]:
        if run_id is None or not token_norm:
            return None
        try:
            return db.get_case_id_by_token_norm("unreported_judgments", token_norm)
        except Exception as exc:  # noqa: BLE001
            log_line(f"[DB][WARN] Failed to resolve case id for token={token_norm}: {exc}")
            return None

    def _log_skip_status(case_id: Optional[int], reason: str) -> None:
        if run_id is None or case_id is None:
            return
        try:
            db.ensure_download_row(run_id, case_id)
            db.update_download_status(
                run_id=run_id,
                case_id=case_id,
                status="skipped",
                attempt_count=0,
                last_attempt_at=_now_iso(),
                error_code=reason,
            )
        except Exception as exc:  # noqa: BLE001
            log_line(f"[DB][WARN] Unable to record skip for case_id={case_id}: {exc}")

    def _start_download_attempt(case_id: Optional[int], box_url: Optional[str]) -> Optional[int]:
        if run_id is None or case_id is None:
            return None
        try:
            row = db.ensure_download_row(run_id, case_id)
            attempt = int(row["attempt_count"]) + 1
            db.update_download_status(
                run_id=run_id,
                case_id=case_id,
                status="in_progress",
                attempt_count=attempt,
                last_attempt_at=_now_iso(),
                box_url_last=box_url,
            )
            return attempt
        except Exception as exc:  # noqa: BLE001
            log_line(
                f"[DB][WARN] Unable to start download attempt for case_id={case_id}: {exc}"
            )
            return None

    def _finish_download_attempt(
        case_id: Optional[int],
        status: str,
        attempt_count: Optional[int],
        *,
        file_path: Optional[str] = None,
        file_size_bytes: Optional[int] = None,
        box_url_last: Optional[str] = None,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        if run_id is None or case_id is None:
            return
        attempts = attempt_count if attempt_count is not None else 0
        try:
            db.update_download_status(
                run_id=run_id,
                case_id=case_id,
                status=status,
                attempt_count=attempts,
                last_attempt_at=_now_iso(),
                file_path=file_path,
                file_size_bytes=file_size_bytes,
                box_url_last=box_url_last,
                error_code=error_code,
                error_message=error_message,
            )
        except Exception as exc:  # noqa: BLE001
            log_line(
                f"[DB][WARN] Unable to update download status for case_id={case_id}: {exc}"
            )

    def _finalize_telemetry(payload: Dict[str, Any]) -> None:
        try:
            telemetry.finalize(payload)
        except Exception as exc:  # noqa: BLE001
            log_line(f"[TELEMETRY] Unable to write run telemetry: {exc}")

    def _persist_state(
        page_index: int,
        button_index: int,
        *,
        last_fname: Optional[str] = None,
        last_title: Optional[str] = None,
    ) -> None:
        try:
            save_checkpoint(
                dt_page_index=page_index,
                button_index=button_index,
                last_fname=last_fname,
                last_title=last_title,
                downloaded_count=summary.get("downloaded", 0),
                skipped_count=summary.get("skipped", 0),
                failed_count=summary.get("failed", 0),
            )
        except Exception as exc:  # noqa: BLE001
            log_line(f"[STATE] Unable to persist checkpoint: {exc}")

    if not CASES_BY_ACTION:
        log_line("No cases loaded from judgments CSV; aborting.")
        _finalize_telemetry(summary)
        return summary

    def _bump_reason(target: Dict[str, int], reason: str) -> None:
        if not reason:
            return
        target[reason] = int(target.get(reason, 0)) + 1

    attempt = 0
    while attempt <= max_retries:
        consecutive_existing = 0
        processed_this_run: Set[str] = set()
        pending_by_fname: Dict[str, Dict[str, Any]] = {}
        failed_items: List[Dict[str, Any]] = []
        active = True
        browser: Optional[Browser] = None
        context: Optional[BrowserContext] = None
        page: Optional[Page] = None
        resume_page_index = 0
        resume_row_index = -1
        if resume_state:
            try:
                resume_page_index = max(0, int(resume_state.get("dt_page_index", 0)))
            except Exception:
                resume_page_index = 0
            try:
                resume_row_index = int(resume_state.get("button_index", -1))
            except Exception:
                resume_row_index = -1
            log_line(
                f"[RUN] Resume requested -> page_index={resume_page_index}, row_index={resume_row_index}"
            )
        elif (
            checkpoint is not None
            and resume_enabled
            and checkpoint.should_resume(
                scrape_mode, max_age_hours=config.SCRAPE_RESUME_MAX_AGE_HOURS
            )
        ):
            resume_page_index = max(0, checkpoint.page_index)
            resume_row_index = checkpoint.row_index
            log_line(
                f"[RUN] Resume checkpoint -> page_index={resume_page_index}, row_index={resume_row_index}"
            )
        else:
            if checkpoint is not None:
                checkpoint.mark_page(0, mode=scrape_mode)
                checkpoint.mark_row(-1, mode=scrape_mode)
        crash_stop = False

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
                        nonlocal consecutive_existing, row_limit_reached, active
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

                            if not isinstance(payload, dict):
                                summary["failed"] += 1
                                return

                            data = payload.get("data") if isinstance(payload, dict) else None
                            box_url = _extract_box_url(data or {}) if data else None
                            if not box_url:
                                log_line(
                                    f"[AJAX] Could not find Box URL in payload for fname={fname_param}"
                                )
                                summary["failed"] += 1
                                return

                            http_fetcher = (
                                lambda download_url, timeout=120: context.request.get(
                                    download_url,
                                    timeout=timeout * 1000,
                                )
                            )

                            norm_fname = normalize_action_token(fname_param or "")
                            case_context = (
                                pending_by_fname.get(norm_fname) if norm_fname else None
                            )
                            if case_context is not None:
                                case_context = dict(case_context)
                                if fid_param:
                                    case_context.setdefault("fid", fid_param)
                            db_token_norm = normalize_action_token_db(
                                fname_param or norm_fname or ""
                            )
                            case_id = _lookup_case_id(db_token_norm)
                            attempt_count_db = _start_download_attempt(case_id, box_url)
                            result, download_info = handle_dl_bfile_from_ajax(
                                mode=scrape_mode,
                                fname=fname_param or norm_fname,
                                box_url=box_url,
                                downloads_dir=config.PDF_DIR,
                                cases_by_action=CASES_BY_ACTION,
                                processed_this_run=processed_this_run,
                                checkpoint=checkpoint if is_new_mode(scrape_mode) else None,
                                metadata=meta,
                                http_client=http_fetcher,
                                case_context=case_context,
                                fid=fid_param,
                            )

                            if norm_fname:
                                pending_by_fname.pop(norm_fname, None)

                                def _remove_failed_record(token: str) -> None:
                                    for idx, item in enumerate(failed_items):
                                        if item.get("fname") == token:
                                            failed_items.pop(idx)
                                            summary["failed"] = max(0, summary["failed"] - 1)
                                            break

                            disk_full_encountered = False

                            if result == "downloaded":
                                summary["downloaded"] += 1
                                consecutive_existing = 0
                                if norm_fname:
                                    _remove_failed_record(norm_fname)
                            elif result == "failed":
                                summary["failed"] += 1
                                _bump_reason(summary["fail_reasons"], "download_other")
                                if norm_fname and case_context:
                                    if not any(item.get("fname") == norm_fname for item in failed_items):
                                        failed_items.append(
                                            {
                                                "fname": norm_fname,
                                                "raw": case_context.get("raw") or fname_param,
                                                "page_index": case_context.get("page_index", 0),
                                                "button_index": case_context.get("row_index", 0),
                                                "metadata_entry": case_context.get("metadata_entry"),
                                                "case": case_context.get("case"),
                                                "slug": case_context.get("slug"),
                                                "fid": case_context.get("fid"),
                                            }
                                        )
                            elif result == "existing_file":
                                summary["skipped"] += 1
                                consecutive_existing += 1
                                if norm_fname:
                                    _remove_failed_record(norm_fname)
                                _bump_reason(summary["skip_reasons"], "exists_ok")
                            elif result == "checkpoint_skip":
                                summary["skipped"] += 1
                                consecutive_existing += 1
                                if norm_fname:
                                    _remove_failed_record(norm_fname)
                                _bump_reason(summary["skip_reasons"], "seen_history")
                            elif result == "duplicate_in_run":
                                summary["skipped"] += 1
                                if norm_fname:
                                    _remove_failed_record(norm_fname)
                                _bump_reason(summary["skip_reasons"], "in_run_dup")
                            elif result == "disk_full":
                                summary.setdefault("stop_reason", "disk_full")
                                disk_full_encountered = True

                            status_for_db: Optional[str] = None
                            error_code: Optional[str] = None
                            error_message = download_info.get("error_message") if isinstance(download_info, dict) else None
                            file_path = download_info.get("file_path") if isinstance(download_info, dict) else None
                            file_size_bytes = download_info.get("file_size_bytes") if isinstance(download_info, dict) else None
                            box_url_last = download_info.get("box_url") if isinstance(download_info, dict) else box_url

                            if result == "downloaded":
                                status_for_db = "downloaded"
                            elif result in {"existing_file", "checkpoint_skip", "duplicate_in_run"}:
                                status_for_db = "skipped"
                            elif result == "failed":
                                status_for_db = "failed"
                                error_code = "unknown"
                            elif result == "disk_full":
                                status_for_db = "failed"
                                error_code = "disk_full"

                            if status_for_db:
                                _finish_download_attempt(
                                    case_id,
                                    status_for_db,
                                    attempt_count_db if status_for_db != "skipped" else attempt_count_db or 0,
                                    file_path=file_path,
                                    file_size_bytes=file_size_bytes,
                                    box_url_last=box_url_last,
                                    error_code=error_code,
                                    error_message=error_message,
                                )

                            if disk_full_encountered:
                                active = False
                                return

                            page_hint = case_context.get("page_index") if case_context else None
                            row_hint = case_context.get("row_index") if case_context else None
                            case_obj = case_context.get("case") if case_context else None
                            meta_entry = (
                                case_context.get("metadata_entry") if case_context else None
                            )
                            last_title = None
                            if case_obj is not None:
                                last_title = getattr(case_obj, "title", None)
                                if last_title is None and isinstance(case_obj, dict):
                                    last_title = case_obj.get("title")

                            meta_payload = {
                                "fname": fname_param or norm_fname or "",
                                "title": last_title or (meta_entry or {}).get("title") or "",
                                "subject": getattr(case_obj, "subject", None)
                                or (meta_entry or {}).get("subject")
                                or "",
                                "court": getattr(case_obj, "court", None)
                                or (meta_entry or {}).get("court")
                                or "",
                                "category": getattr(case_obj, "category", None)
                                or (meta_entry or {}).get("category")
                                or "",
                                "cause_no": getattr(case_obj, "cause_number", None)
                                or (meta_entry or {}).get("cause_number")
                                or "",
                                "judgment_date": getattr(case_obj, "judgment_date", None)
                                or (meta_entry or {}).get("judgment_date")
                                or "",
                                "page": page_hint if page_hint is not None else 0,
                                "idx": row_hint if row_hint is not None else 0,
                                "file_path": (meta_entry or {}).get("local_path")
                                or (meta_entry or {}).get("saved_path")
                                or "",
                                "size": (meta_entry or {}).get("bytes") or 0,
                            }

                            if result == "downloaded":
                                telemetry.add("downloaded", "ok", meta_payload)
                            elif result == "failed":
                                telemetry.add("failed", "download_other", meta_payload)
                            elif result in {"existing_file", "checkpoint_skip"}:
                                telemetry.add("skipped", "exists", meta_payload)
                            elif result == "duplicate_in_run":
                                telemetry.add("skipped", "in_run_dup", meta_payload)
                            _persist_state(
                                page_hint if page_hint is not None else resume_page_index,
                                (row_hint + 1) if row_hint is not None else 0,
                                last_fname=fname_param or norm_fname,
                                last_title=last_title,
                            )

                            if (
                                is_new_mode(scrape_mode)
                                and consecutive_existing >= config.SCRAPE_NEW_CONSECUTIVE_LIMIT
                            ):
                                log_line(
                                    "[RUN] Consecutive already-downloaded threshold reached; halting NEW mode run."
                                )
                                row_limit_reached = True
                                active = False

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
                    row_limit_reached = False
                    rows_evaluated = summary.get("inspected_rows", 0)
                    resume_consumed = False
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
                        try:
                            total_pages = max(1, _get_total_pages(page))
                        except PWError as exc:
                            if _is_target_closed_error(exc):
                                log_line(
                                    "[RUN] Browser target crashed while reading pagination; stopping scrape gracefully."
                                )
                                crash_stop = True
                                active = False
                            else:
                                log_line(f"[RUN] Failed to read pagination: {exc}")
                            total_pages = 0

                        if resume_page_index >= total_pages:
                            resume_page_index = max(0, total_pages - 1)

                        page_indices = list(range(resume_page_index, total_pages))
                        if limit_pages:
                            page_indices = [
                                idx
                                for idx in sorted(set(limit_pages))
                                if resume_page_index <= idx < total_pages
                            ]

                        for page_index_zero in page_indices:
                            if crash_stop or row_limit_reached:
                                break

                            page_number = page_index_zero + 1
                            if not _goto_datatable_page(page, page_index_zero):
                                log_line(
                                    f"[RUN] Unable to navigate to DataTable page {page_number}; stopping pagination loop."
                                )
                                break

                            if checkpoint is not None:
                                checkpoint.mark_page(page_index_zero, reset_row=False, mode=scrape_mode)
                            _persist_state(page_index_zero, -1)

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

                            clicked_on_page: Set[str] = set()

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

                                if (
                                    scrape_mode == "new"
                                    and row_limit is not None
                                    and row_limit > 0
                                    and rows_evaluated >= row_limit
                                ):
                                    log_line(
                                        "Row inspection limit reached for 'new' mode; stopping pagination loop."
                                    )
                                    row_limit_reached = True
                                    break

                                rows_evaluated += 1
                                summary["inspected_rows"] = rows_evaluated

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
                                canonical_token = canon_fname(fname_token or fname_key)
                                db_token_norm = normalize_action_token_db(fname_token or fname_key)
                                case_id_for_logging = _lookup_case_id(db_token_norm)
                                if not fname_key:
                                    log_line(
                                        f"[AJAX][WARN] Skipping button index {i} on page {page_number}: unable to normalise fname token."
                                    )
                                    if checkpoint is not None:
                                        checkpoint.mark_position(page_index_zero, i, mode=scrape_mode)
                                    _log_skip_status(case_id_for_logging, "invalid_token")
                                    continue

                                if (
                                    canonical_token
                                    and canonical_token in processed_this_run
                                ):
                                    log_line(
                                        f"[SKIP] fname={fname_token} already handled earlier in this run; skipping duplicate button."
                                    )
                                    if checkpoint is not None:
                                        checkpoint.mark_position(page_index_zero, i, mode=scrape_mode)
                                    summary["skipped"] += 1
                                    _bump_reason(summary["skip_reasons"], "in_run_dup")
                                    _log_skip_status(case_id_for_logging, "in_run_dup")
                                    continue

                                dedupe_key = canonical_token or fname_key
                                if dedupe_key and dedupe_key in clicked_on_page:
                                    log_line(
                                        f"[SKIP] Button for fname={fname_token} already clicked on page {page_number}; skipping duplicate element."
                                    )
                                    _bump_reason(summary["skip_reasons"], "in_run_dup")
                                    _log_skip_status(case_id_for_logging, "in_run_dup")
                                    continue

                                case_for_fname = CASES_BY_ACTION.get(fname_key)
                                if case_for_fname is None:
                                    log_line(
                                        f"[SKIP][csv_miss] No CSV entry for fname={fname_token}; skipping."
                                    )
                                    if checkpoint is not None:
                                        checkpoint.mark_position(page_index_zero, i, mode=scrape_mode)
                                    summary["skipped"] += 1
                                    _bump_reason(summary["skip_reasons"], "csv_miss")
                                    _log_skip_status(case_id_for_logging, "csv_miss")
                                    continue

                                metadata_entry = downloaded_index.get(fname_key)
                                if metadata_entry and has_local_pdf(metadata_entry):
                                    label = metadata_entry.get("title") or fname_key
                                    log_line(
                                        f"[SKIP] fname={fname_token} already downloaded as {label}; skipping click."
                                    )
                                    if checkpoint is not None:
                                        checkpoint.record_download(
                                            fname_key,
                                            metadata_entry.get("local_filename")
                                            or metadata_entry.get("filename")
                                            or "",
                                            mode=scrape_mode,
                                            page_index=page_index_zero,
                                            row_index=i,
                                        )
                                        checkpoint.mark_position(page_index_zero, i, mode=scrape_mode)
                                    summary["skipped"] += 1
                                    _bump_reason(summary["skip_reasons"], "exists_ok")
                                    _log_skip_status(case_id_for_logging, "exists_ok")
                                    if is_new_mode(scrape_mode):
                                        consecutive_existing += 1
                                        if (
                                            consecutive_existing
                                            >= config.SCRAPE_NEW_CONSECUTIVE_LIMIT
                                        ):
                                            log_line(
                                                "[RUN] Consecutive already-downloaded threshold reached; halting NEW mode run."
                                            )
                                            row_limit_reached = True
                                            break
                                    continue

                                if (
                                    checkpoint is not None
                                    and is_new_mode(scrape_mode)
                                    and fname_key in checkpoint.processed_tokens
                                ):
                                    log_line(
                                        f"[SKIP] fname={fname_token} recorded in checkpoint; skipping click in NEW mode."
                                    )
                                    if checkpoint is not None:
                                        checkpoint.mark_position(page_index_zero, i, mode=scrape_mode)
                                    summary["skipped"] += 1
                                    _bump_reason(summary["skip_reasons"], "seen_history")
                                    if is_new_mode(scrape_mode):
                                        consecutive_existing += 1
                                        if (
                                            consecutive_existing
                                            >= config.SCRAPE_NEW_CONSECUTIVE_LIMIT
                                        ):
                                            log_line(
                                                "[RUN] Consecutive already-downloaded threshold reached; halting NEW mode run."
                                            )
                                            row_limit_reached = True
                                            break
                                    continue

                                slug_value = case_for_fname.action if case_for_fname else fname_key

                                pending_by_fname[fname_key] = {
                                    "case": case_for_fname,
                                    "metadata_entry": metadata_entry,
                                    "slug": slug_value,
                                    "raw": fname_token,
                                    "page_index": page_index_zero,
                                    "row_index": i,
                                    "canonical": canonical_token,
                                }

                                if dedupe_key:
                                    clicked_on_page.add(dedupe_key)

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
                                    _bump_reason(summary["fail_reasons"], "click_timeout")
                                    if checkpoint is not None:
                                        checkpoint.mark_position(page_index_zero, i, mode=scrape_mode)
                                    continue

                                total_clicks += 1
                                summary["processed"] = total_clicks
                                log_line(
                                    f"Clicked download button index {i} on page {page_number} (fname={fname_token})."
                                )

                                time.sleep((per_delay or 0) + 0.4)

                        if crash_stop:
                            log_line(
                                "[RUN] Pagination loop terminated early due to browser crash; collected downloads will be preserved."
                            )

                    if failed_items:
                        retry_failed_downloads(
                            page=page,
                            failed_items=failed_items,
                            scrape_mode=scrape_mode,
                            pending_by_fname=pending_by_fname,
                            processed_this_run=processed_this_run,
                            checkpoint=checkpoint if is_new_mode(scrape_mode) else None,
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
            if attempt > max_retries:
                log_line("[RUN] Exhausted restart attempts; aborting scrape.")
                raise
            delay = RESTART_BACKOFF_SECONDS[
                min(attempt - 1, len(RESTART_BACKOFF_SECONDS) - 1)
            ]
            log_line(
                f"[RUN] Restarting browser in {delay}s (attempt {attempt}/{max_retries})"
            )
            time.sleep(delay)
            continue
        else:
            break

    log_line("Completed run (response-capture strategy).")
    if checkpoint is not None:
        checkpoint.flush()
    log_line(
        "[RUN] Totals: mode=%s, processed=%s, downloaded=%s, skipped=%s, failed=%s, inspected_rows=%s, total_cases=%s"
        % (
            scrape_mode,
            summary.get("processed"),
            summary.get("downloaded"),
            summary.get("skipped"),
            summary.get("failed"),
            summary.get("inspected_rows"),
            summary.get("total_cases"),
        )
    )
    save_json_file(config.SUMMARY_FILE, summary)
    _finalize_telemetry(summary)
    return summary


def run_scrape(
    base_url: Optional[str] = None,
    page_wait: Optional[int] = None,
    per_delay: Optional[float] = None,
    start_message: Optional[str] = None,
    *,
    scrape_mode: Optional[str] = None,
    new_limit: Optional[int] = None,
    max_retries: Optional[int] = None,
    resume: Optional[bool] = None,
    resume_mode: str = "none",
    resume_page: Optional[int] = None,
    resume_index: Optional[int] = None,
    limit_pages: Optional[List[int]] = None,
    row_limit: Optional[int] = None,
    trigger: str = "cli",
) -> Dict[str, Any]:
    """Public entrypoint that wraps the core scraper with retry support."""

    ensure_dirs()
    db.initialize_schema()
    log_path = setup_run_logger()

    raw_mode = (scrape_mode or config.SCRAPE_MODE_DEFAULT).strip().lower()
    if is_full_mode(raw_mode):
        mode = "full"
    elif is_new_mode(raw_mode):
        mode = "new"
    else:
        log_line(
            f"[RUN] Unknown scrape_mode={raw_mode!r}; defaulting to 'new'."
        )
        mode = "new"

    row_limit = new_limit if new_limit is not None else config.SCRAPE_NEW_LIMIT
    retry_limit = max_retries if max_retries is not None else config.SCRAPER_MAX_RETRIES

    resume_enabled = config.SCRAPE_RESUME_DEFAULT if resume is None else bool(resume)
    checkpoint: Optional[Checkpoint]
    if resume_enabled:
        checkpoint = Checkpoint(config.RUN_STATE_FILE)
    else:
        checkpoint = None

    resume_state: Optional[Dict[str, Any]] = None
    normalized_resume = (resume_mode or "none").strip().lower()
    if normalized_resume != "none":
        state: Optional[Dict[str, Any]] = None
        if normalized_resume in {"state", "auto"}:
            state = load_checkpoint()
        if state is None and normalized_resume in {"logs", "auto"}:
            state = derive_checkpoint_from_logs()
        if state is not None:
            resume_state = dict(state)
        if resume_page is not None:
            resume_state = resume_state or {}
            resume_state["dt_page_index"] = resume_page
        if resume_index is not None:
            resume_state = resume_state or {}
            resume_state["button_index"] = resume_index

    next_start_message = start_message
    run_id: Optional[int] = None

    http_session = csv_sync.build_http_session()
    sync_result = csv_sync.sync_csv(config.CSV_URL, session=http_session)
    csv_version_id = sync_result.version_id
    csv_source = sync_result.csv_path or config.CSV_URL

    params_json = json.dumps(
        {
            "base_url": base_url,
            "page_wait": page_wait,
            "per_delay": per_delay,
            "scrape_mode": mode,
            "max_retries": retry_limit,
            "resume": resume_enabled,
            "resume_mode": resume_mode,
            "resume_page": resume_page,
            "resume_index": resume_index,
            "limit_pages": limit_pages,
            "row_limit": row_limit,
        },
        sort_keys=True,
    )

    mode_for_db = "resume" if resume_enabled and normalized_resume != "none" else mode
    try:
        run_id = db.create_run(
            trigger=trigger or "cli",
            mode=mode_for_db,
            csv_version_id=csv_version_id,
            params_json=params_json,
        )
    except Exception as exc:  # noqa: BLE001
        log_line(f"[DB][WARN] Unable to create run record: {exc}")

    def _attempt() -> Dict[str, Any]:
        nonlocal next_start_message
        result = _run_scrape_attempt(
            base_url=base_url,
            page_wait=page_wait,
            per_delay=per_delay,
            start_message=next_start_message,
            scrape_mode=mode,
            new_limit=row_limit,
            log_path=log_path,
            max_retries=0,
            checkpoint=checkpoint,
            resume_enabled=resume_enabled,
            resume_state=resume_state,
            limit_pages=limit_pages,
            row_limit_override=row_limit,
            run_id=run_id,
            csv_source=csv_source,
        )
        next_start_message = None
        return result

    try:
        result = run_with_retries(_attempt, max_retries=max(1, retry_limit))
        if run_id is not None:
            try:
                db.mark_run_completed(run_id)
            except Exception as exc:  # noqa: BLE001
                log_line(f"[DB][WARN] Unable to mark run completed: {exc}")
        return result
    except Exception as exc:  # noqa: BLE001
        if run_id is not None:
            try:
                db.mark_run_failed(run_id, _short_error_message(exc))
            except Exception as mark_exc:  # noqa: BLE001
                log_line(f"[DB][WARN] Unable to mark run failed: {mark_exc}")
        raise


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description="Run the Playwright scraper")
    parser.add_argument("--base-url", default=config.DEFAULT_BASE_URL)
    parser.add_argument("--page-wait", type=int, default=config.PAGE_WAIT_SECONDS)
    parser.add_argument("--per-download-delay", type=float, default=config.PER_DOWNLOAD_DELAY)
    parser.add_argument("--scrape-mode", choices=["new", "full"], default=config.SCRAPE_MODE_DEFAULT)
    parser.add_argument("--new-limit", type=int, default=config.SCRAPE_NEW_LIMIT)
    parser.add_argument("--max-retries", type=int, default=config.SCRAPER_MAX_RETRIES)
    parser.add_argument(
        "--resume",
        choices=["none", "auto", "state", "logs"],
        default="none",
        help="Resume strategy",
    )
    parser.add_argument("--resume-page", type=int, default=None)
    parser.add_argument("--resume-index", type=int, default=None)
    parser.add_argument("--limit-pages", type=int, nargs="*", default=None)
    parser.add_argument("--row-limit", type=int, default=None)

    args = parser.parse_args()

    ensure_dirs()
    run_scrape(
        base_url=args.base_url,
        page_wait=args.page_wait,
        per_delay=args.per_download_delay,
        scrape_mode=args.scrape_mode,
        new_limit=args.new_limit,
        max_retries=args.max_retries,
        resume=args.resume != "none",
        resume_mode=args.resume,
        resume_page=args.resume_page,
        resume_index=args.resume_index,
        limit_pages=args.limit_pages,
        row_limit=args.row_limit,
    )

__all__ = ["run_scrape"]

