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
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

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

from . import box_client, config, csv_sync, db, db_reporting, sources
from . import selectors_public_registers
from .run_creation import create_run_with_source
from .download_executor import DownloadExecutor
from .config import is_full_mode, is_new_mode
from .cases_index import (
    CASES_BY_ACTION,
    CASES_ALL,
    CaseRow,
    find_case_by_fname,
    load_cases_from_csv as load_cases_index,
    normalize_action_token,
)
from .csv_sync import normalize_action_token as normalize_action_token_db
from .download_state import CaseDownloadState
from .error_codes import ErrorCode
from .retry_policy import decide_retry
from .selectors_public_registers import PublicRegistersSelectors
from .logging_utils import _scraper_event
from .state import clear_checkpoint, derive_checkpoint_from_logs, load_checkpoint, save_checkpoint
from .telemetry import RunTelemetry

# Minimal header for stub PDFs created when REPLAY_SKIP_NETWORK is enabled.
REPLAY_STUB_PDF_HEADER = b"%PDF-1.4\n"
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
from . import worklist
from .config_validation import validate_runtime_config


def _should_apply_worklist_filter(scrape_mode: str) -> bool:
    """Return True when DB-backed worklist filtering should be enforced.

    Filtering is activated only when the corresponding DB worklist flags are
    enabled; legacy CSV/JSON paths remain untouched when the flags are off.
    """

    return (
        (is_new_mode(scrape_mode) and config.use_db_worklist_for_new())
        or (is_full_mode(scrape_mode) and config.use_db_worklist_for_full())
        or (scrape_mode.strip().lower() == "resume" and config.use_db_worklist_for_resume())
    )


def _normalize_scrape_mode(raw: str) -> str:
    raw = (raw or "").strip().lower()
    if is_full_mode(raw):
        return "full"
    if is_new_mode(raw):
        return "new"
    if raw == "resume":
        return "resume"
    log_line(f"[RUN] Unknown scrape_mode={raw!r}; defaulting to 'new'.")
    return "new"


def resolve_ajax_case_context(
    fname_param: Optional[str],
    fid_param: Optional[str],
    pending_by_fname: Dict[str, Any],
    target_source: str,
) -> tuple[Optional[Dict[str, Any]], str, str, str]:
    """Resolve case context and tokens for an AJAX download callback."""

    source_norm = sources.normalize_source(target_source)
    norm_fname = normalize_action_token(fname_param or "")
    case_context = pending_by_fname.get(norm_fname) if norm_fname else None
    if case_context is not None:
        case_context = dict(case_context)
        if fid_param:
            case_context.setdefault("fid", fid_param)
    elif norm_fname:
        fallback_case = find_case_by_fname(
            fname_param or norm_fname, source=source_norm
        )
        if fallback_case is not None:
            case_context = {
                "case": fallback_case,
                "slug": fallback_case.action,
                "raw": fname_param or norm_fname,
            }

    canonical_token = canon_fname(fname_param or norm_fname or "")
    db_token_norm = normalize_action_token_db(
        (case_context or {}).get("slug")
        or fname_param
        or norm_fname
        or ""
    )
    return case_context, canonical_token, db_token_norm, norm_fname

ADMIN_AJAX = "https://judicial.ky/wp-admin/admin-ajax.php"

_ONCLICK_FNAME_RE = re.compile(r"dl_bfile[^'\"]*['\"]([A-Za-z0-9]+)['\"]", re.IGNORECASE)
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


RESTART_BACKOFF_SECONDS = [5, 15, 45]


@dataclass(frozen=True)
class SourceSelectors:
    """Source-specific selectors for Playwright interactions."""

    table_selector: str
    row_selector: str
    download_locator: str
    token_attributes: tuple[str, ...]
    row_token_attributes: tuple[str, ...] = ()
    href_attributes: tuple[str, ...] = ("href",)


def _selectors_for_source(source: str) -> SourceSelectors:
    normalized = sources.normalize_source(source)
    if normalized == sources.PUBLIC_REGISTERS:
        pr = selectors_public_registers.PUBLIC_REGISTERS_SELECTORS
        return SourceSelectors(
            table_selector=pr.table_selector,
            row_selector=pr.row_selector,
            download_locator=pr.download_locator,
            token_attributes=pr.token_attributes,
            row_token_attributes=pr.row_token_attributes,
            href_attributes=pr.href_attributes,
        )
    return SourceSelectors(
        table_selector="#judgment-registers",
        row_selector="#judgment-registers tbody tr",
        download_locator="button[data-dl], button:has(i.icon-dl), a:has(i.icon-dl)",
        token_attributes=("data-dl", "data-fname", "data-filename", "data-target"),
        row_token_attributes=("data-dl", "data-fname"),
        href_attributes=("data-dl", "data-url", "href"),
    )


def _now_iso() -> str:
    """Return the current UTC time formatted for DB logging."""

    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _short_error_message(exc: Exception, max_length: int = 200) -> str:
    """Return a truncated string representation of ``exc`` for DB logging."""

    message = str(exc)
    if len(message) > max_length:
        return message[: max_length - 3] + "..."
    return message


def _work_item_to_case_row(item: worklist.WorkItem) -> CaseRow:
    token = item.action_token_norm or normalize_action_token(item.action_token_raw)
    return CaseRow(
        action=token,
        code=token,
        suffix="",
        title=item.title,
        subject=item.title,
        court=item.court,
        category=item.category,
        judgment_date=item.judgment_date,
        cause_number=item.cause_number,
        extra={"case_id": item.case_id, "source": item.source},
    )


def _adapt_work_items_to_case_rows(items: Iterable[worklist.WorkItem]) -> List[CaseRow]:
    return [_work_item_to_case_row(item) for item in items]


def _legacy_plan_cases_for_new_mode(
    sync_result: csv_sync.CsvSyncResult, *, source: str
) -> List[CaseRow]:
    _ = sync_result, source
    return list(CASES_ALL)


def _legacy_plan_cases_for_full_mode(
    sync_result: csv_sync.CsvSyncResult, *, source: str
) -> List[CaseRow]:
    _ = sync_result, source
    return list(CASES_ALL)


def _plan_cases_for_new_mode(
    sync_result: csv_sync.CsvSyncResult, *, source: str
) -> List[CaseRow]:
    if config.use_db_worklist_for_new():
        db_items = worklist.build_new_worklist(sync_result.version_id, source=source)
        return _adapt_work_items_to_case_rows(db_items)
    return _legacy_plan_cases_for_new_mode(sync_result, source=source)


def _plan_cases_for_full_mode(
    sync_result: csv_sync.CsvSyncResult, *, source: str
) -> List[CaseRow]:
    if config.use_db_worklist_for_full():
        db_items = worklist.build_full_worklist(sync_result.version_id, source=source)
        return _adapt_work_items_to_case_rows(db_items)
    return _legacy_plan_cases_for_full_mode(sync_result, source=source)


def _plan_cases_for_resume_mode(
    sync_result: csv_sync.CsvSyncResult, *, source: str
) -> List[CaseRow]:
    if config.use_db_worklist_for_resume():
        db_items = worklist.build_resume_worklist(sync_result.version_id, source=source)
        return _adapt_work_items_to_case_rows(db_items)
    return []


def _prepare_planned_cases(
    scrape_mode: str,
    sync_result: Optional[csv_sync.CsvSyncResult],
    *,
    source: str,
) -> tuple[Dict[str, CaseRow], Dict[str, int]]:
    if sync_result is None:
        return {}, {}

    planned: List[CaseRow] = []
    if is_new_mode(scrape_mode):
        planned = _plan_cases_for_new_mode(sync_result, source=source)
    elif is_full_mode(scrape_mode):
        planned = _plan_cases_for_full_mode(sync_result, source=source)
    elif scrape_mode.strip().lower() == "resume":
        planned = _plan_cases_for_resume_mode(sync_result, source=source)

    planned_by_token: Dict[str, CaseRow] = {}
    planned_case_ids: Dict[str, int] = {}
    for case in planned:
        token = normalize_action_token(getattr(case, "action", ""))
        if not token:
            continue
        planned_by_token[token] = case
        extra = getattr(case, "extra", None)
        if isinstance(extra, dict) and "case_id" in extra:
            try:
                planned_case_ids[token] = int(extra.get("case_id"))
            except Exception:
                continue

    return planned_by_token, planned_case_ids


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
        page.wait_for_load_state(
            "networkidle", timeout=config.PLAYWRIGHT_NAV_TIMEOUT_SECONDS * 1000
        )
    except PWTimeout:
        log_line("Initial networkidle timeout; continuing.")

    last_height = 0
    for i in range(max_scrolls):
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            wait_seconds(page, 0.5)
            page.wait_for_load_state(
                "networkidle", timeout=config.PLAYWRIGHT_NAV_TIMEOUT_SECONDS * 1000
            )
            height = page.evaluate("document.body.scrollHeight")
        except PWError:
            break

        if not isinstance(height, (int, float)):
            break

        if int(height) == int(last_height):
            break

        last_height = int(height)
        log_line(f"Scroll {i+1}: document height now {last_height}")


def _set_datatable_page(page: Page, page_index: int, *, selectors: SourceSelectors) -> bool:
    """Attempt to switch the DataTable to *page_index* (0-based)."""

    if page_index <= 0:
        return True

    try:
        switched = page.evaluate(
            f"""
            (idx) => {{
                const $ = window.jQuery;
                if (!$ || !$.fn || !$.fn.dataTable) {{
                    return false;
                }}
                const table = $('{selectors.table_selector}').DataTable();
                if (!table) {{
                    return false;
                }}
                const info = table.page.info();
                if (!info || idx < 0 || idx >= info.pages) {{
                    return false;
                }}
                table.page(idx).draw('page');
                return true;
            }}
            """,
            page_index,
        )
        return bool(switched)
    except Exception as exc:  # noqa: BLE001
        log_line(f"[RUN] Failed to set DataTable page via JS: {exc}")
        return False


def _refresh_datatable_page(page: Page, page_index: int, *, selectors: SourceSelectors) -> None:
    """Force the current DataTable page to redraw."""

    try:
        page.evaluate(
            f"""
            (idx) => {{
                const $ = window.jQuery;
                if (!$ || !$.fn || !$.fn.dataTable) {{
                    return false;
                }}
                const table = $('{selectors.table_selector}').DataTable();
                if (!table) {{
                    return false;
                }}
                table.page(idx).draw(false);
                return true;
            }}
            """,
            page_index,
        )
    except Exception:
        # Silently ignore; best effort.
        pass


def _safe_goto(page: Page, url: str, *, label: str, wait_until: str = "networkidle") -> bool:
    """Navigate to ``url`` with bounded timeouts and structured logging."""

    try:
        _scraper_event("nav", step="goto", label=label, url=url)
        page.goto(
            url,
            wait_until=wait_until,
            timeout=config.PLAYWRIGHT_NAV_TIMEOUT_SECONDS * 1000,
        )
        page.wait_for_load_state(
            "networkidle", timeout=config.PLAYWRIGHT_NAV_TIMEOUT_SECONDS * 1000
        )
        return True
    except PWTimeout as exc:
        log_line(f"[SCRAPER][ERROR][NAV] goto({url!r}) timed out: {exc}")
        _scraper_event(
            "error",
            phase="nav",
            step="goto_timeout",
            label=label,
            url=url,
            error=str(exc),
        )
        return False
    except PWError as exc:
        if _is_target_closed_error(exc):
            log_line(f"[SCRAPER][ERROR][NAV] Target closed during navigation to {label}: {exc}")
            _scraper_event(
                "error",
                phase="nav",
                step="goto_target_closed",
                label=label,
                url=url,
                error=str(exc),
            )
            return False
        log_line(f"[SCRAPER][ERROR][NAV] goto({url!r}) failed: {exc}")
        _scraper_event(
            "error",
            phase="nav",
            step="goto_error",
            label=label,
            url=url,
            error=str(exc),
        )
        return False
    except Exception as exc:  # noqa: BLE001
        log_line(f"[SCRAPER][ERROR][NAV] goto({url!r}) raised: {exc}")
        _scraper_event(
            "error",
            phase="nav",
            step="goto_exception",
            label=label,
            url=url,
            error=str(exc),
        )
        return False


def _wait_for_datatable_ready(
    page: Page, *, page_index: int, phase: str, selectors: SourceSelectors
) -> bool:
    """Wait for the main DataTable to be present with a bounded timeout."""

    selector = selectors.table_selector or "table.dataTable"
    try:
        _scraper_event(
            "table",
            phase=phase,
            page_index=page_index,
            step="wait_for_table",
        )
        page.wait_for_selector(
            selector, timeout=config.PLAYWRIGHT_SELECTOR_TIMEOUT_SECONDS * 1000
        )
        return True
    except PWTimeout as exc:
        log_line(
            f"[SCRAPER][ERROR][TABLE] DataTable selector timeout on page index {page_index}: {exc}"
        )
        _scraper_event(
            "error",
            phase="table",
            step="wait_for_table_timeout",
            page_index=page_index,
            error=str(exc),
        )
        return False
    except PWError as exc:
        if _is_target_closed_error(exc):
            log_line(
                f"[SCRAPER][ERROR][TABLE] Target closed while waiting for DataTable on page index {page_index}: {exc}"
            )
            _scraper_event(
                "error",
                phase="table",
                step="wait_for_table_target_closed",
                page_index=page_index,
                error=str(exc),
            )
            return False
        log_line(
            f"[SCRAPER][ERROR][TABLE] DataTable selector error on page index {page_index}: {exc}"
        )
        _scraper_event(
            "error",
            phase="table",
            step="wait_for_table_error",
            page_index=page_index,
            error=str(exc),
        )
        return False
    except Exception as exc:  # noqa: BLE001
        log_line(
            f"[SCRAPER][ERROR][TABLE] DataTable wait failed on page index {page_index}: {exc}"
        )
        _scraper_event(
            "error",
            phase="table",
            step="wait_for_table_exception",
            page_index=page_index,
            error=str(exc),
        )
        return False


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


def _goto_datatable_page(
    page: Page, page_index: int, *, selectors: SourceSelectors
) -> bool:
    """Click the DataTable pagination button for ``page_index``."""

    if page_index <= 0:
        return True

    try:
        page.evaluate(
            f"""
            (idx) => {{
              const dt = window.jQuery && window.jQuery.fn && window.jQuery.fn.dataTable ?
                         window.jQuery('{selectors.table_selector}').DataTable() : null;
              if (dt) {{ dt.page(idx).draw('page'); }}
            }}
            """,
            page_index,
        )
        page.wait_for_selector(
            f"li.dt-paging-button.active:has(button[data-dt-idx=\"{page_index}\"])",
            timeout=config.PLAYWRIGHT_SELECTOR_TIMEOUT_SECONDS * 1000,
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


def _first_locator(parent, selector: str):
    try:
        loc = parent.locator(selector)
        return loc.nth(0) if loc.count() else None
    except PWError as exc:
        if _is_target_closed_error(exc):
            raise
        log_line(f"[RUN][WARN] Locator error for {selector!r}: {exc}")
        return None
    except Exception as exc:  # noqa: BLE001
        log_line(f"[RUN][WARN] Locator error for {selector!r}: {exc}")
        return None


def _extract_attr(locator, names: Iterable[str]) -> Optional[str]:
    for name in names:
        try:
            raw_value = locator.get_attribute(name)
        except PWError as exc:
            if _is_target_closed_error(exc):
                raise
            raw_value = None
        except Exception:
            raw_value = None
        if raw_value and str(raw_value).strip():
            return str(raw_value).strip()
    return None


def _locate_download_element(row, selectors: SourceSelectors):
    if selectors.download_locator:
        loc = _first_locator(row, selectors.download_locator)
        if loc is not None:
            return loc

    for selector in _guess_download_locators():
        loc = _first_locator(row, selector)
        if loc is not None:
            return loc
    return None


def _extract_token_from_locator(locator, selectors: SourceSelectors) -> Optional[str]:
    token = _extract_attr(locator, selectors.token_attributes)
    if token:
        return token

    try:
        onclick_raw = locator.get_attribute("onclick") or ""
    except Exception:
        onclick_raw = ""
    if onclick_raw:
        match = _ONCLICK_FNAME_RE.search(onclick_raw)
        if match:
            return match.group(1).strip()

    try:
        text_content = locator.text_content() or ""
    except Exception:
        text_content = ""
    cleaned = (text_content or "").strip()
    return cleaned or None


def _extract_token_from_row(row, selectors: SourceSelectors) -> Optional[str]:
    token = _extract_attr(row, selectors.row_token_attributes)
    if token:
        return token
    return None


def _extract_download_url(locator, selectors: SourceSelectors, row=None) -> Optional[str]:
    url = _extract_attr(locator, selectors.href_attributes)
    if url:
        return url
    if row is not None:
        url = _extract_attr(row, selectors.href_attributes)
        if url:
            return url
    return None


def _normalize_url(raw: str, *, page_url: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    if lowered.startswith("javascript:") or raw == "#":
        return ""
    if raw.startswith("//"):
        return "https:" + raw
    if raw.startswith("/"):
        try:
            return urllib.parse.urljoin(page_url, raw)
        except Exception:
            return raw
    return raw


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
    timeout: int = config.PLAYWRIGHT_DOWNLOAD_TIMEOUT_SECONDS,
    token: Optional[str] = None,
) -> tuple[bool, Optional[dict[str, Any]]]:
    """Download ``url`` to ``dest_path`` with retries and validation."""

    if config.REPLAY_SKIP_NETWORK:
        log_line(f"[REPLAY] Skipping network download for {token or url}")
        _scraper_event(
            "replay",
            phase="download_stub",
            token=token or url,
        )
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        padded = REPLAY_STUB_PDF_HEADER + (b"0" * max(0, box_client.MIN_PDF_BYTES - len(REPLAY_STUB_PDF_HEADER)))
        dest_path.write_bytes(padded)
        return True, {"file_path": str(dest_path)}

    try:
        result = box_client.download_pdf(
            url,
            dest_path,
            http_client=http_client,
            max_retries=max_retries,
            timeout=timeout,
            token=token,
        )
        if result.ok:
            return True, {"status_code": result.status_code}
        return False, {"error_code": ErrorCode.INTERNAL, "error_message": "download_failed"}
    except box_client.DownloadError as exc:
        return False, {
            "error_code": exc.error_code,
            "error_message": str(exc),
            "http_status": getattr(exc, "http_status", None),
        }
    except Exception as exc:  # noqa: BLE001
        return False, {"error_code": ErrorCode.INTERNAL, "error_message": str(exc)}


def _log_download_executor_summary(executor: Optional[DownloadExecutor]) -> None:
    """Emit a summary event for the download executor, if present."""
    if executor is None:
        return

    _scraper_event(
        "state",
        phase="download_executor",
        kind="summary",
        peak_in_flight=executor.peak_in_flight,
        max_parallel=config.MAX_PARALLEL_DOWNLOADS,
    )


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
    run_id: Optional[int] = None,
    download_executor: Optional[DownloadExecutor] = None,
    source: Optional[str] = None,
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
        "error_code": None,
    }

    def _return_result(result: str, details: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        try:
            raw_size = details.get("file_size_bytes") if isinstance(details, dict) else None
            size_bytes = int(raw_size) if isinstance(raw_size, (int, float)) else 0
        except Exception:
            size_bytes = 0
        _scraper_event(
            "box",
            token=norm_fname,
            result=result,
            run_id=run_id,
            size_bytes=size_bytes,
            error_message=details.get("error_message") if isinstance(details, dict) else None,
            error_code=details.get("error_code") if isinstance(details, dict) else None,
        )
        return result, details
    if not norm_fname:
        log_line(
            f"[AJAX][WARN] Unable to normalise fname token '{display_name}'; skipping."
        )
        return _return_result("failed", {**download_details, "error_message": "invalid_token"})

    if processed_this_run is not None and canonical_token:
        if canonical_token in processed_this_run:
            log_line(
                f"[AJAX] fname {display_name} already processed earlier in this run; ignoring duplicate response."
            )
            return _return_result("duplicate_in_run", {**download_details, "slug": norm_fname})

    case_row = case_context.get("case") if case_context else None
    if case_row is None:
        case_row = find_case_by_fname(fname, source=source) or cases_by_action.get(
            norm_fname
        )

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
        return _return_result(
            "existing_file",
            {**download_details, "file_path": str(final_path.name), "file_size_bytes": size_bytes},
        )

    if is_new_mode(mode) and checkpoint is not None:
        processed_tokens = checkpoint.processed_tokens
        if norm_fname in processed_tokens:
            log_line(
                f"[AJAX] {display_name} previously completed; skip in NEW mode."
            )
            return _return_result("checkpoint_skip", download_details)

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
        # Do not assume the pretty pdf_path exists; metadata may point to a hashed
        # fallback filename, so avoid stat() here to prevent spurious failures.
        return _return_result(
            "existing_file",
            {
                **download_details,
                "file_path": str(pdf_path.name),
            },
        )

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
            return _return_result(
                "existing_file",
                {**download_details, "file_path": str(pdf_path.name), "file_size_bytes": pdf_path.stat().st_size},
            )
    except OSError:
        pass

    if not disk_has_room(config.MIN_FREE_MB, downloads_dir):
        log_line(
            f"[AJAX][STOP] Insufficient disk space (<{config.MIN_FREE_MB} MB free); aborting before download."
        )
        return _return_result("disk_full", {**download_details, "error_message": "disk_full"})

    success = False
    error_info: Optional[dict[str, Any]] = None
    download_path = pdf_path

    def _download() -> Tuple[bool, Optional[dict[str, Any]]]:
        return queue_or_download_file(
            box_url,
            download_path,
            http_client=http_client,
            max_retries=config.DOWNLOAD_RETRIES,
            timeout=config.PLAYWRIGHT_DOWNLOAD_TIMEOUT_SECONDS,
            token=norm_fname,
        )

    def _execute_download() -> Tuple[bool, Optional[dict[str, Any]]]:
        try:
            if download_executor is not None:
                return download_executor.submit(norm_fname, _download)
            return _download()
        except OSError as exc:  # noqa: PERF203
            return False, {"error_code": ErrorCode.INTERNAL, "error_message": str(exc)}

    success, error_info = _execute_download()

    error_message: Optional[str] = None
    error_code: Optional[str] = None
    if error_info:
        error_message = (
            error_info.get("error_message") if isinstance(error_info, dict) else str(error_info)
        )
        error_code = error_info.get("error_code") if isinstance(error_info, dict) else None

    if not success and error_message:
        lowered = error_message.lower()
        if "file name too long" in lowered or "errno 36" in lowered or "errno 63" in lowered:
            fallback_path = hashed_fallback_path(downloads_dir, subject_label)
            if fallback_path != pdf_path:
                log_line(
                    f"[AJAX] Retrying save for {display_name} with fallback {fallback_path.name}"
                )
                final_path = fallback_path
                download_path = fallback_path
                success, error_info = _execute_download()
                if error_info:
                    error_message = error_info.get("error_message")
                    error_code = error_info.get("error_code")

    if not success:
        log_line(
            f"[AJAX] Download failed for {display_name} -> {final_path.name}: {error_message}"
        )
        final_path.unlink(missing_ok=True)
        return _return_result(
            "failed",
            {
                **download_details,
                "error_message": error_message or "unknown",
                "error_code": error_code or ErrorCode.INTERNAL,
            },
        )

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

    return _return_result(
        "downloaded",
        {
            **download_details,
            "file_path": saved_path_value,
            "file_size_bytes": size_bytes,
            "box_url": box_url,
        },
    )


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
    selectors: SourceSelectors,
    run_id: Optional[int] = None,
    download_executor: Optional[DownloadExecutor] = None,
) -> None:
    """Retry download clicks for items that previously failed."""

    if page is None or page.is_closed():
        return

    if not failed_items:
        return

    log_line(f"[RETRY] Attempting {len(failed_items)} failed download retries.")

    clicked = 0
    skipped = 0

    for item in list(failed_items):
        if page.is_closed():
            log_line("[RETRY] Page closed before retry could complete; aborting remaining retries.")
            break

        fname = item.get("fname")
        if not fname:
            continue

        error_code = item.get("error_code") or ErrorCode.INTERNAL
        http_status = item.get("http_status") if isinstance(item, dict) else None
        attempt = int(item.get("attempt") or 1)

        case_id = item.get("case_id")
        if run_id is not None and isinstance(case_id, int):
            try:
                state = CaseDownloadState.load(run_id=run_id, case_id=case_id)
                attempt = state.attempt_count
            except Exception as exc:  # noqa: BLE001
                log_line(
                    f"[RETRY] Unable to load download state for run_id={run_id}, case_id={case_id}: {exc}"
                )

        _scraper_event(
            "decision",
            token=fname,
            decision="consider_retry",
            error_code=error_code,
            attempt=attempt,
            run_id=run_id,
            case_id=case_id,
        )

        if not decide_retry(
            attempt_index=attempt,
            max_attempts=config.DOWNLOAD_RETRIES,
            error_code=error_code,
            http_status=http_status,
        ):
            _scraper_event(
                "decision",
                token=fname,
                decision="skip_retry",
                reason=error_code,
                attempt=attempt,
                run_id=run_id,
                case_id=case_id,
                http_status=http_status,
            )
            skipped += 1
            continue

        _scraper_event(
            "decision",
            token=fname,
            decision="retry_click",
            reason=error_code,
            attempt=attempt,
            run_id=run_id,
            case_id=case_id,
            http_status=http_status,
        )
        clicked += 1

        page_index = int(item.get("page_index", 0))
        button_index = int(item.get("button_index", 0))

        try:
            if not _set_datatable_page(page, page_index, selectors=selectors):
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

        if not _wait_for_datatable_ready(
            page, page_index=page_index, phase="retry_navigation", selectors=selectors
        ):
            log_line(
                f"[RETRY] DataTable not ready after navigation to page {page_index + 1} for {fname}; skipping."
            )
            continue

        wait_seconds(page, config.PLAYWRIGHT_RETRY_PAGE_SETTLE_SECONDS)

        locator = None
        try:
            rows = page.locator(selectors.row_selector)
            if rows.count() > button_index:
                row = rows.nth(button_index)
                locator = _locate_download_element(row, selectors)
        except PWError as exc:
            if _is_target_closed_error(exc):
                log_line("[RETRY] Target closed while locating button; aborting retries.")
                locator = None
            else:
                log_line(f"[RETRY] Locator error for selector {selectors.row_selector!r}: {exc}")
        except Exception as exc:  # noqa: BLE001
            log_line(f"[RETRY] Unexpected locator error for selector {selectors.row_selector!r}: {exc}")

        if locator is None:
            for selector in (selectors.download_locator, *_guess_download_locators()):
                if not selector:
                    continue
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
            locator.click(timeout=config.PLAYWRIGHT_CLICK_TIMEOUT_MS)
            processed_this_run.discard(canon_fname(fname))
            wait_seconds(page, config.PLAYWRIGHT_POST_CLICK_SLEEP_SECONDS)
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

    log_line(
        f"[RETRY] Completed retries: clicked={clicked}, skipped={skipped}, total_failed_items={len(failed_items)}."
    )

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
            _scraper_event(
                "error",
                context="run",
                error="playwright_error",
                attempt=attempt,
                max_retries=effective_retries,
            )
            if attempt >= effective_retries:
                log_line("[RUN] Max retries reached; aborting after repeated crashes.")
                _scraper_event(
                    "error",
                    context="run",
                    error="playwright_error_max_retries",
                    attempt=attempt,
                    max_retries=effective_retries,
                )
                raise
            delay = RESTART_BACKOFF_SECONDS[
                min(attempt - 1, len(RESTART_BACKOFF_SECONDS) - 1)
            ]
            log_line(f"[RUN] Retrying in {delay}s after Playwright error...")
            time.sleep(delay)
        except Exception:
            log_line("[RUN] Unexpected non-Playwright error; aborting without retry.")
            _scraper_event("error", context="run", error="unexpected_exception")
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
    sync_result: Optional[csv_sync.CsvSyncResult] = None,
    target_source: str = sources.DEFAULT_SOURCE,
    selectors: Optional[SourceSelectors] = None,
) -> Dict[str, Any]:
    """Execute a scraping run with automatic restart/resume support."""

    selectors = selectors or _selectors_for_source(target_source)

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

    _scraper_event(
        "nav",
        run_id=run_id,
        mode=scrape_mode,
        base_url=base_url,
        csv_source=csv_source or config.CSV_URL,
    )

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
    load_cases_index(
        effective_csv_source,
        source=target_source,
        csv_version_id=(sync_result.version_id if sync_result else None),
    )
    _scraper_event(
        "plan",
        index_backend="db" if cases_index.should_use_db_index() else "csv",
        total_cases=len(CASES_BY_ACTION),
    )
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

    planned_cases_by_token, planned_case_ids = _prepare_planned_cases(
        scrape_mode,
        sync_result,
        source=target_source,
    )
    _scraper_event(
        "plan",
        scrape_mode=scrape_mode,
        planned_cases=len(planned_cases_by_token),
        use_worklist_filter=_should_apply_worklist_filter(scrape_mode),
    )
    allowed_tokens: Optional[Set[str]] = None
    if planned_cases_by_token and _should_apply_worklist_filter(scrape_mode):
        allowed_tokens = set(planned_cases_by_token)

    summary: Dict[str, Any] = {
        "base_url": base_url,
        "processed": 0,
        "downloaded": 0,
        "failed": 0,
        "skipped": 0,
        "inspected_rows": 0,
        "total_cases": len(allowed_tokens) if allowed_tokens is not None else len(CASES_BY_ACTION),
        "log_file": str(log_path),
        "scrape_mode": scrape_mode,
        "skip_reasons": {},
        "fail_reasons": {},
    }

    def _lookup_case_id(token_norm: str) -> Optional[int]:
        if run_id is None or not token_norm:
            return None
        if token_norm in planned_case_ids:
            return planned_case_ids[token_norm]
        try:
            return db.get_case_id_by_token_norm(target_source, token_norm)
        except Exception as exc:  # noqa: BLE001
            log_line(f"[DB][WARN] Failed to resolve case id for token={token_norm}: {exc}")
            return None

    def _log_skip_status(case_id: Optional[int], reason: str) -> None:
        if run_id is None or case_id is None:
            return
        try:
            state = CaseDownloadState.load(run_id=run_id, case_id=case_id)
            state.mark_skipped(reason)
        except Exception as exc:  # noqa: BLE001
            log_line(f"[DB][WARN] Unable to record skip for case_id={case_id}: {exc}")

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

    def _apply_result_to_summary(summary: Dict[str, Any], result: str, *, error_code: Optional[str] = None) -> Optional[str]:
        if result == "downloaded":
            summary["downloaded"] += 1
            return None
        if result == "failed":
            summary["failed"] += 1
            _bump_reason(summary["fail_reasons"], error_code or ErrorCode.INTERNAL)
            return None
        summary["skipped"] += 1
        skip_reason_map = {
            "existing_file": "exists_ok",
            "checkpoint_skip": "seen_history",
            "duplicate_in_run": "in_run_dup",
        }
        reason = skip_reason_map.get(result, result)
        _bump_reason(summary["skip_reasons"], reason)
        return reason

    download_executor = DownloadExecutor(config.MAX_PARALLEL_DOWNLOADS)
    # NOTE: submit() currently blocks; this is a bounded wrapper and telemetry hook
    # for future parallel downloads rather than true concurrent fetching.

    try:
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
                _scraper_event(
                    "state",
                    source="resume_param",
                    resume_mode=scrape_mode,
                    dt_page_index=resume_page_index,
                    row_index=resume_row_index,
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
                _scraper_event(
                    "state",
                    source="checkpoint_state",
                    resume_mode=scrape_mode,
                    dt_page_index=resume_page_index,
                    row_index=resume_row_index,
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
                                    lambda download_url, timeout=config.PLAYWRIGHT_DOWNLOAD_TIMEOUT_SECONDS: context.request.get(
                                        download_url,
                                        timeout=timeout * 1000,
                                    )
                                )
    
                                (
                                    case_context,
                                    canonical_token,
                                    db_token_norm,
                                    norm_fname,
                                ) = resolve_ajax_case_context(
                                    fname_param,
                                    fid_param,
                                    pending_by_fname,
                                    target_source,
                                )
                                case_id = _lookup_case_id(db_token_norm)
                                state: Optional[CaseDownloadState] = None
                                if run_id is not None and case_id is not None:
                                    try:
                                        state = CaseDownloadState.start(
                                            run_id=run_id,
                                            case_id=case_id,
                                            box_url=box_url,
                                        )
                                    except Exception as exc:  # noqa: BLE001
                                        log_line(
                                            f"[DB][WARN] Unable to start download attempt for case_id={case_id}: {exc}"
                                        )
                                elif run_id is not None:
                                    log_line(
                                        f"[MAPPING][WARN] No case_id resolved for fname={db_token_norm!r} source={target_source}"
                                    )
    
                                serialized_case_context: Optional[Dict[str, Any]] = None
                                if case_context:
                                    serialized_case_context = {}
    
                                    def _serialize_value(value: Any) -> Any:
                                        if is_dataclass(value):
                                            return asdict(value)
                                        try:
                                            json.dumps(value)
                                            return value
                                        except TypeError:
                                            return str(value)
    
                                    for key, value in case_context.items():
                                        serialized_case_context[key] = _serialize_value(value)
    
                                if config.RECORD_REPLAY_FIXTURES and run_id is not None:
                                    config.REPLAY_FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
                                    fixtures_path = (
                                        config.REPLAY_FIXTURES_DIR
                                        / f"run_{run_id}_dl_bfile.jsonl"
                                    )
                                    append_json_line(
                                        fixtures_path,
                                        {
                                            "run_id": run_id,
                                            "fname": fname_param,
                                            "canonical_token": canonical_token,
                                            "db_token_norm": db_token_norm,
                                            "box_url": box_url,
                                            "payload": payload,
                                            "case_context": serialized_case_context,
                                            "timestamp": datetime.utcnow().isoformat() + "Z",
                                            "mode": scrape_mode,
                                            "case_id": case_id,
                                            "slug": case_context.get("slug") if case_context else None,
                                        },
                                    )
                                    _scraper_event(
                                        "replay",
                                        phase="capture",
                                        run_id=run_id,
                                        token=norm_fname,
                                    )
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
                                    run_id=run_id,
                                    download_executor=download_executor,
                                    source=target_source,
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
                                    download_info_dict = download_info if isinstance(download_info, dict) else {}
                                    error_code_for_retry = (
                                        download_info_dict.get("error_code") or ErrorCode.INTERNAL
                                    )
                                    _bump_reason(summary["fail_reasons"], error_code_for_retry)
                                    attempt_for_retry = state.attempt_count if state is not None else 1
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
                                                    "case_id": case_id,
                                                    "error_code": error_code_for_retry,
                                                    "http_status": download_info_dict.get("http_status"),
                                                    "attempt": attempt_for_retry,
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
    
                                error_message = download_info.get("error_message") if isinstance(download_info, dict) else None
                                error_code = download_info.get("error_code") if isinstance(download_info, dict) else None
                                file_path = download_info.get("file_path") if isinstance(download_info, dict) else None
                                file_size_bytes = download_info.get("file_size_bytes") if isinstance(download_info, dict) else None
                                box_url_last = download_info.get("box_url") if isinstance(download_info, dict) else box_url
    
                                if state is not None:
                                    try:
                                        if result == "downloaded":
                                            state.mark_downloaded(
                                                file_path=file_path,
                                                file_size_bytes=file_size_bytes,
                                                box_url=box_url_last,
                                            )
                                        elif result in {"existing_file", "checkpoint_skip", "duplicate_in_run"}:
                                            state.mark_skipped(reason=result)
                                        elif result == "disk_full":
                                            state.mark_failed(
                                                error_code="disk_full",
                                                error_message=error_message,
                                            )
                                        elif result == "failed":
                                            state.mark_failed(
                                                error_code=error_code or ErrorCode.INTERNAL,
                                                error_message=error_message,
                                            )
                                    except Exception as exc:  # noqa: BLE001
                                        log_line(
                                            f"[DB][WARN] Unable to update download status for case_id={case_id}: {exc}"
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
                                    _scraper_event(
                                        "table",
                                        reason="consecutive_existing_limit",
                                        mode="new",
                                        consecutive_existing=consecutive_existing,
                                        limit=config.SCRAPE_NEW_CONSECUTIVE_LIMIT,
                                    )
                                    row_limit_reached = True
                                    active = False
    
                            except Exception as exc:
                                log_line(f"[AJAX] handler error: {exc}")
    
                        context.on("response", on_response)
    
                        log_line("Opening judgments page in Playwright...")
                        if not _safe_goto(
                            page, base_url, label=f"{target_source}_root"
                        ):
                            crash_stop = True
                            active = False
                        else:
                            if page_wait:
                                wait_seconds(page, float(page_wait))
    
                            _accept_cookies(page)
                            if not _wait_for_datatable_ready(
                                page,
                                page_index=resume_page_index,
                                phase="initial_nav",
                                selectors=selectors,
                            ):
                                crash_stop = True
                                active = False
                            else:
                                _load_all_results(page)
                                _screenshot(page)
    
                        total_clicks = summary.get("processed", 0)
                        row_limit_reached = False
                        rows_evaluated = summary.get("inspected_rows", 0)
                        resume_consumed = False
                        if crash_stop:
                            log_line("[RUN] Aborting before pagination due to navigation failure.")
                        else:
                            try:
                                page.wait_for_selector(
                                    "button:has(i.icon-dl)",
                                    timeout=config.PLAYWRIGHT_SELECTOR_TIMEOUT_SECONDS * 1000,
                                )
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
                                    _scraper_event("table", total_pages=total_pages)
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
                                    if not _goto_datatable_page(
                                        page, page_index_zero, selectors=selectors
                                    ):
                                        log_line(
                                            f"[RUN] Unable to navigate to DataTable page {page_number}; stopping pagination loop."
                                        )
                                        break
    
                                    if not _wait_for_datatable_ready(
                                        page,
                                        page_index=page_index_zero,
                                        phase="pagination_nav",
                                        selectors=selectors,
                                    ):
                                        log_line(
                                            f"[RUN] DataTable not ready after navigating to page {page_number}; stopping pagination loop."
                                        )
                                        break
    
                                    if checkpoint is not None:
                                        checkpoint.mark_page(page_index_zero, reset_row=False, mode=scrape_mode)
                                    _persist_state(page_index_zero, -1)
    
                                try:
                                    row_locator = page.locator(selectors.row_selector)
                                    count = row_locator.count()
                                    _scraper_event(
                                        "table",
                                        page_index=page_index_zero,
                                        page_number=page_number,
                                        rows_found=count,
                                        source=target_source,
                                    )
                                except PWError as exc:
                                    if _is_target_closed_error(exc):
                                        log_line(
                                            "[RUN] Browser target crashed while enumerating rows; stopping scrape gracefully."
                                        )
                                        crash_stop = True
                                        active = False
                                        break
                                    log_line(f"Failed to enumerate rows: {exc}")
                                    break
                                except Exception as exc:
                                    log_line(f"Failed to enumerate rows: {exc}")
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
                                        _scraper_event(
                                            "table",
                                            reason="row_limit_reached",
                                            mode=scrape_mode,
                                            inspected_rows=rows_evaluated,
                                            row_limit=row_limit,
                                        )
                                        row_limit_reached = True
                                        break
    
                                    rows_evaluated += 1
                                    summary["inspected_rows"] = rows_evaluated
    
                                    try:
                                        row = row_locator.nth(i)
                                    except PWError as exc:
                                        if _is_target_closed_error(exc):
                                            log_line(
                                                "[RUN] Browser target crashed while accessing a row; stopping scrape gracefully."
                                            )
                                            crash_stop = True
                                            active = False
                                            break
                                        log_line(f"Failed to access row index {i}: {exc}")
                                        continue
                                    except Exception as exc:
                                        log_line(f"Failed to access row index {i}: {exc}")
                                        continue

                                    el = _locate_download_element(row, selectors)
                                    if el is None:
                                        log_line(
                                            f"[RUN][WARN] No download element found in row {i} on page {page_number}; skipping."
                                        )
                                        if checkpoint is not None:
                                            checkpoint.mark_position(page_index_zero, i, mode=scrape_mode)
                                        summary["skipped"] += 1
                                        _bump_reason(summary["skip_reasons"], "missing_download")
                                        continue

                                    fname_token: Optional[str] = None
                                    fname_token = _extract_token_from_locator(el, selectors)
                                    if not fname_token:
                                        fname_token = _extract_token_from_row(row, selectors)
    
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
                                        _scraper_event(
                                            "decision",
                                            token=fname_key,
                                            raw_token=fname_token,
                                            decision="skip",
                                            reason="invalid_token",
                                            case_id=case_id_for_logging,
                                            run_id=run_id,
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
                                        _scraper_event(
                                            "decision",
                                            token=fname_key,
                                            raw_token=fname_token,
                                            decision="skip",
                                            reason="in_run_dup",
                                            case_id=case_id_for_logging,
                                            run_id=run_id,
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
                                        _scraper_event(
                                            "decision",
                                            token=fname_key,
                                            raw_token=fname_token,
                                            decision="skip",
                                            reason="in_run_dup",
                                            case_id=case_id_for_logging,
                                            run_id=run_id,
                                        )
                                        _bump_reason(summary["skip_reasons"], "in_run_dup")
                                        _log_skip_status(case_id_for_logging, "in_run_dup")
                                        continue
    
                                    if allowed_tokens is not None and fname_key not in allowed_tokens:
                                        log_line(
                                            f"[SKIP] fname={fname_token} not in planned worklist; skipping click."
                                        )
                                        _scraper_event(
                                            "decision",
                                            token=fname_key,
                                            raw_token=fname_token,
                                            decision="skip",
                                            reason="worklist_filtered",
                                            case_id=case_id_for_logging,
                                            run_id=run_id,
                                        )
                                        if checkpoint is not None:
                                            checkpoint.mark_position(page_index_zero, i, mode=scrape_mode)
                                        summary["skipped"] += 1
                                        _bump_reason(summary["skip_reasons"], "worklist_filtered")
                                        _log_skip_status(case_id_for_logging, "worklist_filtered")
                                        continue
    
                                    case_for_fname = planned_cases_by_token.get(fname_key) or find_case_by_fname(
                                        fname_key, strict=True, source=target_source
                                    )
                                    if case_for_fname is None:
                                        log_line(
                                            f"[SKIP][csv_miss] No CSV entry for fname={fname_token}; skipping."
                                        )
                                        _scraper_event(
                                            "decision",
                                            token=fname_key,
                                            raw_token=fname_token,
                                            decision="skip",
                                            reason="csv_miss",
                                            case_id=case_id_for_logging,
                                            run_id=run_id,
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
                                        _scraper_event(
                                            "decision",
                                            token=fname_key,
                                            raw_token=fname_token,
                                            decision="skip",
                                            reason="already_downloaded",
                                            case_id=case_id_for_logging,
                                            run_id=run_id,
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
                                                _scraper_event(
                                                    "table",
                                                    reason="consecutive_existing_limit",
                                                    mode="new",
                                                    consecutive_existing=consecutive_existing,
                                                    limit=config.SCRAPE_NEW_CONSECUTIVE_LIMIT,
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
                                        _scraper_event(
                                            "decision",
                                            token=fname_key,
                                            raw_token=fname_token,
                                            decision="skip",
                                            reason="seen_history",
                                            case_id=case_id_for_logging,
                                            run_id=run_id,
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
                                                _scraper_event(
                                                    "table",
                                                    reason="consecutive_existing_limit",
                                                    mode="new",
                                                    consecutive_existing=consecutive_existing,
                                                    limit=config.SCRAPE_NEW_CONSECUTIVE_LIMIT,
                                                )
                                                row_limit_reached = True
                                                break
                                        continue

                                    slug_value = case_for_fname.action if case_for_fname else fname_key
                                    raw_url = _extract_download_url(el, selectors, row=row)
                                    box_url = _normalize_url(raw_url, page_url=page.url) if raw_url else None

                                    if target_source == sources.PUBLIC_REGISTERS:
                                        case_context = {
                                            "case": case_for_fname,
                                            "metadata_entry": metadata_entry,
                                            "slug": slug_value,
                                            "raw": fname_token,
                                            "page_index": page_index_zero,
                                            "row_index": i,
                                            "canonical": canonical_token,
                                        }

                                        state: Optional[CaseDownloadState] = None
                                        if run_id is not None and isinstance(case_id_for_logging, int):
                                            try:
                                                state = CaseDownloadState.start(
                                                    run_id=run_id,
                                                    case_id=case_id_for_logging,
                                                    box_url=box_url,
                                                )
                                            except Exception as exc:  # noqa: BLE001
                                                log_line(
                                                    f"[DB][WARN] Unable to start download attempt for case_id={case_id_for_logging}: {exc}"
                                                )

                                        if dedupe_key:
                                            clicked_on_page.add(dedupe_key)

                                        total_clicks += 1
                                        summary["processed"] = total_clicks

                                        if not box_url:
                                            log_line(
                                                f"[RUN][WARN] No download URL found for fname={fname_token}; marking as failed."
                                            )
                                            summary["failed"] += 1
                                            _bump_reason(summary["fail_reasons"], ErrorCode.SITE_STRUCTURE)
                                            if state is not None:
                                                try:
                                                    state.mark_failed(
                                                        error_code=ErrorCode.SITE_STRUCTURE,
                                                        error_message="download_link_missing",
                                                    )
                                                except Exception as exc:  # noqa: BLE001
                                                    log_line(
                                                        f"[DB][WARN] Unable to record missing download URL for case_id={case_id_for_logging}: {exc}"
                                                    )
                                            _persist_state(
                                                page_index_zero,
                                                i,
                                                last_fname=fname_token or fname_key,
                                                last_title=getattr(case_for_fname, "title", None),
                                            )
                                            if checkpoint is not None:
                                                checkpoint.mark_position(page_index_zero, i, mode=scrape_mode)
                                            continue

                                        _scraper_event(
                                            "decision",
                                            token=fname_key,
                                            raw_token=fname_token,
                                            decision="direct_download",
                                            case_id=case_id_for_logging,
                                            run_id=run_id,
                                            source=target_source,
                                        )

                                        result, download_info = handle_dl_bfile_from_ajax(
                                            mode=scrape_mode,
                                            fname=fname_token or fname_key,
                                            box_url=box_url,
                                            downloads_dir=config.PDF_DIR,
                                            cases_by_action=CASES_BY_ACTION,
                                            processed_this_run=processed_this_run,
                                            checkpoint=checkpoint if is_new_mode(scrape_mode) else None,
                                            metadata=meta,
                                            http_client=None,
                                            case_context=case_context,
                                            fid=None,
                                            run_id=run_id,
                                            download_executor=download_executor,
                                            source=target_source,
                                        )

                                        error_message = download_info.get("error_message") if isinstance(download_info, dict) else None
                                        error_code = download_info.get("error_code") if isinstance(download_info, dict) else None
                                        file_path = download_info.get("file_path") if isinstance(download_info, dict) else None
                                        file_size_bytes = download_info.get("file_size_bytes") if isinstance(download_info, dict) else None
                                        box_url_last = download_info.get("box_url") if isinstance(download_info, dict) else box_url

                                        if state is not None:
                                            try:
                                                if result == "downloaded":
                                                    state.mark_downloaded(
                                                        file_path=file_path,
                                                        file_size_bytes=file_size_bytes,
                                                        box_url=box_url_last,
                                                    )
                                                elif result in {"existing_file", "checkpoint_skip", "duplicate_in_run"}:
                                                    state.mark_skipped(reason=result)
                                                elif result == "disk_full":
                                                    state.mark_failed(
                                                        error_code="disk_full",
                                                        error_message=error_message,
                                                    )
                                                elif result == "failed":
                                                    state.mark_failed(
                                                        error_code=error_code or ErrorCode.INTERNAL,
                                                        error_message=error_message,
                                                    )
                                            except Exception as exc:  # noqa: BLE001
                                                log_line(
                                                    f"[DB][WARN] Unable to update download status for case_id={case_id_for_logging}: {exc}"
                                                )

                                        skip_reason = _apply_result_to_summary(
                                            summary, result, error_code=error_code or ErrorCode.INTERNAL
                                        )
                                        if result == "downloaded":
                                            consecutive_existing = 0
                                        elif result in {"existing_file", "checkpoint_skip"}:
                                            consecutive_existing += 1
                                        elif result == "disk_full":
                                            summary.setdefault("stop_reason", "disk_full")
                                            active = False
                                            crash_stop = True

                                        last_title = getattr(case_for_fname, "title", None)
                                        if last_title is None and isinstance(case_for_fname, dict):
                                            last_title = case_for_fname.get("title")

                                        meta_payload = {
                                            "fname": fname_token or fname_key,
                                            "title": last_title or (metadata_entry or {}).get("title") or "",
                                            "subject": getattr(case_for_fname, "subject", None)
                                            or (metadata_entry or {}).get("subject")
                                            or "",
                                            "court": getattr(case_for_fname, "court", None)
                                            or (metadata_entry or {}).get("court")
                                            or "",
                                            "category": getattr(case_for_fname, "category", None)
                                            or (metadata_entry or {}).get("category")
                                            or "",
                                            "cause_no": getattr(case_for_fname, "cause_number", None)
                                            or (metadata_entry or {}).get("cause_number")
                                            or "",
                                            "judgment_date": getattr(case_for_fname, "judgment_date", None)
                                            or (metadata_entry or {}).get("judgment_date")
                                            or "",
                                            "page": page_index_zero,
                                            "idx": i,
                                            "file_path": (metadata_entry or {}).get("local_path")
                                            or (metadata_entry or {}).get("saved_path")
                                            or "",
                                            "size": (metadata_entry or {}).get("bytes") or 0,
                                        }

                                        if result == "downloaded":
                                            telemetry.add("downloaded", "ok", meta_payload)
                                        elif result == "failed":
                                            telemetry.add("failed", "download_other", meta_payload)
                                        elif result in {"existing_file", "checkpoint_skip"}:
                                            telemetry.add("skipped", "exists", meta_payload)
                                        elif result == "duplicate_in_run":
                                            telemetry.add("skipped", "in_run_dup", meta_payload)

                                        _persist_state(page_index_zero, i + 1, last_fname=fname_token or fname_key, last_title=last_title)

                                        if (
                                            is_new_mode(scrape_mode)
                                            and consecutive_existing >= config.SCRAPE_NEW_CONSECUTIVE_LIMIT
                                        ):
                                            log_line(
                                                "[RUN] Consecutive already-downloaded threshold reached; halting NEW mode run."
                                            )
                                            _scraper_event(
                                                "table",
                                                reason="consecutive_existing_limit",
                                                mode="new",
                                                consecutive_existing=consecutive_existing,
                                                limit=config.SCRAPE_NEW_CONSECUTIVE_LIMIT,
                                            )
                                            row_limit_reached = True
                                            break

                                        if checkpoint is not None:
                                            checkpoint.mark_position(page_index_zero, i, mode=scrape_mode)

                                        if crash_stop or not active:
                                            break

                                        continue
    
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
                                            el.click(timeout=config.PLAYWRIGHT_CLICK_TIMEOUT_MS)
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
                                            _refresh_datatable_page(
                                                page, page_index_zero, selectors=selectors
                                            )
                                            wait_seconds(page, config.PLAYWRIGHT_RETRY_PAGE_SETTLE_SECONDS)
    
                                    if crash_stop:
                                        break
    
                                    if not click_success:
                                        log_line(
                                            f"[RUN][WARN] Unable to click download button index {i} on page {page_number} after retries; marking as failed."
                                        )
                                        pending_by_fname.pop(fname_key, None)
                                        summary["failed"] += 1
                                        _bump_reason(summary["fail_reasons"], "click_timeout")
                                        attempt_for_retry = 1
                                        if run_id is not None and isinstance(case_id_for_logging, int):
                                            try:
                                                click_state = CaseDownloadState.start(
                                                    run_id=run_id,
                                                    case_id=case_id_for_logging,
                                                    box_url=None,
                                                )
                                                attempt_for_retry = max(1, click_state.attempt_count)
                                                click_state.mark_failed(
                                                    error_code="click_timeout",
                                                    error_message="click_retries_exhausted",
                                                )
                                            except Exception as exc:  # noqa: BLE001
                                                log_line(
                                                    f"[DB][WARN] Unable to record click failure for case_id={case_id_for_logging}: {exc}"
                                                )
                                        if fname_key and not any(
                                            item.get("fname") == fname_key for item in failed_items
                                        ):
                                            failed_items.append(
                                                {
                                                    "fname": fname_key,
                                                    "raw": fname_token,
                                                    "page_index": page_index_zero,
                                                    "button_index": i,
                                                    "case": case_for_fname,
                                                    "case_id": case_id_for_logging,
                                                    "error_code": "click_timeout",
                                                    "attempt": attempt_for_retry,
                                                }
                                            )
                                        if checkpoint is not None:
                                            checkpoint.mark_position(page_index_zero, i, mode=scrape_mode)
                                        continue
    
                                    total_clicks += 1
                                    summary["processed"] = total_clicks
                                    log_line(
                                        f"Clicked download button index {i} on page {page_number} (fname={fname_token})."
                                    )
    
                                    time.sleep((per_delay or 0) + config.PLAYWRIGHT_POST_CLICK_SLEEP_SECONDS)
    
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
                                selectors=selectors,
                                run_id=run_id,
                                download_executor=download_executor,
                            )
    
                        time.sleep(config.PLAYWRIGHT_RETRY_AFTER_SWEEP_SECONDS)
    
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
                _scraper_event(
                    "error",
                    context="run",
                    error="scrape_attempt_error",
                    attempt=attempt,
                    max_retries=max_retries,
                )
                if attempt > max_retries:
                    log_line("[RUN] Exhausted restart attempts; aborting scrape.")
                    _scraper_event(
                        "error",
                        context="run",
                        error="scrape_attempt_error_max_retries",
                        attempt=attempt,
                        max_retries=max_retries,
                    )
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

        _log_download_executor_summary(download_executor)

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
    finally:
        try:
            download_executor.shutdown()
        except Exception as exc:  # noqa: BLE001
            log_line(f"[RUN][WARN] Error shutting down DownloadExecutor: {exc}")


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
    target_source: Optional[str] = None,
) -> Dict[str, Any]:
    """Public entrypoint that wraps the core scraper with retry support."""

    ensure_dirs()
    db.initialize_schema()
    log_path = setup_run_logger()

    raw_mode = (scrape_mode or config.SCRAPE_MODE_DEFAULT).strip().lower()
    mode = _normalize_scrape_mode(raw_mode)

    effective_source = sources.coerce_source(
        target_source if target_source is not None else config.DEFAULT_SOURCE
    )

    runtime = config.get_source_runtime(effective_source)
    selectors = _selectors_for_source(effective_source)

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
            if state is not None:
                _scraper_event(
                    "state",
                    source="checkpoint_file",
                    resume_mode=normalized_resume,
                    dt_page_index=state.get("dt_page_index"),
                    row_index=state.get("button_index"),
                )
        if state is None and normalized_resume in {"logs", "auto"}:
            state = derive_checkpoint_from_logs()
            if state is not None:
                _scraper_event(
                    "state",
                    source="logs",
                    resume_mode=normalized_resume,
                    dt_page_index=state.get("dt_page_index"),
                    row_index=state.get("row_index") or state.get("button_index"),
                )
        if state is not None:
            resume_state = dict(state)
        if resume_page is not None:
            resume_state = resume_state or {}
            resume_state["dt_page_index"] = resume_page
            _scraper_event(
                "state",
                source="cli_override",
                resume_mode=normalized_resume,
                dt_page_index=resume_page,
                row_index=(resume_state or {}).get("button_index"),
            )
        if resume_index is not None:
            resume_state = resume_state or {}
            resume_state["button_index"] = resume_index
            _scraper_event(
                "state",
                source="cli_override",
                resume_mode=normalized_resume,
                dt_page_index=(resume_state or {}).get("dt_page_index"),
                row_index=resume_index,
            )

    next_start_message = start_message
    run_id: Optional[int] = None

    base_url = (base_url or runtime.base_url).strip()

    http_session = csv_sync.build_http_session()
    csv_url = runtime.csv_url
    sync_result = csv_sync.sync_csv(csv_url, session=http_session, source=effective_source)
    csv_version_id = sync_result.version_id
    csv_source = sync_result.csv_path or csv_url

    _scraper_event(
        "plan",
        csv_version_id=sync_result.version_id,
        csv_row_count=sync_result.row_count,
        new_case_ids=len(sync_result.new_case_ids),
        changed_case_ids=len(sync_result.changed_case_ids),
        removed_case_ids=len(sync_result.removed_case_ids),
    )

    run_params = {
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
    }

    try:
        run_id = create_run_with_source(
            trigger=trigger or "cli",
            mode=mode,
            csv_version_id=csv_version_id,
            target_source=effective_source,
            extra_params=run_params,
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
            sync_result=sync_result,
            target_source=effective_source,
            selectors=selectors,
        )
        next_start_message = None
        return result

    try:
        result = run_with_retries(_attempt, max_retries=max(1, retry_limit))
        result.setdefault("target_source", effective_source)
        coverage: Dict[str, Any] = {}
        if run_id is not None:
            result.setdefault("run_id", run_id)
        result.setdefault("csv_version_id", csv_version_id)
        if run_id is not None:
            try:
                coverage = db_reporting.get_run_coverage(run_id)
                result.update(coverage)
            except Exception as coverage_exc:  # noqa: BLE001
                log_line(f"[DB][WARN] Unable to compute coverage for run_id={run_id}: {coverage_exc}")
            if coverage:
                try:
                    db.update_run_coverage(run_id, coverage)
                except Exception as exc:  # noqa: BLE001
                    log_line(
                        f"[DB][WARN] Unable to persist coverage for run_id={run_id}: {exc}"
                    )
            try:
                db.mark_run_completed(run_id)
            except Exception as exc:  # noqa: BLE001
                log_line(f"[DB][WARN] Unable to mark run completed: {exc}")
        try:
            save_json_file(config.SUMMARY_FILE, result)
        except Exception as exc:  # noqa: BLE001
            log_line(f"[RUN][WARN] Unable to write summary with coverage: {exc}")
        return result
    except Exception as exc:  # noqa: BLE001
        _scraper_event(
            "error",
            run_id=run_id,
            mode=mode,
            error=_short_error_message(exc),
        )
        if run_id is not None:
            try:
                coverage = db_reporting.get_run_coverage(run_id)
                db.update_run_coverage(run_id, coverage)
            except Exception as coverage_exc:  # noqa: BLE001
                log_line(
                    f"[DB][WARN] Unable to compute or persist coverage for failed run {run_id}: {coverage_exc}"
                )
            try:
                db.mark_run_failed(run_id, _short_error_message(exc))
            except Exception as mark_exc:  # noqa: BLE001
                log_line(f"[DB][WARN] Unable to mark run failed: {mark_exc}")
        raise


def _cli_entrypoint(argv: Optional[List[str]] = None) -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(description="Run the Playwright scraper")
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--page-wait", type=int, default=config.PAGE_WAIT_SECONDS)
    parser.add_argument("--per-download-delay", type=float, default=config.PER_DOWNLOAD_DELAY)
    parser.add_argument(
        "--scrape-mode",
        choices=["new", "full", "resume"],
        default=config.SCRAPE_MODE_DEFAULT,
    )
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

    parser.add_argument(
        "--source",
        dest="target_source",
        help=(
            "Logical source to scrape (e.g. 'unreported_judgments', 'public_registers'). "
            "Unknown values fall back to the default source with a warning; if omitted, "
            "the default is used silently."
        ),
        default=None,
    )

    args = parser.parse_args(argv)

    ensure_dirs()
    validate_runtime_config("cli", mode=args.scrape_mode)
    target_source = sources.coerce_source(args.target_source)

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
        target_source=target_source,
    )


if __name__ == "__main__":  # pragma: no cover
    _cli_entrypoint()

__all__ = ["run_scrape", "_cli_entrypoint"]
