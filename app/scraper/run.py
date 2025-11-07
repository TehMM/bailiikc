"""High-level orchestration for scraping operations (Playwright-based)."""

from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import Page, sync_playwright

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
    "Chrome/142.0.0.0 Safari/537.36"
)


# ---------- DOM helpers ----------

_LOAD_MORE_SELECTORS: Sequence[str] = (
    "button.pt-cv-loadmore",
    "button:has-text(\"Load more\")",
    "button:has-text(\"Load More\")",
    "a.pt-cv-loadmore",
    ".pt-cv-loadmore button",
    ".pt-cv-loadmore a",
)

_ATTR_FNAME_KEYS: Sequence[str] = (
    "data-fname",
    "data-name",
    "data-file-name",
    "data-filename",
    "data-title",
    "data-code",
)

_ATTR_FID_KEYS: Sequence[str] = (
    "data-fid",
    "data-id",
    "data-file",
    "data-file-id",
    "data-dfid",
    "data-dl-fid",
)

_ATTR_NONCE_KEYS: Sequence[str] = (
    "data-s",
    "data-security",
    "data-nonce",
    "data-token",
)

_RE_PAIR_CAPTURE = re.compile(
    r"(?P<key>[A-Za-z0-9_-]+)[\"']?\s*(?:[:=])\s*[\"']?(?P<val>[A-Za-z0-9._-]+)",
    flags=re.I,
)

_NONCE_REGEXES = (
    re.compile(r"['\"](?:_?nonce|security)['\"]\s*[:=]\s*['\"]([A-Za-z0-9]{6,})['\"]"),
    re.compile(r"dl_bfile[^A-Za-z0-9]+['\"]([A-Za-z0-9]{6,})['\"]", flags=re.I),
)


# ---------- Utility helpers ----------

def _normalize_fname(value: str) -> str:
    cleaned = value.strip()
    if cleaned.lower().endswith(".pdf"):
        cleaned = cleaned[:-4]
    return cleaned.strip().upper()


def _fname_variants(value: str) -> List[str]:
    cleaned = value.strip()
    variants = {cleaned, cleaned.upper()}
    if cleaned.lower().endswith(".pdf"):
        core = cleaned[:-4]
        variants.update({core, core.upper()})
    sanitized = sanitize_filename(cleaned)
    variants.update({sanitized, sanitized.upper()})
    if sanitized.lower().endswith(".pdf"):
        core = sanitized[:-4]
        variants.update({core, core.upper()})
    return [v for v in variants if v]


def _first_attr(tag, keys: Sequence[str]) -> str:
    for key in keys:
        value = tag.get(key)
        if value:
            return str(value).strip()
    return ""


def _parse_payload(text: str) -> Tuple[str | None, str | None, str | None]:
    if not text:
        return (None, None, None)

    fid: str | None = None
    fname: str | None = None
    nonce: str | None = None

    for match in _RE_PAIR_CAPTURE.finditer(text.replace("&amp;", "&")):
        key = match.group("key").lower()
        val = match.group("val").strip()
        if not val:
            continue
        if key in {"fid", "file", "id"}:
            if not fid or val.isdigit():
                fid = val
        elif key in {"fname", "name"}:
            if not fname:
                fname = val
        elif key in {"security", "nonce", "s"}:
            if not nonce:
                nonce = val

    return (fid, fname, nonce)


def _iter_related_nodes(tag) -> Iterable:
    yield tag
    # limited parents
    depth = 0
    for parent in getattr(tag, "parents", []):
        if depth >= 4:
            break
        yield parent
        depth += 1
    # limited children
    try:
        for child in tag.find_all(True, limit=10):
            yield child
    except Exception:  # noqa: BLE001
        return


# ---------- Playwright helpers ----------

def _load_all_results(page: Page, max_loadmore: int = 200) -> None:
    """Trigger lazy loading by clicking \"Load more\" buttons and scrolling."""
    clicks = 0
    while clicks < max_loadmore:
        load_button = None
        for selector in _LOAD_MORE_SELECTORS:
            try:
                locator = page.locator(selector)
                if locator.count() and locator.first.is_enabled() and locator.first.is_visible():
                    load_button = locator.first
                    break
            except Exception:  # noqa: BLE001
                continue
        if not load_button:
            break
        try:
            load_button.click()
            clicks += 1
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(400)
        except Exception as exc:  # noqa: BLE001
            log_line(f"Load-more click failed after {clicks} interactions: {exc}")
            break

    last_height = 0
    for _ in range(30):
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:  # noqa: BLE001
            break
        page.wait_for_timeout(350)
        try:
            page.wait_for_load_state("networkidle")
        except Exception:  # noqa: BLE001
            pass
        try:
            height = page.evaluate("document.body.scrollHeight")
        except Exception:  # noqa: BLE001
            break
        if height == last_height:
            break
        last_height = height

    if clicks:
        log_line(f"Triggered {clicks} load-more interactions before scrolling complete.")


def _collect_buttons(page: Page) -> Tuple[List[Tuple[str, str, str]], List[str], str]:
    """Collect (fid, fname, nonce) tuples from the rendered DOM."""
    html = page.content()
    soup = BeautifulSoup(html, "html.parser")

    nonce_candidates: set[str] = set()
    results: list[Tuple[str, str, str]] = []
    seen: set[Tuple[str, str]] = set()

    def maybe_add(fid: str | None, fname: str | None, nonce: str | None) -> None:
        if not fid or not fname:
            return
        fid = fid.strip()
        fname = fname.strip()
        nonce_val = (nonce or "").strip()
        if not re.fullmatch(r"\d{5,}", fid):
            return
        key = (fid, fname)
        if key in seen:
            return
        seen.add(key)
        if nonce_val:
            nonce_candidates.add(nonce_val)
        results.append((fid, fname, nonce_val))

    for tag in soup.select("[data-s]"):
        nonce_val = str(tag.get("data-s")).strip()
        if nonce_val and re.fullmatch(r"[A-Za-z0-9]{6,}", nonce_val):
            nonce_candidates.add(nonce_val)

    for tag in soup.select("[data-security]"):
        nonce_val = str(tag.get("data-security")).strip()
        if nonce_val and re.fullmatch(r"[A-Za-z0-9]{6,}", nonce_val):
            nonce_candidates.add(nonce_val)

    for tag in soup.select("[data-fid]"):
        fid = _first_attr(tag, _ATTR_FID_KEYS)
        fname = _first_attr(tag, _ATTR_FNAME_KEYS)
        nonce = _first_attr(tag, _ATTR_NONCE_KEYS)

        if not fname or not nonce:
            for related in _iter_related_nodes(tag):
                if not fname:
                    fname = _first_attr(related, _ATTR_FNAME_KEYS)
                if not nonce:
                    nonce = _first_attr(related, _ATTR_NONCE_KEYS)
                if fname and nonce:
                    break

        if not fname:
            payload_attrs = (
                "data-params",
                "data-options",
                "data-config",
                "data-request",
                "data-payload",
                "data-data",
            )
            for attr in payload_attrs:
                value = tag.get(attr)
                if not value:
                    continue
                fid_candidate, fname_candidate, nonce_candidate = _parse_payload(str(value))
                fid = fid or fid_candidate or ""
                fname = fname or fname_candidate or ""
                nonce = nonce or nonce_candidate or ""
                if fname:
                    break

        if not fname:
            text = tag.get_text(" ", strip=True)
            match = re.search(r"[A-Za-z0-9._-]{6,}", text or "")
            if match:
                fname = match.group(0)

        maybe_add(fid or "", fname or "", nonce or "")

    anchor_selectors = (
        "a[href*='dl_bfile']",
        "button[onclick*='dl_bfile']",
        "a[data-params]",
        "button[data-params]",
    )
    for selector in anchor_selectors:
        for tag in soup.select(selector):
            payload_sources = (
                tag.get("href"),
                tag.get("onclick"),
                tag.get("data-params"),
                tag.get("data-request"),
            )
            fid = fname = nonce = ""
            for source in payload_sources:
                if not source:
                    continue
                fid_candidate, fname_candidate, nonce_candidate = _parse_payload(str(source))
                fid = fid or fid_candidate or ""
                fname = fname or fname_candidate or ""
                nonce = nonce or nonce_candidate or ""
            maybe_add(fid or "", fname or "", nonce or "")

    if not nonce_candidates:
        script_text = "\n".join(script.get_text(" ", strip=True) for script in soup.find_all("script"))
        for regex in _NONCE_REGEXES:
            match = regex.search(script_text)
            if match:
                nonce_candidates.add(match.group(1))

    return results, sorted(nonce_candidates), html


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
    """Execute a scraping run using Playwright + dl_bfile AJAX."""
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

    cases = load_cases_from_csv(config.CSV_URL)
    meta = load_metadata()

    cases_by_fname: dict[str, dict[str, Any]] = {}
    for case in cases:
        fname = case.get("fname")
        if not fname:
            continue
        for variant in _fname_variants(fname):
            cases_by_fname.setdefault(variant.upper(), case)

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
        summary["error"] = "no_cases"
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

            _load_all_results(page)

            buttons, nonce_candidates, page_html = _collect_buttons(page)
            log_line(f"Found {len(buttons)} download candidates on page.")

            if not buttons:
                snippet = re.sub(r"\s+", " ", page_html[:600])
                log_line(
                    "No valid download buttons discovered on page. "
                    f"HTML snippet: {snippet}"
                )
                summary["error"] = "no_buttons"
                return summary

            api = context.request
            global_nonce = nonce_candidates[0] if nonce_candidates else ""

            for fid, fname, candidate_nonce in buttons:
                if summary["processed"] >= entry_cap:
                    break

                normalized_fname = _normalize_fname(fname)
                case = None
                for key in _fname_variants(fname):
                    case = cases_by_fname.get(key.upper())
                    if case:
                        break

                record_fid = (case or {}).get("fid") or normalized_fname

                sanitized = sanitize_filename(normalized_fname or fname)
                filename = sanitized if sanitized.lower().endswith(".pdf") else f"{sanitized}.pdf"
                out_path = config.PDF_DIR / filename

                if out_path.exists() or is_duplicate(record_fid, filename, meta):
                    log_line(
                        f"Skipping {record_fid} ({filename}) – already downloaded/recorded"
                    )
                    summary["skipped"] += 1
                    continue

                summary["processed"] += 1

                nonce_options = []
                if candidate_nonce:
                    nonce_options.append(candidate_nonce)
                for value in nonce_candidates:
                    if value and value not in nonce_options:
                        nonce_options.append(value)
                if global_nonce and global_nonce not in nonce_options:
                    nonce_options.append(global_nonce)

                box_url = None
                last_error: Exception | None = None
                for nonce_val in nonce_options or [""]:
                    if not nonce_val:
                        continue
                    try:
                        log_line(
                            f"Requesting Box URL for fid={fid} fname={fname} nonce={nonce_val}"
                        )
                        box_url = _fetch_box_url(api, fid, fname, nonce_val, referer=base_url)
                        break
                    except Exception as exc:  # noqa: BLE001
                        last_error = exc
                        log_line(
                            f"AJAX lookup failed for fid={fid} with nonce={nonce_val}: {exc}"
                        )
                        continue

                if not box_url:
                    if last_error:
                        summary["failed"] += 1
                        log_line(
                            f"Exhausted nonce attempts for fid={fid} fname={fname}: {last_error}"
                        )
                        time.sleep(per_delay)
                        continue
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
                        fid=record_fid,
                        filename=filename,
                        fields={
                            "title": (case or {}).get("title"),
                            "category": (case or {}).get("category"),
                            "judgment_date": (case or {}).get("judgment_date"),
                            "source_url": box_url,
                            "size_bytes": size_bytes,
                            "box_fid": fid,
                            "box_fname": fname,
                        },
                    )

                    log_line(
                        f"Saved {filename} ({size_bytes / 1024:.1f} KiB) "
                        f"for case '{(case or {}).get('title', '').strip()}'"
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
