"""CSV parsing utilities for judicial case metadata."""
from __future__ import annotations

import csv
import html
import re
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

from . import config
from .utils import ensure_dirs, log_line, sanitize_filename


_PLAIN_TEXT_FID = re.compile(r"([A-Za-z]{1,6}\d{4,})")
_FID_ATTR_PATTERN = re.compile(r"fid[^=]*=[\"']?([A-Za-z0-9._-]+)")
_FNAME_ATTR_PATTERN = re.compile(r"fname[^=]*=[\"']?([A-Za-z0-9._-]+)")


def _extract_anchor_data(actions_html: str) -> tuple[str | None, str | None]:
    """Extract fid and fname attributes from an HTML anchor snippet."""
    soup = BeautifulSoup(actions_html, "html5lib")
    anchors = soup.find_all("a") or [soup.find("a")]

    best_fid: str | None = None
    best_fname: str | None = None

    def update_best(fid_candidate: str | None, fname_candidate: str | None) -> None:
        nonlocal best_fid, best_fname
        if fid_candidate:
            if not best_fid:
                best_fid = fid_candidate
            elif fid_candidate.isdigit() and not (best_fid and best_fid.isdigit()):
                # Prefer purely numeric identifiers which the AJAX endpoint expects.
                best_fid = fid_candidate
        if fname_candidate and not best_fname:
            best_fname = fname_candidate

    for anchor in filter(None, anchors):
        fid_candidate: str | None = None
        fname_candidate: str | None = None

        attr_candidates = [
            "data-fid",
            "data-file",
            "data-id",
            "data-file-id",
            "data-dfid",
            "data-params",
            "data-options",
            "data-config",
        ]
        for key in attr_candidates:
            if anchor.has_attr(key):
                value = anchor.get(key)
                if not value:
                    continue
                value_str = str(value)
                if key in {"data-params", "data-options", "data-config"}:
                    match = _FID_ATTR_PATTERN.search(value_str)
                    if match:
                        fid_candidate = match.group(1)
                    match = _FNAME_ATTR_PATTERN.search(value_str)
                    if match:
                        fname_candidate = match.group(1)
                else:
                    fid_candidate = value_str
                if fid_candidate:
                    break

        name_candidates = [
            "data-fname",
            "data-name",
            "data-file-name",
            "data-filename",
            "data-title",
        ]
        for key in name_candidates:
            if anchor.has_attr(key):
                value = anchor.get(key)
                if value:
                    fname_candidate = str(value)
                if fname_candidate:
                    break

        href = anchor.get("href")
        if href:
            query = parse_qs(urlparse(href).query)
            for key in ["fid", "file", "id"]:
                if key in query and not fid_candidate:
                    fid_candidate = query[key][0]
                    break
            for key in ["fname", "name", "file"]:
                if key in query and not fname_candidate:
                    fname_candidate = query[key][0]
                    break

        if not fid_candidate or not fname_candidate:
            anchor_html = str(anchor)
            if not fid_candidate:
                match = _FID_ATTR_PATTERN.search(anchor_html)
                if match:
                    fid_candidate = match.group(1)
            if not fname_candidate:
                match = _FNAME_ATTR_PATTERN.search(anchor_html)
                if match:
                    fname_candidate = match.group(1)

        if not fname_candidate:
            fname_candidate = anchor.get_text(strip=True) or None

        update_best(fid_candidate, fname_candidate)

    fid = best_fid
    fname = best_fname

    # Fallback for cases where the "Actions" column does not contain an anchor
    text_content = soup.get_text(separator=" ", strip=True)
    text_content = text_content or actions_html.strip()
    if text_content:
        match = _PLAIN_TEXT_FID.search(text_content)
        if match and not fid:
            fid = match.group(1)
        if match:
            after = text_content[match.end():].strip(" -:_")
            if after and not fname:
                fname = after

    return (fid or None, fname or None)


def load_cases_from_csv(csv_url: str = config.CSV_URL) -> list[dict[str, Any]]:
    """Download and parse the CSV file of cases from the judiciary website.

    Args:
        csv_url: URL of the CSV file to be processed.

    Returns:
        A list of dictionaries describing each non-criminal case.
    """
    ensure_dirs()
    log_line(f"Downloading CSV from {csv_url}")
    try:
        response = requests.get(csv_url, headers=config.COMMON_HEADERS, timeout=60)
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        log_line(f"Failed to download CSV: {exc}")
        return []

    decoded = response.content.decode("utf-8-sig")
    reader = csv.DictReader(decoded.splitlines())

    cases: list[dict[str, Any]] = []
    for row in reader:
        category = (row.get("Category") or "").strip()
        if "criminal" in category.lower():
            continue

        actions_raw = html.unescape(row.get("Actions", ""))
        if not actions_raw.strip():
            continue

        fid, fname = _extract_anchor_data(actions_raw)
        if not fid:
            log_line(f"Skipping row with missing fid: {actions_raw[:80]}")
            continue

        fname = sanitize_filename(fname or fid)
        log_line(
            "Parsed case: fid=%s fname=%s title=%s" % (
                fid,
                fname,
                row.get("Title", "").strip(),
            )
        )

        case = {
            "fid": fid,
            "fname": fname,
            "title": row.get("Title", "").strip(),
            "category": category,
            "court": row.get("Court", "").strip(),
            "neutral_citation": row.get("Neutral Citation", "").strip(),
            "cause_number": row.get("Cause Number", "").strip(),
            "judgment_date": row.get("Judgment Date", "").strip(),
            "subject": row.get("Subject", "").strip(),
            "actions_raw": actions_raw,
        }
        cases.append(case)

    log_line(f"Loaded {len(cases)} non-criminal cases from CSV")
    return cases


__all__ = ["load_cases_from_csv"]
