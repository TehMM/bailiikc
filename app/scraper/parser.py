"""CSV parsing utilities for judicial case metadata."""
from __future__ import annotations

import csv
import html
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

from . import config
from .utils import log_line, sanitize_filename


def _extract_anchor_data(actions_html: str) -> tuple[str | None, str | None]:
    """Extract fid and fname attributes from an HTML anchor snippet."""
    soup = BeautifulSoup(actions_html, "html5lib")
    anchor = soup.find("a")
    if not anchor:
        return None, None

    attr_candidates = [
        "data-fid",
        "data-file",
        "data-id",
        "data-file-id",
        "data-dfid",
    ]
    fid = None
    for key in attr_candidates:
        if anchor.has_attr(key):
            fid = anchor.get(key)
            break

    name_candidates = [
        "data-fname",
        "data-name",
        "data-file-name",
        "data-filename",
        "data-title",
    ]
    fname = None
    for key in name_candidates:
        if anchor.has_attr(key):
            fname = anchor.get(key)
            break

    if not fid and anchor.has_attr("href"):
        query = parse_qs(urlparse(anchor["href"]).query)
        for key in ["fid", "file", "id"]:
            if key in query:
                fid = query[key][0]
                break

    if not fname:
        if anchor.has_attr("href"):
            query = parse_qs(urlparse(anchor["href"]).query)
            for key in ["fname", "name", "file"]:
                if key in query:
                    fname = query[key][0]
                    break
        if not fname:
            fname = anchor.get_text(strip=True)

    return (fid or None, fname or None)


def load_cases_from_csv(csv_url: str = config.CSV_URL) -> list[dict[str, Any]]:
    """Download and parse the CSV file of cases from the judiciary website.

    Args:
        csv_url: URL of the CSV file to be processed.

    Returns:
        A list of dictionaries describing each non-criminal case.
    """
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
