from __future__ import annotations

"""Shared helpers for building download rows for reporting."""

from pathlib import Path
from typing import Any, Dict, List

from app.scraper import config, sources
from app.scraper.utils import ensure_dirs, load_json_lines
from app.scraper.date_utils import sortable_date


def load_download_records() -> List[Dict[str, Any]]:
    """Return download records sourced from ``downloads.jsonl``."""

    ensure_dirs()
    return load_json_lines(config.DOWNLOADS_LOG)


def build_download_rows(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build structured rows for the downloads table."""

    rows: List[Dict[str, Any]] = []
    for entry in records:
        if not isinstance(entry, dict):
            continue

        saved_path = entry.get("saved_path") or ""
        title = entry.get("title") or entry.get("subject") or saved_path
        judgment_date = entry.get("judgment_date") or ""
        actions_token = entry.get("actions_token") or ""

        filename = Path(saved_path).name if saved_path else ""

        rows.append(
            {
                "actions_token": actions_token,
                "title": title,
                "subject": entry.get("subject") or "",
                "court": entry.get("court") or "",
                "category": entry.get("category") or "",
                "judgment_date": judgment_date,
                "sort_judgment_date": sortable_date(str(judgment_date)),
                "cause_number": entry.get("cause_number") or "",
                "downloaded_at": entry.get("downloaded_at") or "",
                "saved_path": saved_path,
                "filename": filename,
                "size_kb": round((entry.get("bytes") or 0) / 1024, 1)
                if entry.get("bytes")
                else 0,
                "source": sources.coerce_source(entry.get("source")),
            }
        )
    return rows
