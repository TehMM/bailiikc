"""CSV sync utilities for the judgments index.

This module is aligned with the SCRAPER_DESIGN_FRAMEWORK and is used by the
live scraper to fetch the remote judgments CSV, record ``csv_versions`` rows,
and upsert case metadata into the SQLite ``cases`` table. ``sync_csv`` returns
the concrete CSV file path and row count so scraper runs can build their
in-memory index from the exact payload recorded in the database (with
``BAILIIKC_USE_DB_CASES`` enabling an optional DB-backed index).
"""
from __future__ import annotations

import csv
import hashlib
import io
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import requests

from . import config, db, sources
from .cases_index import normalize_action_token as normalize_action_token_cases
from .utils import log_line


@dataclass
class CsvSyncResult:
    """Outcome of a CSV sync attempt.

    ``csv_path`` and ``row_count`` describe the concrete CSV file that was
    fetched and used to drive this version, so scraper runs can build their
    case index from the exact payload recorded in ``csv_versions``. ``source``
    records the logical source string persisted into ``cases.source``.
    """

    version_id: int
    is_new_version: bool
    new_case_ids: List[int]
    changed_case_ids: List[int]
    removed_case_ids: List[int]
    csv_path: str = ""
    row_count: int = 0
    source: str = sources.DEFAULT_SOURCE


def normalize_action_token(token: str) -> str:
    """Normalise a raw 'Actions' token from the judgments CSV.

    This delegates to :func:`cases_index.normalize_action_token` to ensure the
    scraper and CSV sync share a single canonical implementation.
    """

    return normalize_action_token_cases(token)


def parse_judgment_date(raw: str) -> str:
    """Return a normalised judgment date in ``YYYY-MM-DD`` format when possible."""

    candidate = (raw or "").strip()
    if not candidate:
        return ""

    formats = (
        "%Y-%m-%d",
        "%Y-%b-%d",
        "%d/%m/%Y",
        "%d-%b-%Y",
    )
    for fmt in formats:
        try:
            return datetime.strptime(candidate, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    digits = re.sub(r"[^0-9]", "", candidate)
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"

    if candidate:
        trimmed = candidate.strip()
        if len(trimmed) > 64:
            trimmed = trimmed[:61] + "..."
        log_line(
            f"[CSV][WARN] Unable to normalise judgment date {trimmed!r}; leaving as-is."
        )
    return candidate


def build_http_session() -> requests.Session:
    """Return a requests session configured for CSV fetches."""

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": config.COMMON_HEADERS.get("User-Agent", "bailiikc scraper"),
            "Accept": "text/csv, */*;q=0.8",
        }
    )
    return session


def _save_csv_copy(content: bytes, sha256: str) -> Path:
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    short_sha = sha256[:8]
    csv_dir = config.DATA_DIR / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    path = csv_dir / f"judgments_{timestamp}_{short_sha}.csv"
    path.write_bytes(content)
    return path


def _validate_fieldnames(fieldnames: Optional[list[str]]) -> None:
    """Ensure required columns are present in the CSV headers."""

    if not fieldnames:
        raise ValueError("CSV is missing header row")
    if "Actions" not in fieldnames and "Action" not in fieldnames:
        raise ValueError("CSV missing required Actions column")


def _parse_row(row: dict[str, str]) -> tuple[list[str], dict[str, str]]:
    actions_raw = (row.get("Actions") or row.get("Action") or "").strip()
    if not actions_raw:
        return [], {}

    title = (row.get("Title") or row.get("Case Title") or row.get("Subject") or "").strip()
    subject = (row.get("Subject") or title or "").strip()
    court = (row.get("Court") or row.get("Court file") or "").strip()
    category = (row.get("Category") or "").strip()
    judgment_date = parse_judgment_date(row.get("Judgment Date") or row.get("Date") or "")
    cause_number = (
        row.get("Cause Number")
        or row.get("Cause number")
        or row.get("Cause No.")
        or row.get("Cause")
        or ""
    ).strip()

    tokens: list[str] = []
    for piece in re.split(r"[|,;/\\\s]+", actions_raw):
        norm = normalize_action_token(piece)
        if norm:
            tokens.append(norm)

    metadata = {
        "action_token_raw": actions_raw,
        "title": title,
        "subject": subject,
        "court": court,
        "category": category,
        "judgment_date": judgment_date,
        "cause_number": cause_number,
    }
    return tokens, metadata


def infer_is_criminal(metadata: dict[str, str]) -> int:
    """Placeholder classifier for criminal matters derived from CSV metadata."""

    # TODO: implement real classification logic based on category/subject/title.
    return 0


def sync_csv(
    source_url: str,
    session: Optional[requests.Session] = None,
    *,
    source: str = sources.UNREPORTED_JUDGMENTS,
) -> CsvSyncResult:
    """Fetch, validate, and persist the remote judgments CSV.

    The function records a csv_versions row regardless of whether the payload is
    new. It populates the cases table with upserts but does not alter the live
    scraping workflow yet.
    """

    http_session = session or build_http_session()
    response = http_session.get(source_url, timeout=(10, 60))
    response.raise_for_status()
    content = response.content
    sha256 = hashlib.sha256(content).hexdigest()

    fetched_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    latest = db.get_latest_valid_csv_version()
    is_new_version = not latest or latest["sha256"] != sha256
    # NOTE: Even if ``is_new_version`` is False, we still parse and upsert all
    # rows for now. Future optimisation may short-circuit when the hash is
    # unchanged.

    csv_path = _save_csv_copy(content, sha256)
    rows: list[dict[str, str]] = []
    row_count = 0
    try:
        decoded = content.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(decoded))
        _validate_fieldnames(reader.fieldnames)
        rows = list(reader)
        row_count = len(rows)
    except Exception as exc:  # noqa: BLE001
        db.record_csv_version(
            fetched_at=fetched_at,
            source_url=source_url,
            etag=response.headers.get("ETag"),
            last_modified=response.headers.get("Last-Modified"),
            sha256=sha256,
            row_count=row_count,
            file_path=str(csv_path),
            valid=False,
            error_message=str(exc),
        )
        raise

    version_id = db.record_csv_version(
        fetched_at=fetched_at,
        source_url=source_url,
        etag=response.headers.get("ETag"),
        last_modified=response.headers.get("Last-Modified"),
        sha256=sha256,
        row_count=row_count,
        file_path=str(csv_path),
        valid=True,
    )

    new_case_ids: List[int] = []
    changed_case_ids: List[int] = []

    conn = db.get_connection()
    with conn:
        for row in rows:
            tokens, metadata = _parse_row(row)
            if not tokens:
                continue

            for token in tokens:
                norm = token

                cursor = conn.execute(
                    """
                    SELECT * FROM cases
                    WHERE action_token_norm = ? AND source = ?
                    LIMIT 1
                    """,
                    (norm, source),
                )
                existing = cursor.fetchone()

                fields = (
                    metadata.get("title"),
                    metadata.get("cause_number"),
                    metadata.get("court"),
                    metadata.get("category"),
                    metadata.get("judgment_date"),
                )

                if existing:
                    if (
                        existing["title"],
                        existing["cause_number"],
                        existing["court"],
                        existing["category"],
                        existing["judgment_date"],
                    ) != fields:
                        conn.execute(
                            """
                            UPDATE cases
                            SET title = ?, cause_number = ?, court = ?, category = ?,
                                judgment_date = ?, last_seen_version_id = ?, is_active = 1
                            WHERE id = ?
                            """,
                            (
                                metadata.get("title"),
                                metadata.get("cause_number"),
                                metadata.get("court"),
                                metadata.get("category"),
                                metadata.get("judgment_date"),
                                version_id,
                                existing["id"],
                            ),
                        )
                        changed_case_ids.append(int(existing["id"]))
                    else:
                        conn.execute(
                            "UPDATE cases SET last_seen_version_id = ?, is_active = 1 WHERE id = ?",
                            (version_id, existing["id"]),
                        )
                else:
                    is_criminal = infer_is_criminal(metadata)
                    cursor = conn.execute(
                        """
                        INSERT INTO cases (
                            action_token_raw, action_token_norm, title, cause_number,
                            court, category, judgment_date, is_criminal, is_active,
                            source, first_seen_version_id, last_seen_version_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                        """,
                        (
                            metadata.get("action_token_raw"),
                            norm,
                            metadata.get("title"),
                            metadata.get("cause_number"),
                            metadata.get("court"),
                            metadata.get("category"),
                            metadata.get("judgment_date"),
                            is_criminal,
                            source,
                            version_id,
                            version_id,
                        ),
                    )
                    new_case_ids.append(int(cursor.lastrowid))

        cursor = conn.execute(
            """
            SELECT id FROM cases
            WHERE source = ? AND last_seen_version_id < ? AND is_active = 1
            """,
            (source, version_id),
        )
        removed_case_ids = [int(row["id"]) for row in cursor.fetchall()]
        if removed_case_ids:
            # Build the correct number of placeholders for a parameterised IN clause.
            conn.execute(
                """
                UPDATE cases
                SET is_active = 0, last_seen_version_id = ?
                WHERE id IN (%s)
                """
                % ",".join(["?"] * len(removed_case_ids)),
                [version_id, *removed_case_ids],
            )

    log_line(
        "[CSV_SYNC] version=%s new_version=%s new=%s changed=%s removed=%s rows=%s file=%s"
        % (
            version_id,
            is_new_version,
            len(new_case_ids),
            len(changed_case_ids),
            len(removed_case_ids),
            row_count,
            csv_path,
        )
    )

    return CsvSyncResult(
        version_id=version_id,
        is_new_version=is_new_version,
        new_case_ids=new_case_ids,
        changed_case_ids=changed_case_ids,
        removed_case_ids=removed_case_ids,
        csv_path=str(csv_path),
        row_count=row_count,
        source=source,
    )
