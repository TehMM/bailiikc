"""CSV sync utilities for the judgments index.

This module is aligned with the SCRAPER_DESIGN_FRAMEWORK and remains
unconnected to the live scraper for now. It can fetch the remote judgments CSV,
record versions in SQLite, and upsert case metadata for future runs.
"""
from __future__ import annotations

import csv
import hashlib
import io
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import requests

from . import config, db
from .utils import log_line


@dataclass
class CsvSyncResult:
    """Outcome of a CSV sync attempt."""

    version_id: int
    is_new_version: bool
    new_case_ids: List[int]
    changed_case_ids: List[int]
    removed_case_ids: List[int]


def normalize_action_token(token: str) -> str:
    """Normalise a raw 'Actions' token from the judgments CSV.

    Strips whitespace, uppercases the token, and removes any character that is
    not ``A-Z`` or ``0-9``. Keep this logic aligned with the design framework
    and migrate data if the source format changes.
    """

    token = (token or "").strip().upper()
    return re.sub(r"[^A-Z0-9]+", "", token)


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


def _parse_row(row: dict[str, str]) -> tuple[list[str], dict[str, str]]:
    actions_raw = (row.get("Actions") or row.get("Action") or "").strip()
    if not actions_raw:
        return [], {}

    title = (row.get("Title") or row.get("Case Title") or row.get("Subject") or "").strip()
    subject = (row.get("Subject") or title or "").strip()
    court = (row.get("Court") or row.get("Court file") or "").strip()
    category = (row.get("Category") or "").strip()
    judgment_date = (row.get("Judgment Date") or row.get("Date") or "").strip()
    cause_number = (
        row.get("Cause Number")
        or row.get("Cause number")
        or row.get("Cause No.")
        or row.get("Cause")
        or ""
    ).strip()

    tokens = [
        normalize_action_token(piece)
        for piece in re.split(r"[|,;/\\\s]+", actions_raw)
        if normalize_action_token(piece)
    ]

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


def sync_csv(source_url: str, session: Optional[requests.Session] = None) -> CsvSyncResult:
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

    latest = db.get_latest_csv_version()
    is_new_version = not latest or latest.get("sha256") != sha256

    csv_path = _save_csv_copy(content, sha256)

    try:
        decoded = content.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(decoded))
        rows = list(reader)
        row_count = len(rows)
    except Exception as exc:  # noqa: BLE001
        version_id = db.record_csv_version(
            fetched_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            source_url=source_url,
            etag=response.headers.get("ETag"),
            last_modified=response.headers.get("Last-Modified"),
            sha256=sha256,
            row_count=0,
            file_path=str(csv_path),
            valid=False,
            error_message=str(exc),
        )
        raise

    version_id = db.record_csv_version(
        fetched_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
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
                norm = normalize_action_token(token)
                if not norm:
                    continue

                cursor = conn.execute(
                    """
                    SELECT * FROM cases
                    WHERE action_token_norm = ? AND source = 'unreported_judgments'
                    LIMIT 1
                    """,
                    (norm,),
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
                    cursor = conn.execute(
                        """
                        INSERT INTO cases (
                            action_token_raw, action_token_norm, title, cause_number,
                            court, category, judgment_date, is_criminal, is_active,
                            source, first_seen_version_id, last_seen_version_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, 'unreported_judgments', ?, ?)
                        """,
                        (
                            metadata.get("action_token_raw"),
                            norm,
                            metadata.get("title"),
                            metadata.get("cause_number"),
                            metadata.get("court"),
                            metadata.get("category"),
                            metadata.get("judgment_date"),
                            version_id,
                            version_id,
                        ),
                    )
                    new_case_ids.append(int(cursor.lastrowid))

        cursor = conn.execute(
            """
            SELECT id FROM cases
            WHERE source = 'unreported_judgments' AND last_seen_version_id < ? AND is_active = 1
            """,
            (version_id,),
        )
        removed_case_ids = [int(row["id"]) for row in cursor.fetchall()]
        if removed_case_ids:
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
        "[CSV_SYNC] version=%s new=%s changed=%s removed=%s rows=%s file=%s"
        % (
            version_id,
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
    )
