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


@dataclass(frozen=True)
class CasePayload:
    """Normalised case metadata extracted from a CSV row."""

    action_token_raw: str
    action_token_norm: str
    title: str
    subject: str
    court: str
    category: str
    judgment_date: str
    sort_judgment_date: str
    cause_number: str
    is_criminal: int
    source: str


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


def _save_csv_copy(content: bytes, sha256: str, *, source: str) -> Path:
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    short_sha = sha256[:8]
    csv_dir = config.DATA_DIR / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    path = csv_dir / f"{source}_{timestamp}_{short_sha}.csv"
    path.write_bytes(content)
    return path


def _validate_fieldnames(fieldnames: Optional[list[str]], *, source: str) -> None:
    """Ensure required columns are present in the CSV headers for the source."""

    if not fieldnames:
        raise ValueError("CSV is missing header row")

    normalized_source = sources.normalize_source(source)
    header_lower = {str(field).strip().lower() for field in fieldnames if field}

    if normalized_source == sources.PUBLIC_REGISTERS:
        if not {"name", "register", "register type"} & header_lower:
            raise ValueError("CSV missing required public_registers identifying columns")
        return

    if "actions" not in header_lower and "action" not in header_lower:
        raise ValueError("CSV missing required Actions column")


def _clean(value: Optional[str]) -> str:
    return (value or "").strip()


def _payloads_from_unreported_row(row: dict[str, str]) -> list[CasePayload]:
    actions_raw = _clean(row.get("Actions") or row.get("Action"))
    if not actions_raw:
        return []

    title = _clean(row.get("Title") or row.get("Case Title") or row.get("Subject"))
    subject = _clean(row.get("Subject") or title)
    court = _clean(row.get("Court") or row.get("Court file"))
    category = _clean(row.get("Category"))
    judgment_date = parse_judgment_date(row.get("Judgment Date") or row.get("Date") or "")
    cause_number = _clean(
        row.get("Cause Number")
        or row.get("Cause number")
        or row.get("Cause No.")
        or row.get("Cause")
    )

    tokens: list[str] = []
    for piece in re.split(r"[|,;/\\\s]+", actions_raw):
        norm = normalize_action_token(piece)
        if norm:
            tokens.append(norm)

    if not tokens:
        return []

    metadata = {
        "action_token_raw": actions_raw,
        "title": title,
        "subject": subject,
        "court": court,
        "category": category,
        "judgment_date": judgment_date,
        "cause_number": cause_number,
    }
    is_criminal = infer_is_criminal(metadata)

    payloads: list[CasePayload] = []
    for token in tokens:
        payloads.append(
            CasePayload(
                action_token_raw=actions_raw,
                action_token_norm=token,
                title=title,
                subject=subject,
                court=court,
                category=category,
                judgment_date=judgment_date,
                sort_judgment_date=judgment_date,
                cause_number=cause_number,
                is_criminal=is_criminal,
                source=sources.UNREPORTED_JUDGMENTS,
            )
        )
    return payloads


def _first_present(row: dict[str, str], keys: list[str]) -> str:
    for key in keys:
        candidate = row.get(key)
        cleaned = _clean(candidate)
        if cleaned:
            return cleaned
    return ""


def _payloads_from_public_registers_row(row: dict[str, str]) -> list[CasePayload]:
    register_type = _first_present(row, ["RegisterType", "Register Type", "Register", "Type"])
    name = _first_present(row, ["Name", "Full Name", "Person", "Entity", "Appointee"])
    reference = _first_present(
        row,
        [
            "Reference",
            "Ref",
            "Number",
            "Licence",
            "License",
            "Licence Number",
            "Registration",
            "Reg No",
            "Record",
        ],
    )
    date_raw = _first_present(
        row,
        [
            "Date",
            "Appointment Date",
            "Effective Date",
            "Start Date",
            "Registered Date",
        ],
    )

    if not (name or reference):
        log_line(
            "[CSV][WARN] public_registers row missing both name and reference; "
            "skipping entry."
        )
        return []

    if not reference:
        log_line(
            "[CSV][WARN] public_registers row missing reference/number; "
            "falling back to name-only token for action_token_norm."
        )
    if not date_raw:
        log_line(
            "[CSV][WARN] public_registers row missing date; "
            "judgment_date/sort_judgment_date will be empty for this entry."
        )

    token_raw_parts = [register_type, reference or name]
    token_raw = " ".join(part for part in token_raw_parts if part)
    token_norm = normalize_action_token(token_raw)
    if not token_norm:
        return []

    judgment_date = parse_judgment_date(date_raw)
    subject_parts = [register_type, reference, date_raw]
    subject = " - ".join(part for part in subject_parts if part)
    category = register_type or "Public Register"
    title = name or reference or register_type or token_norm

    payload = CasePayload(
        action_token_raw=token_raw,
        action_token_norm=token_norm,
        title=title,
        subject=subject or title,
        court="Public Register",
        category=category,
        judgment_date=judgment_date,
        sort_judgment_date=judgment_date,
        cause_number=reference,
        is_criminal=0,
        source=sources.PUBLIC_REGISTERS,
    )
    return [payload]


def _payloads_for_source(row: dict[str, str], source: str) -> list[CasePayload]:
    source_norm = sources.normalize_source(source)
    if source_norm == sources.PUBLIC_REGISTERS:
        return _payloads_from_public_registers_row(row)
    return _payloads_from_unreported_row(row)


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

    source_norm = sources.normalize_source(source)
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

    csv_path = _save_csv_copy(content, sha256, source=source_norm)
    rows: list[dict[str, str]] = []
    row_count = 0
    try:
        decoded = content.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(decoded))
        _validate_fieldnames(reader.fieldnames, source=source_norm)
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
            payloads = _payloads_for_source(row, source_norm)
            if not payloads:
                continue

            for payload in payloads:
                norm = payload.action_token_norm
                if not norm:
                    continue

                cursor = conn.execute(
                    """
                    SELECT * FROM cases
                    WHERE action_token_norm = ? AND source = ?
                    LIMIT 1
                    """,
                    (norm, source_norm),
                )
                existing = cursor.fetchone()

                fields = (
                    payload.title,
                    payload.subject,
                    payload.cause_number,
                    payload.court,
                    payload.category,
                    payload.judgment_date,
                    payload.sort_judgment_date,
                    payload.is_criminal,
                )

                if existing:
                    if (
                        existing["title"],
                        existing["subject"],
                        existing["cause_number"],
                        existing["court"],
                        existing["category"],
                        existing["judgment_date"],
                        existing["sort_judgment_date"],
                        existing["is_criminal"],
                    ) != fields:
                        conn.execute(
                            """
                            UPDATE cases
                            SET title = ?, subject = ?, cause_number = ?, court = ?, category = ?,
                                judgment_date = ?, sort_judgment_date = ?, is_criminal = ?,
                                last_seen_version_id = ?, is_active = 1
                            WHERE id = ?
                            """,
                            (
                                payload.title,
                                payload.subject,
                                payload.cause_number,
                                payload.court,
                                payload.category,
                                payload.judgment_date,
                                payload.sort_judgment_date,
                                payload.is_criminal,
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
                            action_token_raw, action_token_norm, title, subject, cause_number,
                            court, category, judgment_date, sort_judgment_date, is_criminal, is_active,
                            source, first_seen_version_id, last_seen_version_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                        """,
                        (
                            payload.action_token_raw,
                            norm,
                            payload.title,
                            payload.subject,
                            payload.cause_number,
                            payload.court,
                            payload.category,
                            payload.judgment_date,
                            payload.sort_judgment_date,
                            payload.is_criminal,
                            source_norm,
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
            (source_norm, version_id),
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
        source=source_norm,
    )
