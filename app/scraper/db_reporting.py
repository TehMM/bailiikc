from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from . import db
from .date_utils import sortable_date
from .utils import log_line


def get_latest_run_id() -> Optional[int]:
    """Return the ID of the most recent run (by started_at DESC), or None."""

    conn = db.get_connection()
    cursor = conn.execute("SELECT id FROM runs ORDER BY started_at DESC LIMIT 1")
    row = cursor.fetchone()
    return int(row["id"]) if row else None


class RunNotFoundError(Exception):
    """Raised when a requested run identifier does not exist."""


class CsvVersionNotFoundError(Exception):
    """Raised when a requested CSV version does not exist or is invalid."""



def get_run_summary(run_id: int) -> Optional[Dict[str, Any]]:
    """Return a summary dict for the given run_id, or None if not found."""

    conn = db.get_connection()
    cursor = conn.execute(
        """
        SELECT id, trigger, mode, csv_version_id, status, started_at, ended_at, error_summary
        FROM runs WHERE id = ?
        """,
        (run_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None

    return {
        "id": int(row["id"]),
        "trigger": row["trigger"],
        "mode": row["mode"],
        "csv_version_id": row["csv_version_id"],
        "status": row["status"],
        "started_at": row["started_at"],
        "ended_at": row["ended_at"],
        "error_summary": row["error_summary"],
    }



def get_downloaded_cases_for_run(run_id: int) -> List[Dict[str, Any]]:
    """Return successfully downloaded cases for the provided run identifier."""

    conn = db.get_connection()

    cursor = conn.execute("SELECT 1 FROM runs WHERE id = ? LIMIT 1", (run_id,))
    if cursor.fetchone() is None:
        raise RunNotFoundError(f"Run {run_id} not found")

    cursor = conn.execute(
        """
        SELECT
            d.id AS download_id,
            d.run_id,
            d.case_id,
            d.status,
            d.attempt_count,
            d.last_attempt_at,
            d.file_path,
            d.file_size_bytes,
            d.box_url_last,
            d.error_code,
            d.error_message,
            d.created_at,
            d.updated_at,
            c.action_token_raw,
            c.action_token_norm,
            c.title,
            c.cause_number,
            c.court,
            c.category,
            c.judgment_date,
            c.is_criminal,
            c.source
        FROM downloads d
        JOIN cases c ON c.id = d.case_id
        WHERE d.run_id = ?
          AND d.status = 'downloaded'
        ORDER BY d.id ASC
        """,
        (run_id,),
    )

    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_run_download_stats(run_id: int) -> Dict[str, int]:
    """Return aggregate download counts for a given run.

    The keys are:

    - "total": total downloads rows for this run (all statuses)
    - "downloaded": number of rows with status="downloaded"
    - "failed": number of rows with status="failed"
    - "skipped": number of rows with status="skipped"
    - "pending": number of rows with status="pending"
    - "in_progress": number of rows with status="in_progress"

    Additional statuses (if present) are counted only toward "total". This is
    used to back ``/api/runs/latest`` and any UI that shows run-level summary
    stats.
    """

    conn = db.get_connection()
    cursor = conn.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM downloads
        WHERE run_id = ?
        GROUP BY status
        """,
        (run_id,),
    )

    totals: Dict[str, int] = {
        "total": 0,
        "downloaded": 0,
        "failed": 0,
        "skipped": 0,
        "pending": 0,
        "in_progress": 0,
    }

    for row in cursor.fetchall():
        status = row["status"]
        count = int(row["count"])
        totals["total"] += count
        if status in totals:
            totals[status] += count

    return totals


def get_download_rows_for_run(
    run_id: Optional[int] = None, status_filter: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Return download rows for the given run, optionally filtered by status."""

    resolved_run_id = run_id or get_latest_run_id()
    if resolved_run_id is None:
        log_line("[DB_REPORTING] No runs found when building download rows")
        return []

    conn = db.get_connection()
    query = [
        """
        SELECT
            d.run_id,
            d.status,
            d.last_attempt_at,
            d.file_path,
            d.file_size_bytes,
            d.box_url_last,
            c.action_token_raw,
            c.action_token_norm,
            c.title,
            c.cause_number,
            c.court,
            c.category,
            c.judgment_date,
            c.is_criminal,
            c.source
        FROM downloads d
        JOIN cases c ON d.case_id = c.id
        WHERE d.run_id = ?
        """
    ]
    params: list[Any] = [resolved_run_id]

    if status_filter:
        query.append("AND d.status = ?")
        params.append(status_filter)

    query.append("ORDER BY d.id ASC")

    cursor = conn.execute("\n".join(query), params)
    rows: List[Dict[str, Any]] = []

    for row in cursor.fetchall():
        saved_path = row["file_path"] or ""
        judgment_date = row["judgment_date"] or ""
        actions_token = row["action_token_norm"] or row["action_token_raw"] or ""
        title = row["title"] or actions_token or saved_path
        filename = Path(saved_path).name if saved_path else ""
        file_size_bytes = row["file_size_bytes"]
        if file_size_bytes:
            try:
                size_kb = round(file_size_bytes / 1024.0, 1)
            except TypeError:
                size_kb = 0
        else:
            size_kb = 0

        rows.append(
            {
                "actions_token": actions_token,
                "title": title,
                "subject": row["title"] or "",
                "court": row["court"] or "",
                "category": row["category"] or "",
                "judgment_date": judgment_date,
                "sort_judgment_date": sortable_date(str(judgment_date)),
                "cause_number": row["cause_number"] or "",
                "downloaded_at": row["last_attempt_at"] or "",
                "saved_path": saved_path,
                "filename": filename,
                "size_kb": size_kb,
            }
        )

    return rows


def get_case_diff_for_csv_version(version_id: int) -> Dict[str, Any]:
    """Return new and removed cases for the given CSV version.

    The result is derived solely from the ``cases`` table invariants:

    - New: cases where ``first_seen_version_id == version_id``.
    - Removed: cases where ``last_seen_version_id == version_id`` and
      ``is_active == 0``.

    Only ``source = 'unreported_judgments'`` rows are included.
    """

    conn = db.get_connection()

    cursor = conn.execute(
        "SELECT id, valid FROM csv_versions WHERE id = ? LIMIT 1", (version_id,)
    )
    row = cursor.fetchone()
    if row is None or int(row["valid"]) == 0:
        raise CsvVersionNotFoundError(
            f"CSV version {version_id} not found or invalid"
        )

    new_cursor = conn.execute(
        """
        SELECT
            id,
            action_token_norm,
            title,
            cause_number,
            court,
            category,
            judgment_date,
            is_criminal,
            is_active,
            source,
            first_seen_version_id,
            last_seen_version_id
        FROM cases
        WHERE source = 'unreported_judgments'
          AND first_seen_version_id = ?
        ORDER BY id ASC
        """,
        (version_id,),
    )
    new_columns = [col[0] for col in new_cursor.description]
    new_cases = [dict(zip(new_columns, row)) for row in new_cursor.fetchall()]

    removed_cursor = conn.execute(
        """
        SELECT
            id,
            action_token_norm,
            title,
            cause_number,
            court,
            category,
            judgment_date,
            is_criminal,
            is_active,
            source,
            first_seen_version_id,
            last_seen_version_id
        FROM cases
        WHERE source = 'unreported_judgments'
          AND last_seen_version_id = ?
          AND is_active = 0
        ORDER BY id ASC
        """,
        (version_id,),
    )
    removed_columns = [col[0] for col in removed_cursor.description]
    removed_cases = [dict(zip(removed_columns, row)) for row in removed_cursor.fetchall()]

    return {
        "csv_version_id": version_id,
        "new_cases": new_cases,
        "removed_cases": removed_cases,
        "new_count": len(new_cases),
        "removed_count": len(removed_cases),
    }
