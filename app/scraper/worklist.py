from __future__ import annotations

from dataclasses import dataclass
from typing import List

from . import config, db
from .utils import log_line

DEFAULT_SOURCE = "unreported_judgments"


@dataclass(frozen=True)
class WorkItem:
    """Represents a single case to be processed in a scrape run.

    This is derived from the SQLite `cases` table and is intentionally small:
    it carries enough information for logging/debugging and future integration
    with the Playwright pipeline, but it does not embed any runtime state.
    """

    case_id: int
    action_token_norm: str
    action_token_raw: str
    title: str
    court: str
    category: str
    judgment_date: str
    cause_number: str
    is_criminal: bool
    is_active: bool
    first_seen_version_id: int
    last_seen_version_id: int
    source: str


def _row_to_work_item(row) -> WorkItem:
    """Convert a `cases` row into a WorkItem.

    Callers must ensure the row includes all required columns.
    """

    return WorkItem(
        case_id=int(row["id"]),
        action_token_norm=(row["action_token_norm"] or "").strip(),
        action_token_raw=(row["action_token_raw"] or "").strip(),
        title=(row["title"] or "").strip(),
        court=(row["court"] or "").strip(),
        category=(row["category"] or "").strip(),
        judgment_date=(row["judgment_date"] or "").strip(),
        cause_number=(row["cause_number"] or "").strip(),
        is_criminal=bool(row["is_criminal"]),
        is_active=bool(row["is_active"]),
        first_seen_version_id=int(row["first_seen_version_id"]),
        last_seen_version_id=int(row["last_seen_version_id"]),
        source=(row["source"] or "").strip(),
    )


def build_full_worklist(
    csv_version_id: int,
    *,
    source: str = DEFAULT_SOURCE,
) -> List[WorkItem]:
    """Return all active, non-criminal cases for the given CSV version.

    This derives its data from the SQLite `cases` table only.
    """

    conn = db.get_connection()
    cursor = conn.execute(
        """
        SELECT
            id,
            action_token_raw,
            action_token_norm,
            title,
            court,
            category,
            judgment_date,
            cause_number,
            is_criminal,
            is_active,
            first_seen_version_id,
            last_seen_version_id,
            source
        FROM cases
        WHERE source = ?
          AND is_active = 1
          AND is_criminal = 0
          AND last_seen_version_id = ?
        ORDER BY action_token_norm ASC, id ASC
        """,
        (source, csv_version_id),
    )

    rows = [_row_to_work_item(row) for row in cursor.fetchall()]
    log_line(
        f"[WORKLIST] full mode: csv_version_id={csv_version_id} source={source} count={len(rows)}"
    )
    return rows


def build_new_worklist(
    csv_version_id: int,
    *,
    source: str = DEFAULT_SOURCE,
) -> List[WorkItem]:
    """Return new active, non-criminal cases for the given CSV version.

    "New" is defined as `first_seen_version_id == csv_version_id`.
    """

    conn = db.get_connection()
    cursor = conn.execute(
        """
        SELECT
            id,
            action_token_raw,
            action_token_norm,
            title,
            court,
            category,
            judgment_date,
            cause_number,
            is_criminal,
            is_active,
            first_seen_version_id,
            last_seen_version_id,
            source
        FROM cases
        WHERE source = ?
          AND is_active = 1
          AND is_criminal = 0
          AND first_seen_version_id = ?
        ORDER BY action_token_norm ASC, id ASC
        """,
        (source, csv_version_id),
    )

    rows = [_row_to_work_item(row) for row in cursor.fetchall()]
    log_line(
        f"[WORKLIST] new mode: csv_version_id={csv_version_id} source={source} count={len(rows)}"
    )
    return rows


def build_worklist(
    mode: str,
    csv_version_id: int,
    *,
    source: str = DEFAULT_SOURCE,
) -> List[WorkItem]:
    """Build a per-case worklist for the given mode and CSV version.

    Supported modes:
      - "full": all active, non-criminal cases for the version.
      - "new": cases whose first_seen_version_id == csv_version_id.
      - "resume": not yet implemented (raises NotImplementedError).

    This function does NOT alter any scraper behaviour yet; it is not wired
    into `run.py` in this PR.
    """

    normalized = (mode or "").strip().lower()

    if config.is_full_mode(normalized):
        return build_full_worklist(csv_version_id, source=source)

    if config.is_new_mode(normalized):
        return build_new_worklist(csv_version_id, source=source)

    if normalized == "resume":
        raise NotImplementedError(
            "DB-backed resume worklists are planned for PR19, not PR16."
        )

    raise ValueError(
        f"Unsupported worklist mode {mode!r}; expected 'full', 'new', or 'resume'."
    )


__all__ = [
    "WorkItem",
    "build_full_worklist",
    "build_new_worklist",
    "build_worklist",
]
