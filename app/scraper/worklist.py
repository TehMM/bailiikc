from __future__ import annotations

import json
from dataclasses import dataclass
from typing import List, Optional

from . import config, db, sources
from .utils import log_line

DEFAULT_SOURCE = sources.DEFAULT_SOURCE


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


def build_resume_worklist_for_run(
    run_id: int,
    *,
    source: str = DEFAULT_SOURCE,
) -> List[WorkItem]:
    """Return cases that should be retried for a given run.

    Semantics:
      - Only consider downloads for the provided ``run_id``.
      - Include cases whose ``downloads.status`` is one of ``pending``,
        ``failed``, or ``in_progress`` (treating in-progress rows as needing a
        retry on the next attempt).
      - Join against ``cases`` with ``source`` match, ``is_active = 1``, and
        ``is_criminal = 0``.
      - If multiple downloads exist for the same case, collapse to a single
        ``WorkItem`` ordered by most recent ``updated_at``.
      - If the ``run_id`` does not exist, return an empty list and log.
    """

    conn = db.get_connection()

    cursor = conn.execute("SELECT 1 FROM runs WHERE id = ? LIMIT 1", (run_id,))
    if cursor.fetchone() is None:
        log_line(
            f"[WORKLIST] resume mode: run_id={run_id} source={source} not found"
        )
        return []

    # ``cases`` has one row per id; ``GROUP BY`` only collapses multiple download
    # attempts for the same case so we return a single WorkItem each.
    cursor = conn.execute(
        """
        SELECT
            c.id AS id,
            c.action_token_raw,
            c.action_token_norm,
            c.title,
            c.court,
            c.category,
            c.judgment_date,
            c.cause_number,
            c.is_criminal,
            c.is_active,
            c.first_seen_version_id,
            c.last_seen_version_id,
            c.source
        FROM downloads d
        JOIN cases c ON c.id = d.case_id
        WHERE d.run_id = ?
          AND d.status IN ('pending', 'failed', 'in_progress')
          AND c.source = ?
          AND c.is_active = 1
          AND c.is_criminal = 0
        GROUP BY c.id
        ORDER BY MAX(d.updated_at) DESC, c.id DESC
        """,
        (run_id, source),
    )

    rows = [_row_to_work_item(row) for row in cursor.fetchall()]
    log_line(
        f"[WORKLIST] resume mode: run_id={run_id} source={source} count={len(rows)}"
    )
    return rows


def _select_run_for_resume(
    csv_version_id: int,
    *,
    source: str = DEFAULT_SOURCE,
) -> Optional[int]:
    """Return the run identifier to resume for a CSV version.

    Baseline semantics:
      - Select the most recent run for ``csv_version_id`` whose status is not
        ``completed`` (including currently running entries, so a stalled run
        can be retried).
      - Prefer runs with ``mode = 'resume'`` if present by ordering on
        ``started_at``; otherwise accept ``new``/``full`` runs.
      - Only consider runs whose planned source matches the requested source
        (encoded in ``params_json`` when available). If the params do not
        encode a source, assume it matches.
      - Return ``None`` when no suitable run exists.
    """

    conn = db.get_connection()

    cursor = conn.execute(
        """
        SELECT id, params_json
        FROM runs
        WHERE csv_version_id = ?
          AND status != 'completed'
        ORDER BY (mode = 'resume') DESC, started_at DESC
        LIMIT 20
        """,
        (csv_version_id,),
    )

    for row in cursor.fetchall():
        params_json = row["params_json"] or ""
        run_params = {}
        if params_json:
            try:
                loaded = json.loads(params_json)
                if isinstance(loaded, dict):
                    run_params = loaded
            except json.JSONDecodeError:
                log_line(
                    "[WORKLIST] resume mode: could not decode params_json; assuming source matches"
                )
                run_params = {}

        param_source = run_params.get("target_source") or run_params.get("source")
        if param_source and source and str(param_source) != source:
            continue
        return int(row["id"])

    return None


def build_resume_worklist(
    csv_version_id: int,
    *,
    source: str = DEFAULT_SOURCE,
) -> List[WorkItem]:
    """Return a resume worklist for the most relevant prior run.

    If no suitable run is found, an empty list is returned and a log line is
    emitted.
    """

    run_id = _select_run_for_resume(csv_version_id, source=source)
    if run_id is None:
        log_line(
            f"[WORKLIST] resume mode: no prior run found for csv_version_id={csv_version_id}"
        )
        return []
    return build_resume_worklist_for_run(run_id, source=source)


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
      - "resume": cases to retry based on runs/downloads status codes.

    This function does NOT alter any scraper behaviour yet; it is not wired
    into `run.py` in this PR.
    """

    normalized = (mode or "").strip().lower()

    if config.is_full_mode(normalized):
        return build_full_worklist(csv_version_id, source=source)

    if config.is_new_mode(normalized):
        return build_new_worklist(csv_version_id, source=source)

    if normalized == "resume":
        return build_resume_worklist(csv_version_id, source=source)

    raise ValueError(
        f"Unsupported worklist mode {mode!r}; expected 'full', 'new', or 'resume'."
    )


__all__ = [
    "WorkItem",
    "build_full_worklist",
    "build_new_worklist",
    "build_resume_worklist",
    "build_resume_worklist_for_run",
    "build_worklist",
]
