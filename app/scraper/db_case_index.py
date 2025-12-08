from __future__ import annotations

"""Helpers for loading case index data from SQLite."""

import sqlite3
from typing import Any, Dict, Optional

from . import db, sources
from .utils import log_line


def load_case_index_from_db(
    *,
    source: str = sources.DEFAULT_SOURCE,
    only_active: bool = True,
    csv_version_id: Optional[int] = None,
) -> Dict[str, Dict[str, Any]]:
    """Return a mapping of action_token_norm to case metadata from SQLite."""

    conn = db.get_connection()
    conn.row_factory = sqlite3.Row

    clauses = ["source = ?"]
    params: list[Any] = [source]

    if csv_version_id is not None:
        clauses.append("first_seen_version_id <= ? AND last_seen_version_id >= ?")
        params.extend([csv_version_id, csv_version_id])
    if only_active:
        clauses.append("is_active = 1")

    cursor = conn.execute(
        """
        SELECT
            action_token_raw,
            action_token_norm,
            title,
            cause_number,
            court,
            category,
            judgment_date,
            sort_judgment_date,
            subject,
            is_criminal,
            source,
            is_active
        FROM cases
        WHERE {where_clause}
        """.format(where_clause=" AND ".join(clauses)),
        params,
    )

    records: Dict[str, Dict[str, Any]] = {}
    for row in cursor.fetchall():
        token_norm = (row["action_token_norm"] or "").strip()
        if not token_norm:
            continue

        if token_norm in records:
            log_line(
                f"[DB] Duplicate action_token_norm {token_norm} encountered; keeping first occurrence."
            )
            continue

        records[token_norm] = {
            "action_token_raw": (row["action_token_raw"] or "").strip(),
            "action_token_norm": token_norm,
            "title": (row["title"] or "").strip(),
            "subject": (row["subject"] or "").strip(),
            "cause_number": (row["cause_number"] or "").strip(),
            "court": (row["court"] or "").strip(),
            "category": (row["category"] or "").strip(),
            "judgment_date": (row["judgment_date"] or "").strip(),
            "sort_judgment_date": (row["sort_judgment_date"] or "").strip(),
            "is_criminal": row["is_criminal"],
            "source": (row["source"] or "").strip(),
            "is_active": row["is_active"],
        }

    return records
