from __future__ import annotations

"""Helpers for loading case index data from SQLite."""

import sqlite3
from typing import Any, Dict

from . import db, sources
from .utils import log_line


def load_case_index_from_db(
    *, source: str = sources.DEFAULT_SOURCE, only_active: bool = True
) -> Dict[str, Dict[str, Any]]:
    """Return a mapping of action_token_norm to case metadata from SQLite."""

    conn = db.get_connection()
    conn.row_factory = sqlite3.Row

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
            is_criminal,
            source,
            is_active
        FROM cases
        WHERE source = ?
          AND (? = 0 OR is_active = 1)
        """,
        (source, 1 if only_active else 0),
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
            "cause_number": (row["cause_number"] or "").strip(),
            "court": (row["court"] or "").strip(),
            "category": (row["category"] or "").strip(),
            "judgment_date": (row["judgment_date"] or "").strip(),
            "is_criminal": row["is_criminal"],
            "source": (row["source"] or "").strip(),
            "is_active": row["is_active"],
        }

    return records
