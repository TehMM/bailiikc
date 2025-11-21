"""SQLite helpers for the bailiikc scraper.

This module defines the project database path, connection helper, schema
initialisation, and early helper functions for CSV version tracking. It is not
yet wired into the main scraping pipeline; future PRs will integrate these
tables while preserving current scraper behaviour.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, Optional

from . import config

DB_PATH: Path = config.DB_PATH


def get_connection() -> sqlite3.Connection:
    """Return a SQLite connection to the project database.

    The parent directory is created if missing and ``check_same_thread`` is
    disabled to allow reuse from different threads. Callers must manage
    concurrency at a higher layer.
    """

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_schema() -> None:
    """Create the baseline tables if they do not yet exist.

    Safe to call multiple times; each statement uses ``IF NOT EXISTS`` to avoid
    duplicate objects.
    """

    statements: Iterable[str] = (
        """
        CREATE TABLE IF NOT EXISTS csv_versions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at      TEXT NOT NULL,
            source_url      TEXT NOT NULL,
            etag            TEXT,
            last_modified   TEXT,
            sha256          TEXT NOT NULL,
            row_count       INTEGER NOT NULL,
            valid           INTEGER NOT NULL,
            error_message   TEXT,
            file_path       TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS cases (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            action_token_raw      TEXT NOT NULL,
            action_token_norm     TEXT NOT NULL,
            title                 TEXT,
            cause_number          TEXT,
            court                 TEXT,
            category              TEXT,
            judgment_date         TEXT,
            is_criminal           INTEGER NOT NULL DEFAULT 0,
            is_active             INTEGER NOT NULL DEFAULT 1,
            source                TEXT NOT NULL,
            first_seen_version_id INTEGER NOT NULL,
            last_seen_version_id  INTEGER NOT NULL,
            FOREIGN KEY(first_seen_version_id) REFERENCES csv_versions(id),
            FOREIGN KEY(last_seen_version_id) REFERENCES csv_versions(id)
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_cases_token_norm
            ON cases(action_token_norm);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_cases_source
            ON cases(source);
        """,
        """
        CREATE TABLE IF NOT EXISTS runs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at      TEXT NOT NULL,
            ended_at        TEXT,
            trigger         TEXT NOT NULL,
            mode            TEXT NOT NULL,
            csv_version_id  INTEGER NOT NULL,
            params_json     TEXT NOT NULL,
            status          TEXT NOT NULL,
            error_summary   TEXT,
            FOREIGN KEY(csv_version_id) REFERENCES csv_versions(id)
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_runs_started_at
            ON runs(started_at DESC);
        """,
        """
        CREATE TABLE IF NOT EXISTS downloads (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            case_id         INTEGER NOT NULL,
            status          TEXT NOT NULL,
            attempt_count   INTEGER NOT NULL DEFAULT 0,
            last_attempt_at TEXT,
            file_path       TEXT,
            file_size_bytes INTEGER,
            box_url_last    TEXT,
            error_code      TEXT,
            error_message   TEXT,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES runs(id),
            FOREIGN KEY(case_id) REFERENCES cases(id)
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_downloads_run_case
            ON downloads(run_id, case_id);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_downloads_status
            ON downloads(status);
        """,
        """
        CREATE TABLE IF NOT EXISTS events (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id       INTEGER,
            case_id      INTEGER,
            event_type   TEXT NOT NULL,
            payload_json TEXT,
            created_at   TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES runs(id),
            FOREIGN KEY(case_id) REFERENCES cases(id)
        );
        """,
    )

    conn = get_connection()
    with conn:
        for statement in statements:
            conn.execute(statement)


def record_csv_version(
    *,
    fetched_at: str,
    source_url: str,
    sha256: str,
    row_count: int,
    file_path: str,
    valid: bool = True,
    etag: Optional[str] = None,
    last_modified: Optional[str] = None,
    error_message: Optional[str] = None,
) -> int:
    """Insert a csv_versions row and return its identifier.

    The ``valid`` flag is stored as an integer for SQLite compatibility.
    """

    conn = get_connection()
    with conn:
        cursor = conn.execute(
            """
            INSERT INTO csv_versions (
                fetched_at, source_url, etag, last_modified,
                sha256, row_count, valid, error_message, file_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fetched_at,
                source_url,
                etag,
                last_modified,
                sha256,
                row_count,
                1 if valid else 0,
                error_message,
                file_path,
            ),
        )
        return int(cursor.lastrowid)


def get_latest_valid_csv_version() -> Optional[sqlite3.Row]:
    """Return the most recent valid csv_versions row, if any."""

    conn = get_connection()
    cursor = conn.execute(
        "SELECT * FROM csv_versions WHERE valid = 1 ORDER BY id DESC LIMIT 1"
    )
    return cursor.fetchone()


def get_latest_csv_version() -> Optional[sqlite3.Row]:
    """Return the newest csv_versions row regardless of validity."""

    conn = get_connection()
    cursor = conn.execute("SELECT * FROM csv_versions ORDER BY id DESC LIMIT 1")
    return cursor.fetchone()
