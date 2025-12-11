from __future__ import annotations

from pathlib import Path

import pytest

from app.scraper import db, sources
from tests.test_runs_api_db import _configure_temp_paths, _reload_main_module


def _record_version() -> int:
    return db.record_csv_version(
        fetched_at="2024-01-01T00:00:00Z",
        source_url="http://example.com/csv",
        sha256="abc123",
        row_count=1,
        file_path="judgments.csv",
    )


def _insert_case(
    conn, csv_version_id: int, token_suffix: str, source: str | None
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO cases (
            action_token_raw, action_token_norm, title, cause_number, court, category,
            judgment_date, is_criminal, is_active, source, first_seen_version_id,
            last_seen_version_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, ?, ?, ?)
        """,
        (
            f"RAW-{token_suffix}",
            f"NORM-{token_suffix}",
            f"Title {token_suffix}",
            f"Cause {token_suffix}",
            "Court",
            "Category",
            "2024-01-01",
            source,
            csv_version_id,
            csv_version_id,
        ),
    )
    return int(cursor.lastrowid)


def _insert_download(
    conn, *, run_id: int, case_id: int, file_path: str = "pdfs/dummy.pdf"
) -> None:
    ts = "2024-01-02T00:00:00Z"
    conn.execute(
        """
        INSERT INTO downloads (
            run_id, case_id, status, attempt_count, last_attempt_at, file_path,
            file_size_bytes, box_url_last, error_code, error_message, created_at,
            updated_at
        ) VALUES (?, ?, 'downloaded', 1, ?, ?, 2048, NULL, NULL, NULL, ?, ?)
        """,
        (run_id, case_id, ts, file_path, ts, ts),
    )


def test_api_downloaded_cases_includes_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    # Ensure we exercise the DB-backed reporting path rather than JSONL.
    monkeypatch.setenv("BAILIIKC_USE_DB_REPORTING", "1")
    db.initialize_schema()

    conn = db.get_connection()
    with conn:
        csv_version_id = _record_version()
        run_id = db.create_run(
            trigger="ui",
            mode="full",
            csv_version_id=csv_version_id,
            params_json="{}",
        )
        uj_case = _insert_case(conn, csv_version_id, "UJ", sources.UNREPORTED_JUDGMENTS)
        pr_case = _insert_case(conn, csv_version_id, "PR", sources.PUBLIC_REGISTERS)
        unknown_case = _insert_case(conn, csv_version_id, "UNK", "")

        _insert_download(conn, run_id=run_id, case_id=uj_case, file_path="pdfs/uj.pdf")
        _insert_download(conn, run_id=run_id, case_id=pr_case, file_path="pdfs/pr.pdf")
        _insert_download(
            conn, run_id=run_id, case_id=unknown_case, file_path="pdfs/unknown.pdf"
        )

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get("/api/downloaded-cases")
    assert resp.status_code == 200

    payload = resp.get_json()
    assert isinstance(payload["data"], list)
    assert payload["data"], "Expected at least one download row"

    sources_by_token = {row["actions_token"]: row["source"] for row in payload["data"]}

    assert sources_by_token["NORM-UJ"] == sources.UNREPORTED_JUDGMENTS
    assert sources_by_token["NORM-PR"] == sources.PUBLIC_REGISTERS
    assert sources_by_token["NORM-UNK"] == sources.DEFAULT_SOURCE
    assert all("source" in row for row in payload["data"])
