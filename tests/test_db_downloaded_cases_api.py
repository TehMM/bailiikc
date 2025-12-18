from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.scraper import db, db_reporting
from tests.test_runs_api_db import (
    _configure_temp_paths,
    _insert_case,
    _reload_main_module,
)


def _seed_downloaded_case(csv_version_id: int, run_id: int) -> int:
    conn = db.get_connection()
    with conn:
        case_id = _insert_case(conn, csv_version_id, "DL-CASE")
        ts = "2024-01-02T00:00:00Z"
        conn.execute(
            """
            INSERT INTO downloads (
                run_id, case_id, status, attempt_count, last_attempt_at, file_path,
                file_size_bytes, box_url_last, error_code, error_message, created_at,
                updated_at
            ) VALUES (?, ?, 'downloaded', 1, ?, ?, ?, NULL, NULL, NULL, ?, ?)
            """,
            (run_id, case_id, ts, "pdfs/dummy.pdf", 2048, ts, ts),
        )
    return case_id


def _create_run_with_version() -> tuple[int, int]:
    csv_version_id = db.record_csv_version(
        fetched_at="2024-01-01T00:00:00Z",
        source_url="http://example.com/csv",
        sha256="abc123",
        row_count=1,
        file_path="judgments.csv",
    )
    return db.create_run(
        trigger="test",
        mode="full",
        csv_version_id=csv_version_id,
        params_json="{}",
    ), csv_version_id


def _insert_case_with_source(conn, csv_version_id: int, *, source: str, token_suffix: str) -> int:
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


def test_get_downloaded_cases_for_run_returns_joined_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    run_id, csv_version_id = _create_run_with_version()
    case_id = _seed_downloaded_case(csv_version_id, run_id)

    rows = db_reporting.get_downloaded_cases_for_run(run_id)
    assert len(rows) == 1

    row = rows[0]
    assert row["run_id"] == run_id
    assert row["case_id"] == case_id
    assert row["status"] == "downloaded"
    assert row["file_path"].endswith("dummy.pdf")
    assert row["action_token_norm"].startswith("NORM-")


def test_get_downloaded_cases_for_run_missing_run_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    with pytest.raises(db_reporting.RunNotFoundError):
        db_reporting.get_downloaded_cases_for_run(999)


def test_api_db_downloaded_cases_for_run_returns_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    run_id, csv_version_id = _create_run_with_version()
    case_id = _seed_downloaded_case(csv_version_id, run_id)

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get(f"/api/db/runs/{run_id}/downloaded-cases")
    assert resp.status_code == 200

    payload = resp.get_json()
    assert payload["ok"] is True
    assert payload["run_id"] == run_id
    assert payload["count"] == 1
    assert payload["downloads"][0]["case_id"] == case_id


def test_api_db_downloaded_cases_for_run_missing_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get("/api/db/runs/999/downloaded-cases")
    assert resp.status_code == 404
    payload = resp.get_json()
    assert payload["ok"] is False
    assert payload["error"] == "run_not_found"


def test_api_db_downloaded_cases_filters_by_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setenv("BAILIIKC_USE_DB_REPORTING", "1")
    db.initialize_schema()

    conn = db.get_connection()
    with conn:
        csv_version_id = db.record_csv_version(
            fetched_at="2024-01-01T00:00:00Z",
            source_url="http://example.com/csv",
            sha256="abc123",
            row_count=2,
            file_path="judgments.csv",
        )
        run_id = db.create_run(
            trigger="test",
            mode="new",
            csv_version_id=csv_version_id,
            params_json=json.dumps({"target_source": "public_registers"}),
        )

        pr_case = _insert_case_with_source(
            conn, csv_version_id, source="public_registers", token_suffix="PR"
        )
        uj_case = _insert_case_with_source(
            conn, csv_version_id, source="unreported_judgments", token_suffix="UJ"
        )

        ts = "2024-01-02T00:00:00Z"
        for case_id in (pr_case, uj_case):
            conn.execute(
                """
                INSERT INTO downloads (
                    run_id, case_id, status, attempt_count, last_attempt_at, file_path,
                    file_size_bytes, box_url_last, error_code, error_message, created_at,
                    updated_at
                ) VALUES (?, ?, 'downloaded', 1, ?, 'pdfs/dummy.pdf', 100, NULL, NULL, NULL, ?, ?)
                """,
                (run_id, case_id, ts, ts, ts),
            )

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get(
        f"/api/db/downloaded-cases?run_id={run_id}&source=public_registers&status=downloaded"
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert len(payload["data"]) == 1
    assert payload["data"][0]["source"] == "public_registers"
