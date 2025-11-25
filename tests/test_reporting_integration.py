from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from app.scraper import config, db


def _configure_temp_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "bailiikc.db"
    monkeypatch.setattr(config, "DATA_DIR", data_dir)
    monkeypatch.setattr(config, "PDF_DIR", data_dir / "pdfs")
    monkeypatch.setattr(config, "LOG_DIR", data_dir / "logs")
    monkeypatch.setattr(config, "DOWNLOADS_LOG", data_dir / "downloads.jsonl")
    monkeypatch.setattr(config, "SUMMARY_FILE", data_dir / "last_summary.json")
    monkeypatch.setattr(config, "LOG_FILE", data_dir / "logs" / "latest.log")
    monkeypatch.setattr(config, "DB_PATH", db_path)
    monkeypatch.setattr(db, "DB_PATH", db_path)


def _reload_main_module():
    if "app.main" in sys.modules:
        del sys.modules["app.main"]
    return importlib.import_module("app.main")


def test_get_download_rows_for_ui_json_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setenv("BAILIIKC_USE_DB_REPORTING", "0")

    downloads_log = config.DOWNLOADS_LOG
    downloads_log.parent.mkdir(parents=True, exist_ok=True)
    downloads_log.write_text(
        '{"actions_token": "json-token", "saved_path": "pdfs/json.pdf", "title": "JSON title", "bytes": 512}\n',
        encoding="utf-8",
    )

    main = _reload_main_module()

    rows = main._get_download_rows_for_ui()
    assert rows
    assert rows[0]["actions_token"] == "json-token"
    assert rows[0]["size_kb"] == pytest.approx(0.5)


def test_get_download_rows_for_ui_db_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setenv("BAILIIKC_USE_DB_REPORTING", "1")

    main = _reload_main_module()
    db.initialize_schema()

    csv_version_id = db.record_csv_version(
        fetched_at="2024-01-01T00:00:00Z",
        source_url="http://example.com/csv",
        sha256="abc123",
        row_count=1,
        file_path="judgments.csv",
    )
    run_id = db.create_run(
        trigger="test",
        mode="full",
        csv_version_id=csv_version_id,
        params_json="{}",
    )
    conn = db.get_connection()
    with conn:
        cursor = conn.execute(
            """
            INSERT INTO cases (
                action_token_raw, action_token_norm, title, cause_number, court, category,
                judgment_date, is_criminal, is_active, source, first_seen_version_id,
                last_seen_version_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, ?, ?, ?)
            """,
            (
                "RAW-DB",
                "NORM-DB",
                "DB title",
                "DB-123",
                "DB Court",
                "DB Category",
                "2024-02-01",
                "source-db",
                csv_version_id,
                csv_version_id,
            ),
        )
        case_id = cursor.lastrowid
        conn.execute(
            """
            INSERT INTO downloads (
                run_id, case_id, status, attempt_count, last_attempt_at, file_path,
                file_size_bytes, box_url_last, error_code, error_message, created_at,
                updated_at
            ) VALUES (?, ?, 'downloaded', 1, '2024-02-02T00:00:00Z', 'pdfs/db.pdf', 2048,
                'https://example.com/db', NULL, NULL, '2024-02-02T00:00:00Z',
                '2024-02-02T00:00:00Z')
            """,
            (run_id, case_id),
        )

    db.mark_run_completed(run_id)

    rows = main._get_download_rows_for_ui()
    assert rows
    row = rows[0]
    assert row["actions_token"] == "NORM-DB"
    assert row["saved_path"].endswith("db.pdf")
    assert row["size_kb"] == pytest.approx(2.0)
