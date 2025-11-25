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


def _seed_db_with_download(csv_version_id: int) -> None:
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
                action_token_raw,
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, ?, ?, ?)
            """,
            (
                "RAW-SEED",
                "TOK-SEED",
                "Seed Title",
                "CASE-001",
                "Seed Court",
                "Seed Category",
                "2024-03-01",
                "unreported_judgments",
                csv_version_id,
                csv_version_id,
            ),
        )
        case_id = cursor.lastrowid
        conn.execute(
            """
            INSERT INTO downloads (
                run_id,
                case_id,
                status,
                attempt_count,
                last_attempt_at,
                file_path,
                file_size_bytes,
                box_url_last,
                error_code,
                error_message,
                created_at,
                updated_at
            ) VALUES (
                ?, ?, 'downloaded', 1, '2024-03-02T00:00:00Z', 'pdfs/seed.pdf', 2048,
                'https://example.com/db', NULL, NULL, '2024-03-02T00:00:00Z',
                '2024-03-02T00:00:00Z'
            )
            """,
            (run_id, case_id),
        )

    db.mark_run_completed(run_id)


def _seed_json_downloads(downloads_log: Path) -> None:
    downloads_log.parent.mkdir(parents=True, exist_ok=True)
    downloads_log.write_text(
        (
            '{"actions_token": "TOK-SEED", "title": "Seed Title", "subject": "Seed Title", '
            '"court": "Seed Court", "category": "Seed Category", "judgment_date": "2024-03-01", '
            '"cause_number": "CASE-001", "downloaded_at": "2024-03-02T00:00:00Z", '
            '"saved_path": "pdfs/seed.pdf", "bytes": 2048}\n'
        ),
        encoding="utf-8",
    )


def test_downloaded_cases_endpoint_matches_json_and_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    csv_version_id = db.record_csv_version(
        fetched_at="2024-03-01T00:00:00Z",
        source_url="http://example.com/csv",
        sha256="abc123",
        row_count=1,
        file_path="judgments.csv",
    )
    _seed_db_with_download(csv_version_id)
    _seed_json_downloads(config.DOWNLOADS_LOG)

    monkeypatch.setenv("BAILIIKC_USE_DB_REPORTING", "0")
    main = _reload_main_module()
    client = main.app.test_client()

    resp_legacy = client.get("/api/downloaded-cases")
    assert resp_legacy.status_code == 200
    payload_legacy = resp_legacy.get_json()

    monkeypatch.setenv("BAILIIKC_USE_DB_REPORTING", "1")
    main = _reload_main_module()
    client = main.app.test_client()
    resp_db = client.get("/api/downloaded-cases")
    assert resp_db.status_code == 200
    payload_db = resp_db.get_json()

    assert set(payload_legacy.keys()) == set(payload_db.keys()) == {"data"}
    assert len(payload_legacy["data"]) == len(payload_db["data"]) == 1

    row_legacy = payload_legacy["data"][0]
    row_db = payload_db["data"][0]
    for field in (
        "actions_token",
        "title",
        "subject",
        "court",
        "category",
        "judgment_date",
        "sort_judgment_date",
        "cause_number",
        "downloaded_at",
        "saved_path",
        "filename",
    ):
        assert row_legacy[field] == row_db[field]

    assert row_legacy["size_kb"] == pytest.approx(row_db["size_kb"])


def test_report_renders_in_both_reporting_modes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    csv_version_id = db.record_csv_version(
        fetched_at="2024-03-01T00:00:00Z",
        source_url="http://example.com/csv",
        sha256="abc123",
        row_count=1,
        file_path="judgments.csv",
    )
    _seed_db_with_download(csv_version_id)
    _seed_json_downloads(config.DOWNLOADS_LOG)

    monkeypatch.setenv("BAILIIKC_USE_DB_REPORTING", "0")
    main = _reload_main_module()
    client = main.app.test_client()
    resp_legacy = client.get("/report")
    assert resp_legacy.status_code == 200
    html_legacy = resp_legacy.get_data(as_text=True)
    assert "<strong>Downloaded Cases:</strong> 1" in html_legacy

    monkeypatch.setenv("BAILIIKC_USE_DB_REPORTING", "1")
    main = _reload_main_module()
    client = main.app.test_client()
    resp_db = client.get("/report")
    assert resp_db.status_code == 200
    html_db = resp_db.get_data(as_text=True)
    assert "<strong>Downloaded Cases:</strong> 1" in html_db

    assert "/api/downloaded-cases" in html_legacy
    assert "/api/downloaded-cases" in html_db
