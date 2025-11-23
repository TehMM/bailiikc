from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.scraper import config, db
from app.scraper.consistency import compare_latest_downloads_json_vs_db


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


def _seed_json_downloads(downloads_log: Path, *, title: str) -> None:
    downloads_log.parent.mkdir(parents=True, exist_ok=True)
    downloads_log.write_text(
        (
            '{"actions_token": "TOK-SEED", "title": "'
            + title
            + '", "subject": "'
            + title
            + '", "court": "Seed Court", "category": "Seed Category", "judgment_date": "2024-03-01", '
            '"cause_number": "CASE-001", "downloaded_at": "2024-03-02T00:00:00Z", '
            '"saved_path": "pdfs/seed.pdf", "bytes": 2048}\n'
        ),
        encoding="utf-8",
    )


def _setup_db_and_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, title: str) -> None:
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
    _seed_json_downloads(config.DOWNLOADS_LOG, title=title)


def test_consistency_checker_reports_match(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _setup_db_and_json(tmp_path, monkeypatch, title="Seed Title")

    report = compare_latest_downloads_json_vs_db()

    assert report["ok"] is True
    assert report["json_count"] == 1
    assert report["db_count"] == 1
    assert report["case_diffs"] == []
    assert report["errors"] == []


def test_consistency_checker_flags_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _setup_db_and_json(tmp_path, monkeypatch, title="Different Title")

    report = compare_latest_downloads_json_vs_db()

    assert report["ok"] is False
    assert report["json_count"] == 1
    assert report["db_count"] == 1
    assert len(report["case_diffs"]) >= 1
    mismatch_issues = {diff["issue_type"] for diff in report["case_diffs"]}
    assert "field_mismatch" in mismatch_issues
    assert any("title" in diff["details"] for diff in report["case_diffs"])


def test_consistency_checker_errors_when_no_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    _seed_json_downloads(config.DOWNLOADS_LOG, title="Seed Title")

    report = compare_latest_downloads_json_vs_db()

    assert report["ok"] is False
    assert report["db_count"] == 0
    assert report["json_count"] == 1
    assert any(
        diff["issue_type"] == "missing_in_db" for diff in report["case_diffs"]
    )
    assert any("no latest run" in err.lower() for err in report["errors"])
