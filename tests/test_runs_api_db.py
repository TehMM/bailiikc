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
    monkeypatch.setattr(config, "METADATA_FILE", data_dir / "metadata.json")
    monkeypatch.setattr(config, "DB_PATH", db_path)
    monkeypatch.setattr(db, "DB_PATH", db_path)


def _reload_main_module():
    if "app.main" in sys.modules:
        del sys.modules["app.main"]
    return importlib.import_module("app.main")


def _insert_case(conn, csv_version_id: int, token_suffix: str) -> int:
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
            "source",
            csv_version_id,
            csv_version_id,
        ),
    )
    return int(cursor.lastrowid)


def test_api_runs_latest_returns_db_backed_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setenv("BAILIIKC_USE_DB_REPORTING", "0")

    main = _reload_main_module()

    csv_version_id = db.record_csv_version(
        fetched_at="2024-01-01T00:00:00Z",
        source_url="http://example.com/csv",
        sha256="abc123",
        row_count=5,
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
        downloaded_case = _insert_case(conn, csv_version_id, "DL")
        failed_case = _insert_case(conn, csv_version_id, "FAIL")
        skipped_case = _insert_case(conn, csv_version_id, "SKIP")
        pending_case = _insert_case(conn, csv_version_id, "PEND")
        in_progress_case = _insert_case(conn, csv_version_id, "PROG")

        ts = "2024-01-02T00:00:00Z"
        for status, case_id, path, size in [
            ("downloaded", downloaded_case, "pdfs/downloaded.pdf", 1024),
            ("failed", failed_case, None, None),
            ("skipped", skipped_case, None, None),
            ("pending", pending_case, None, None),
            ("in_progress", in_progress_case, None, None),
        ]:
            conn.execute(
                """
                INSERT INTO downloads (
                    run_id, case_id, status, attempt_count, last_attempt_at, file_path,
                    file_size_bytes, box_url_last, error_code, error_message, created_at,
                    updated_at
                ) VALUES (?, ?, ?, 1, ?, ?, ?, NULL, NULL, NULL, ?, ?)
                """,
                (run_id, case_id, status, ts, path, size, ts, ts),
            )

    db.mark_run_completed(run_id)

    client = main.app.test_client()
    response = client.get("/api/runs/latest")
    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True

    run = data["run"]
    assert run["id"] == run_id
    assert run["mode"] == "full"
    assert run["downloads"]["total"] == 5
    assert run["downloads"]["downloaded"] == 1
    assert run["downloads"]["failed"] == 1
    assert run["downloads"]["skipped"] == 1
    assert run["downloads"]["pending"] == 1
    assert run["downloads"]["in_progress"] == 1


def test_api_runs_latest_no_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setenv("BAILIIKC_USE_DB_REPORTING", "0")

    main = _reload_main_module()

    client = main.app.test_client()
    response = client.get("/api/runs/latest")

    assert response.status_code == 404
    data = response.get_json()
    assert data["ok"] is False
