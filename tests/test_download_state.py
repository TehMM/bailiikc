import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.scraper import config, db
from app.scraper.download_state import CaseDownloadState, DownloadStatus


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


def _create_run_and_case() -> Dict[str, int]:
    csv_version_id = db.record_csv_version(
        fetched_at="2024-03-01T00:00:00Z",
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
        case_id = int(cursor.lastrowid)
    return {"run_id": int(run_id), "case_id": case_id}


def _select_download(run_id: int, case_id: int) -> Any:
    conn = db.get_connection()
    cursor = conn.execute(
        "SELECT * FROM downloads WHERE run_id = ? AND case_id = ?",
        (run_id, case_id),
    )
    return cursor.fetchone()


def test_download_state_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()
    ids = _create_run_and_case()

    events: List[Any] = []
    monkeypatch.setattr(
        sys.modules["app.scraper.download_state"],
        "_scraper_event",
        lambda phase, **fields: events.append((phase, fields)),
    )

    state = CaseDownloadState.start(
        run_id=ids["run_id"],
        case_id=ids["case_id"],
        box_url="https://example.com/box",
    )
    assert state.status is DownloadStatus.IN_PROGRESS
    assert state.attempt_count == 1

    row = _select_download(ids["run_id"], ids["case_id"])
    assert row["status"] == DownloadStatus.IN_PROGRESS.value
    assert row["attempt_count"] == 1

    state.mark_downloaded(
        file_path="pdfs/seed.pdf",
        file_size_bytes=2048,
        box_url="https://example.com/box",
    )

    row = _select_download(ids["run_id"], ids["case_id"])
    assert row["status"] == DownloadStatus.DOWNLOADED.value
    assert row["file_path"] == "pdfs/seed.pdf"
    assert row["file_size_bytes"] == 2048

    phases = [entry[0] for entry in events]
    assert "state" in phases
    assert any(
        evt[1].get("from_status") == "pending" and evt[1].get("to_status") == "in_progress"
        for evt in events
    )
    assert any(
        evt[1].get("from_status") == "in_progress" and evt[1].get("to_status") == "downloaded"
        for evt in events
    )


def test_download_state_skip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()
    ids = _create_run_and_case()

    state = CaseDownloadState.start(
        run_id=ids["run_id"],
        case_id=ids["case_id"],
        box_url=None,
    )
    state.mark_skipped("already_downloaded")

    row = _select_download(ids["run_id"], ids["case_id"])
    assert row["status"] == DownloadStatus.SKIPPED.value
    assert row["error_code"] == "already_downloaded"


def test_download_state_failed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()
    ids = _create_run_and_case()

    state = CaseDownloadState.start(
        run_id=ids["run_id"],
        case_id=ids["case_id"],
        box_url="https://example.com/box",
    )
    state.mark_failed(error_code="disk_full", error_message="no space left")

    row = _select_download(ids["run_id"], ids["case_id"])
    assert row["status"] == DownloadStatus.FAILED.value
    assert row["error_code"] == "disk_full"
    assert row["error_message"] == "no space left"
