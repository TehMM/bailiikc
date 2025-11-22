import json
import sys
from pathlib import Path
from typing import Optional

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.scraper import config, csv_sync, db


class _DummyResponse:
    def __init__(self, content: bytes):
        self.content = content
        self.headers = {}

    def raise_for_status(self) -> None:  # pragma: no cover - simple stub
        return None


class _DummySession:
    def __init__(self, content: bytes):
        self._content = content

    def get(self, url: str, timeout: Optional[tuple[int, int]] = None) -> _DummyResponse:  # noqa: ARG002
        return _DummyResponse(self._content)


def _configure_temp_paths(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "bailiikc.db"
    # Update shared module globals to ensure all helpers use the temporary DB.
    config.DATA_DIR = data_dir
    config.DB_PATH = db_path
    config.PDF_DIR = data_dir / "pdfs"
    db.DB_PATH = db_path


def test_csv_sync_populates_cases_and_versions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path)
    db.initialize_schema()

    sample_csv = Path(__file__).parent / "data" / "judgments_sample.csv"
    session = _DummySession(sample_csv.read_bytes())

    result = csv_sync.sync_csv("http://example.com/judgments.csv", session=session)
    assert result.version_id > 0

    conn = db.get_connection()
    cursor = conn.execute("SELECT COUNT(*) AS cnt FROM csv_versions WHERE valid = 1")
    assert cursor.fetchone()["cnt"] == 1

    cursor = conn.execute("SELECT COUNT(*) AS cnt FROM cases WHERE source = 'unreported_judgments'")
    assert cursor.fetchone()["cnt"] == 2

    case_id = db.get_case_id_by_token_norm("unreported_judgments", "FSD0151202511062025ATPLIFESCIENCE")
    assert case_id is not None


def test_download_logging_helpers(tmp_path: Path) -> None:
    _configure_temp_paths(tmp_path)
    db.initialize_schema()

    # Seed a CSV row to link against downloads.
    sample_csv = Path(__file__).parent / "data" / "judgments_sample.csv"
    session = _DummySession(sample_csv.read_bytes())
    sync_result = csv_sync.sync_csv("http://example.com/judgments.csv", session=session)

    run_id = db.create_run(
        trigger="cli",
        mode="full",
        csv_version_id=sync_result.version_id,
        params_json=json.dumps({"test": True}),
    )

    case_id = db.get_case_id_by_token_norm("unreported_judgments", "FSD9999202412312024EXAMPLE")
    assert case_id is not None

    row = db.ensure_download_row(run_id, case_id)
    assert row["status"] == "pending"
    db.update_download_status(
        run_id=run_id,
        case_id=case_id,
        status="downloaded",
        attempt_count=1,
        last_attempt_at="2024-01-01T00:00:00Z",
        file_path="pdfs/example.pdf",
        file_size_bytes=1234,
        box_url_last="http://box.test/file",
    )

    conn = db.get_connection()
    cursor = conn.execute(
        "SELECT status, attempt_count, file_path, file_size_bytes FROM downloads WHERE run_id=? AND case_id=?",
        (run_id, case_id),
    )
    updated = cursor.fetchone()
    assert updated["status"] == "downloaded"
    assert updated["attempt_count"] == 1
    assert updated["file_path"] == "pdfs/example.pdf"
    assert updated["file_size_bytes"] == 1234
