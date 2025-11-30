import json
from pathlib import Path
from typing import Optional

import pytest

from app.scraper import config, csv_sync, db, sources
from app.scraper import db_reporting


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


def _configure_temp_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "bailiikc.db"
    monkeypatch.setattr(config, "DATA_DIR", data_dir)
    monkeypatch.setattr(config, "DB_PATH", db_path)
    monkeypatch.setattr(config, "PDF_DIR", data_dir / "pdfs")
    monkeypatch.setattr(db, "DB_PATH", db_path)


@pytest.fixture()
def sample_csv_bytes() -> bytes:
    sample_csv = Path(__file__).parent / "data" / "judgments_sample.csv"
    return sample_csv.read_bytes()


@pytest.fixture()
def populated_runs_and_downloads_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sample_csv_bytes: bytes) -> dict:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()
    session = _DummySession(sample_csv_bytes)
    csv_result = csv_sync.sync_csv("http://example.com/judgments.csv", session=session)

    params = {
        "base_url": config.DEFAULT_BASE_URL,
        "scrape_mode": "full",
        "target_source": sources.UNREPORTED_JUDGMENTS,
    }
    run_id = db.create_run(
        trigger="test",
        mode="full",
        csv_version_id=csv_result.version_id,
        params_json=json.dumps(params),
    )
    db.mark_run_completed(run_id)

    conn = db.get_connection()
    case_rows = conn.execute(
        "SELECT id, action_token_norm FROM cases ORDER BY id ASC LIMIT 3"
    ).fetchall()
    primary_case_id = case_rows[0]["id"]
    failed_case_id = case_rows[1]["id"] if len(case_rows) > 1 else primary_case_id
    skipped_case_id = case_rows[2]["id"] if len(case_rows) > 2 else primary_case_id

    downloaded_ts = "2024-01-01T00:00:00Z"
    failed_ts = "2024-01-02T00:00:00Z"
    skipped_ts = "2024-01-03T00:00:00Z"

    with conn:
        conn.execute(
            """
            INSERT INTO downloads (
                run_id, case_id, status, attempt_count, last_attempt_at, file_path,
                file_size_bytes, box_url_last, error_code, error_message, created_at, updated_at
            ) VALUES (?, ?, 'downloaded', 1, ?, ?, 1024, 'https://example.com/box1', NULL, NULL, ?, ?)
            """,
            (run_id, primary_case_id, downloaded_ts, "pdfs/example1.pdf", downloaded_ts, downloaded_ts),
        )
        conn.execute(
            """
            INSERT INTO downloads (
                run_id, case_id, status, attempt_count, last_attempt_at, file_path,
                file_size_bytes, box_url_last, error_code, error_message, created_at, updated_at
            ) VALUES (?, ?, 'failed', 2, ?, NULL, NULL, 'https://example.com/box2', 'timeout', 'network issue', ?, ?)
            """,
            (run_id, failed_case_id, failed_ts, failed_ts, failed_ts),
        )
        conn.execute(
            """
            INSERT INTO downloads (
                run_id, case_id, status, attempt_count, last_attempt_at, file_path,
                file_size_bytes, box_url_last, error_code, error_message, created_at, updated_at
            ) VALUES (?, ?, 'skipped', 0, ?, NULL, NULL, 'https://example.com/box3', NULL, NULL, ?, ?)
            """,
            (run_id, skipped_case_id, skipped_ts, skipped_ts, skipped_ts),
        )

    return {
        "run_id": run_id,
        "csv_version_id": csv_result.version_id,
        "case_rows": case_rows,
    }


def test_get_latest_run_id_and_summary(populated_runs_and_downloads_db: dict) -> None:
    run_id = populated_runs_and_downloads_db["run_id"]
    latest = db_reporting.get_latest_run_id()
    assert latest == run_id

    summary = db_reporting.get_run_summary(run_id)
    assert summary
    assert summary["status"] == "completed"
    assert summary["mode"] == "full"
    assert summary["trigger"] == "test"


def test_get_download_rows_for_run_basic(populated_runs_and_downloads_db: dict) -> None:
    run_id = populated_runs_and_downloads_db["run_id"]
    rows = db_reporting.get_download_rows_for_run(run_id)

    assert rows
    required_keys = {
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
        "size_kb",
    }

    for row in rows:
        assert required_keys.issubset(row.keys())
        if row["saved_path"]:
            assert row["filename"] == Path(row["saved_path"]).name
        if row["judgment_date"]:
            assert row["sort_judgment_date"]


def test_get_download_rows_for_run_status_filter(populated_runs_and_downloads_db: dict) -> None:
    run_id = populated_runs_and_downloads_db["run_id"]
    rows = db_reporting.get_download_rows_for_run(run_id, status_filter="downloaded")
    assert rows
    assert all(row["saved_path"].endswith("example1.pdf") for row in rows)
    assert rows[0]["size_kb"] == pytest.approx(1.0)
    assert len(rows) == 1


def test_get_run_coverage_counts_default_source(populated_runs_and_downloads_db: dict) -> None:
    run_id = populated_runs_and_downloads_db["run_id"]
    csv_version_id = populated_runs_and_downloads_db["csv_version_id"]

    conn = db.get_connection()
    count_row = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM cases
        WHERE source = ?
          AND is_active = 1
          AND first_seen_version_id <= ?
          AND last_seen_version_id >= ?
        """,
        (sources.UNREPORTED_JUDGMENTS, csv_version_id, csv_version_id),
    ).fetchone()

    summary = db_reporting.get_run_coverage(run_id)
    assert summary["cases_total"] == int(count_row["count"])


def test_get_download_rows_for_run_uses_latest_run_when_none(populated_runs_and_downloads_db: dict) -> None:
    first_run_id = populated_runs_and_downloads_db["run_id"]
    csv_version_id = populated_runs_and_downloads_db["csv_version_id"]
    case_id = populated_runs_and_downloads_db["case_rows"][0]["id"]

    second_run_id = db.create_run(
        trigger="webhook", mode="new", csv_version_id=csv_version_id, params_json="{}"
    )
    conn = db.get_connection()
    with conn:
        conn.execute(
            "UPDATE runs SET started_at = ? WHERE id = ?",
            ("2024-01-01T00:00:00Z", first_run_id),
        )
        conn.execute(
            "UPDATE runs SET started_at = ? WHERE id = ?",
            ("2024-02-01T00:00:00Z", second_run_id),
        )
        conn.execute(
            """
            INSERT INTO downloads (
                run_id, case_id, status, attempt_count, last_attempt_at, file_path,
                file_size_bytes, box_url_last, error_code, error_message, created_at, updated_at
            ) VALUES (?, ?, 'downloaded', 1, '2024-02-01T12:00:00Z', 'pdfs/latest.pdf', 2048,
                'https://example.com/latest', NULL, NULL, '2024-02-01T12:00:00Z', '2024-02-01T12:00:00Z')
            """,
            (second_run_id, case_id),
        )
    db.mark_run_completed(second_run_id)

    rows = db_reporting.get_download_rows_for_run(run_id=None)
    assert rows
    assert all("latest" in row["saved_path"] for row in rows)
    assert all(row["saved_path"].endswith("latest.pdf") for row in rows)
    assert rows[0]["size_kb"] == pytest.approx(2.0)


def test_get_run_coverage_respects_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sample_csv_bytes: bytes
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    session = _DummySession(sample_csv_bytes)
    csv_result = csv_sync.sync_csv("http://example.com/judgments.csv", session=session)
    csv_version_id = csv_result.version_id

    conn = db.get_connection()
    with conn:
        cursor = conn.execute(
            """
            INSERT INTO cases (
                action_token_raw, action_token_norm, title, cause_number,
                court, category, judgment_date, is_criminal, is_active,
                source, first_seen_version_id, last_seen_version_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, ?, ?, ?)
            """,
            (
                "PUB001", "PUB001", "Public Register Case", "PR-1", "Court", "Cat", "2024-01-01",
                sources.PUBLIC_REGISTERS, csv_version_id, csv_version_id,
            ),
        )
        public_case_id = int(cursor.lastrowid)

    params = {"target_source": sources.PUBLIC_REGISTERS}
    run_id = db.create_run(
        trigger="test",
        mode="full",
        csv_version_id=csv_version_id,
        params_json=json.dumps(params),
    )
    db.mark_run_completed(run_id)

    with conn:
        conn.execute(
            """
            INSERT INTO downloads (
                run_id, case_id, status, attempt_count, last_attempt_at, file_path,
                file_size_bytes, box_url_last, error_code, error_message, created_at, updated_at
            ) VALUES (?, ?, 'downloaded', 1, '2024-03-01T00:00:00Z', 'pdfs/public.pdf', 2048,
                'https://example.com/public', NULL, NULL, '2024-03-01T00:00:00Z', '2024-03-01T00:00:00Z')
            """,
            (run_id, public_case_id),
        )

    summary = db_reporting.get_run_coverage(run_id)
    assert summary["cases_total"] == 1
    assert summary["cases_planned"] == 1
