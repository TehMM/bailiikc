import pytest
from pathlib import Path

from app.scraper import config, db, worklist


def _configure_temp_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "bailiikc.db"

    monkeypatch.setattr(config, "DATA_DIR", data_dir)
    monkeypatch.setattr(config, "DB_PATH", db_path)
    monkeypatch.setattr(config, "PDF_DIR", data_dir / "pdfs")
    monkeypatch.setattr(config, "LOG_DIR", data_dir / "logs")
    monkeypatch.setattr(config, "LOG_FILE", (data_dir / "logs" / "latest.log"))
    monkeypatch.setattr(config, "METADATA_FILE", data_dir / "metadata.json")
    monkeypatch.setattr(config, "CONFIG_FILE", data_dir / "config.txt")
    monkeypatch.setattr(config, "CHECKPOINT_PATH", data_dir / "state.json")
    monkeypatch.setattr(config, "RUN_STATE_FILE", data_dir / "run_state.json")
    monkeypatch.setattr(config, "DOWNLOADS_LOG", data_dir / "downloads.jsonl")
    monkeypatch.setattr(config, "SUMMARY_FILE", data_dir / "last_summary.json")
    monkeypatch.setattr(config, "HISTORY_ACTIONS_FILE", data_dir / "history_actions.json")
    monkeypatch.setattr(db, "DB_PATH", db_path)


@pytest.fixture
def seeded_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    version_id = db.record_csv_version(
        fetched_at="2024-01-01T00:00:00Z",
        source_url="http://example.com/judgments.csv",
        sha256="abc123",
        row_count=1,
        file_path="/tmp/judgments.csv",
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "TOKEN-RAW",
                "TOKENNORM",
                "Sample title",
                "123/2024",
                "Grand Court",
                "Civil",
                "2024-01-01",
                0,
                1,
                worklist.DEFAULT_SOURCE,
                version_id,
                version_id,
            ),
        )
        case_id = int(cursor.lastrowid)
    return version_id, case_id


def test_selects_run_and_builds_resume_worklist(seeded_db):
    version_id, case_id = seeded_db

    conn = db.get_connection()
    with conn:
        cursor = conn.execute(
            """
            INSERT INTO runs (
                started_at, ended_at, trigger, mode, csv_version_id, params_json, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2024-01-02T00:00:00Z",
                None,
                "cli",
                "resume",
                version_id,
                '{"source": "unreported_judgments"}',
                "failed",
            ),
        )
        run_id = int(cursor.lastrowid)
        conn.execute(
            """
            INSERT INTO downloads (
                run_id, case_id, status, attempt_count, last_attempt_at,
                file_path, file_size_bytes, box_url_last, error_code, error_message,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                case_id,
                "failed",
                1,
                "2024-01-02T01:00:00Z",
                None,
                None,
                None,
                "NETWORK",
                "timeout",
                "2024-01-02T00:30:00Z",
                "2024-01-02T01:00:00Z",
            ),
        )

    selected = worklist._select_run_for_resume(version_id)
    assert selected == run_id

    items = worklist.build_resume_worklist(version_id)
    assert len(items) == 1
    assert items[0].case_id == case_id
    assert items[0].action_token_norm == "TOKENNORM"


def test_newest_incomplete_run_is_selected(seeded_db):
    version_id, case_id = seeded_db

    conn = db.get_connection()
    with conn:
        cursor_old = conn.execute(
            """
            INSERT INTO runs (started_at, ended_at, trigger, mode, csv_version_id, params_json, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2024-01-01T00:00:00Z",
                None,
                "cli",
                "new",
                version_id,
                "{}",
                "failed",
            ),
        )
        old_run = int(cursor_old.lastrowid)

        cursor_new = conn.execute(
            """
            INSERT INTO runs (started_at, ended_at, trigger, mode, csv_version_id, params_json, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2024-01-03T00:00:00Z",
                None,
                "cli",
                "resume",
                version_id,
                "{}",
                "running",
            ),
        )
        new_run = int(cursor_new.lastrowid)

        conn.executemany(
            """
            INSERT INTO downloads (
                run_id, case_id, status, attempt_count, last_attempt_at,
                file_path, file_size_bytes, box_url_last, error_code, error_message,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    old_run,
                    case_id,
                    "failed",
                    1,
                    "2024-01-01T01:00:00Z",
                    None,
                    None,
                    None,
                    None,
                    None,
                    "2024-01-01T00:30:00Z",
                    "2024-01-01T01:00:00Z",
                ),
                (
                    new_run,
                    case_id,
                    "failed",
                    1,
                    "2024-01-03T01:00:00Z",
                    None,
                    None,
                    None,
                    None,
                    None,
                    "2024-01-03T00:30:00Z",
                    "2024-01-03T01:00:00Z",
                ),
            ],
        )

    selected = worklist._select_run_for_resume(version_id)
    assert selected == new_run

    items = worklist.build_resume_worklist(version_id)
    assert {item.case_id for item in items} == {case_id}


def test_completed_runs_are_ignored(seeded_db):
    version_id, _ = seeded_db

    conn = db.get_connection()
    with conn:
        conn.execute(
            """
            INSERT INTO runs (
                started_at, ended_at, trigger, mode, csv_version_id, params_json, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2024-01-04T00:00:00Z",
                "2024-01-04T02:00:00Z",
                "cli",
                "resume",
                version_id,
                "{}",
                "completed",
            ),
        )

    assert worklist._select_run_for_resume(version_id) is None
    assert worklist.build_resume_worklist(version_id) == []


def test_criminal_and_inactive_cases_excluded(seeded_db):
    version_id, _ = seeded_db

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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "CRIM-RAW",
                "CRIMNORM",
                "Criminal",
                "999/2024",
                "Criminal Court",
                "Criminal",
                "2024-01-05",
                1,
                1,
                worklist.DEFAULT_SOURCE,
                version_id,
                version_id,
            ),
        )
        criminal_case_id = int(cursor.lastrowid)

        cursor_inactive = conn.execute(
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "INACTIVE-RAW",
                "INACTIVENORM",
                "Inactive",
                "100/2024",
                "Civil",
                "Civil",
                "2024-01-06",
                0,
                0,
                worklist.DEFAULT_SOURCE,
                version_id,
                version_id,
            ),
        )
        inactive_case_id = int(cursor_inactive.lastrowid)

        cursor_run = conn.execute(
            """
            INSERT INTO runs (
                started_at, ended_at, trigger, mode, csv_version_id, params_json, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2024-01-06T00:00:00Z",
                None,
                "cli",
                "resume",
                version_id,
                "{}",
                "failed",
            ),
        )
        run_id = int(cursor_run.lastrowid)

        conn.executemany(
            """
            INSERT INTO downloads (
                run_id, case_id, status, attempt_count, last_attempt_at,
                file_path, file_size_bytes, box_url_last, error_code, error_message,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    criminal_case_id,
                    "failed",
                    1,
                    "2024-01-06T01:00:00Z",
                    None,
                    None,
                    None,
                    None,
                    None,
                    "2024-01-06T00:30:00Z",
                    "2024-01-06T01:00:00Z",
                ),
                (
                    run_id,
                    inactive_case_id,
                    "pending",
                    0,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "2024-01-06T00:30:00Z",
                    "2024-01-06T00:30:00Z",
                ),
            ],
        )

    items = worklist.build_resume_worklist_for_run(run_id)
    ids = {item.case_id for item in items}
    assert criminal_case_id not in ids
    assert inactive_case_id not in ids
    assert ids == set()


def test_invalid_run_id_returns_empty(seeded_db):
    version_id, _ = seeded_db

    # No runs exist for this version; invalid identifiers should not raise.
    assert worklist.build_resume_worklist_for_run(99999) == []
    assert worklist.build_resume_worklist(version_id) == []
