from __future__ import annotations

from pathlib import Path
from typing import Optional

import pytest

from app.scraper import config, csv_sync, db, worklist


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


def test_build_full_and_new_worklists_match_csv_sync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    sample_csv = Path(__file__).parent / "data" / "judgments_sample.csv"
    session = _DummySession(sample_csv.read_bytes())

    sync_result = csv_sync.sync_csv("http://example.com/judgments.csv", session=session)
    version_id = sync_result.version_id

    full_items = worklist.build_full_worklist(version_id)
    new_items = worklist.build_new_worklist(version_id)

    full_ids = {item.case_id for item in full_items}
    new_ids = {item.case_id for item in new_items}
    expected_ids = set(sync_result.new_case_ids)

    assert full_ids == expected_ids
    assert new_ids == expected_ids

    assert all(not item.is_criminal for item in full_items)
    assert all(item.is_active for item in full_items)
    assert all(item.source == worklist.DEFAULT_SOURCE for item in full_items)


def test_build_worklist_dispatches_by_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    sample_csv = Path(__file__).parent / "data" / "judgments_sample.csv"
    session = _DummySession(sample_csv.read_bytes())
    sync_result = csv_sync.sync_csv("http://example.com/judgments.csv", session=session)
    version_id = sync_result.version_id

    full_items = worklist.build_worklist("full", version_id)
    new_items = worklist.build_worklist("new", version_id)

    assert {i.case_id for i in full_items} == set(sync_result.new_case_ids)
    assert {i.case_id for i in new_items} == set(sync_result.new_case_ids)

    resume_items = worklist.build_worklist("resume", version_id)
    assert resume_items == []

    with pytest.raises(ValueError):
        worklist.build_worklist("unknown-mode", version_id)


def test_full_worklist_excludes_criminal_cases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    sample_csv = Path(__file__).parent / "data" / "judgments_sample.csv"
    session = _DummySession(sample_csv.read_bytes())
    sync_result = csv_sync.sync_csv("http://example.com/judgments.csv", session=session)
    version_id = sync_result.version_id

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
                "CRIM-TOKEN-RAW",
                "CRIMTOKEN",
                "Criminal Sample",
                "CR/123/2024",
                "Criminal Court",
                "Criminal Division",
                "2024-01-01",
                1,
                1,
                worklist.DEFAULT_SOURCE,
                version_id,
                version_id,
            ),
        )
        criminal_id = int(cursor.lastrowid)

    items = worklist.build_full_worklist(version_id)
    ids = {item.case_id for item in items}

    assert criminal_id not in ids
    assert all(not item.is_criminal for item in items)
