from pathlib import Path

import pytest

from app.scraper import db, db_reporting
from tests.test_runs_api_db import _configure_temp_paths, _reload_main_module


def _create_run(conn, trigger: str, mode: str, csv_version_id: int) -> int:
    return db.create_run(trigger=trigger, mode=mode, csv_version_id=csv_version_id, params_json="{}")


def _record_version(fetched_at: str, source_url: str, sha256: str) -> int:
    return db.record_csv_version(
        fetched_at=fetched_at,
        source_url=source_url,
        sha256=sha256,
        row_count=1,
        file_path="judgments.csv",
    )


def test_list_recent_runs_orders_by_started_at_desc(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    conn = db.get_connection()
    with conn:
        v1 = _record_version("2024-01-01T00:00:00Z", "http://example.com/csv1", "sha1")
        v2 = _record_version("2024-01-02T00:00:00Z", "http://example.com/csv2", "sha2")
        first = _create_run(conn, "test", "full", v1)
        second = _create_run(conn, "test", "new", v2)

    runs = db_reporting.list_recent_runs(limit=10)
    ids = [run["id"] for run in runs]
    assert ids[:2] == [second, first]


def test_api_db_runs_list_returns_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    conn = db.get_connection()
    with conn:
        version_id = _record_version("2024-01-01T00:00:00Z", "http://example.com/csv", "sha1")
        db.create_run(trigger="ui", mode="full", csv_version_id=version_id, params_json="{}")
        db.create_run(trigger="webhook", mode="new", csv_version_id=version_id, params_json="{}")

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get("/api/db/runs")
    assert resp.status_code == 200

    payload = resp.get_json()
    assert payload["ok"] is True
    assert payload["count"] == 2
    assert len(payload["runs"]) == 2
    for run in payload["runs"]:
        assert "id" in run
        assert "trigger" in run
        assert "mode" in run
        assert "csv_version_id" in run
        assert "status" in run
        assert "started_at" in run
        assert "ended_at" in run
        assert "error_summary" in run


def test_api_db_runs_list_respects_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    conn = db.get_connection()
    with conn:
        version_id = _record_version("2024-01-01T00:00:00Z", "http://example.com/csv", "sha1")
        for i in range(3):
            db.create_run(trigger=f"t{i}", mode="full", csv_version_id=version_id, params_json="{}")

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get("/api/db/runs?limit=1")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    assert payload["count"] == 1
    assert len(payload["runs"]) == 1
