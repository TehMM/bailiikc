import json
from pathlib import Path

import pytest

from app.scraper import db, db_reporting
from app.scraper import sources
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


def _insert_case(
    conn,
    csv_version_id: int,
    *,
    source: str,
    token_suffix: str,
) -> int:
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
            source,
            csv_version_id,
            csv_version_id,
        ),
    )
    return int(cursor.lastrowid)


def _insert_download(
    conn,
    *,
    run_id: int,
    case_id: int,
    status: str,
    ts: str = "2024-01-02T00:00:00Z",
) -> None:
    conn.execute(
        """
        INSERT INTO downloads (
            run_id, case_id, status, attempt_count, last_attempt_at, file_path,
            file_size_bytes, box_url_last, error_code, error_message, created_at,
            updated_at
        ) VALUES (?, ?, ?, 1, ?, NULL, NULL, NULL, NULL, NULL, ?, ?)
        """,
        (run_id, case_id, status, ts, ts, ts),
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
        assert run["target_source"] == sources.DEFAULT_SOURCE


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


def test_list_recent_runs_filters_by_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    conn = db.get_connection()
    with conn:
        version_id = _record_version("2024-01-01T00:00:00Z", "http://example.com/csv", "sha1")
        db.create_run(
            trigger="ui",
            mode="full",
            csv_version_id=version_id,
            params_json=json.dumps({"target_source": sources.PUBLIC_REGISTERS}),
        )
        db.create_run(
            trigger="webhook",
            mode="new",
            csv_version_id=version_id,
            params_json=json.dumps({"target_source": sources.UNREPORTED_JUDGMENTS}),
        )
        db.create_run(trigger="legacy", mode="new", csv_version_id=version_id, params_json="{}")

    all_runs = db_reporting.list_recent_runs(10)
    assert {run["target_source"] for run in all_runs} == {
        sources.PUBLIC_REGISTERS,
        sources.UNREPORTED_JUDGMENTS,
    }

    pr_runs = db_reporting.list_recent_runs(10, source=sources.PUBLIC_REGISTERS)
    assert len(pr_runs) == 1
    assert pr_runs[0]["target_source"] == sources.PUBLIC_REGISTERS

    default_runs = db_reporting.list_recent_runs(10, source=sources.DEFAULT_SOURCE)
    assert len(default_runs) == 2
    assert all(run["target_source"] == sources.DEFAULT_SOURCE for run in default_runs)


def test_api_db_runs_list_filters_by_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    conn = db.get_connection()
    with conn:
        version_id = _record_version("2024-01-01T00:00:00Z", "http://example.com/csv", "sha1")
        db.create_run(
            trigger="ui",
            mode="full",
            csv_version_id=version_id,
            params_json=json.dumps({"target_source": sources.PUBLIC_REGISTERS}),
        )
        db.create_run(
            trigger="webhook",
            mode="new",
            csv_version_id=version_id,
            params_json=json.dumps({"target_source": sources.UNREPORTED_JUDGMENTS}),
        )

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get(f"/api/db/runs?source={sources.PUBLIC_REGISTERS}")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    assert payload["count"] == 1
    assert payload["runs"][0]["target_source"] == sources.PUBLIC_REGISTERS


def test_api_db_run_summary_returns_coverage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setenv("BAILIIKC_USE_DB_REPORTING", "1")
    db.initialize_schema()

    conn = db.get_connection()
    with conn:
        version_id = _record_version("2024-01-01T00:00:00Z", "http://example.com/csv", "sha1")
        run_id = db.create_run(
            trigger="ui",
            mode="new",
            csv_version_id=version_id,
            params_json=json.dumps({"target_source": sources.PUBLIC_REGISTERS}),
        )

        pr_one = _insert_case(
            conn, version_id, source=sources.PUBLIC_REGISTERS, token_suffix="PR-ONE"
        )
        pr_two = _insert_case(
            conn, version_id, source=sources.PUBLIC_REGISTERS, token_suffix="PR-TWO"
        )
        uj_case = _insert_case(
            conn, version_id, source=sources.UNREPORTED_JUDGMENTS, token_suffix="UJ-ONE"
        )

        _insert_download(conn, run_id=run_id, case_id=pr_one, status="downloaded")
        _insert_download(conn, run_id=run_id, case_id=pr_two, status="failed")
        _insert_download(conn, run_id=run_id, case_id=uj_case, status="downloaded")

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get(f"/api/db/runs/{run_id}/summary")
    assert resp.status_code == 200

    payload = resp.get_json()
    assert payload["ok"] is True
    run_payload = payload["run"]
    coverage = payload["coverage"]

    assert run_payload["id"] == run_id
    assert run_payload["target_source"] == sources.PUBLIC_REGISTERS
    assert coverage["cases_total"] == 2
    assert coverage["cases_planned"] == 2
    assert coverage["cases_attempted"] == 2
    assert coverage["cases_downloaded"] == 1
    assert coverage["cases_failed"] == 1
    assert coverage["cases_skipped"] == 0
    assert coverage["coverage_ratio"] == pytest.approx(0.5)
    assert coverage["run_health"] == "partial"

    downloads = payload["downloads"]
    assert downloads["status_counts"]["downloaded"] == 1
    assert downloads["status_counts"]["failed"] == 1
    assert downloads["status_counts"].get("skipped", 0) == 0
    assert downloads["fail_reasons"]["unknown"] == 1
    assert downloads["skip_reasons"] == {}


def test_api_db_run_summary_not_found(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get("/api/db/runs/999/summary")
    assert resp.status_code == 404
    payload = resp.get_json()
    assert payload["ok"] is False
    assert payload["error"] == "run_not_found"
