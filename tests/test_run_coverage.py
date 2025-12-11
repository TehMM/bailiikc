from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.scraper import db, db_reporting, sources
from tests.test_runs_api_db import _configure_temp_paths, _reload_main_module


def _record_version() -> int:
    return db.record_csv_version(
        fetched_at="2024-01-01T00:00:00Z",
        source_url="http://example.com/csv",
        sha256="abc123",
        row_count=1,
        file_path="judgments.csv",
    )

def _insert_unreported_case(conn, csv_version_id: int, token_suffix: str) -> int:
    cursor = conn.execute(
        """
        INSERT INTO cases (
            action_token_raw, action_token_norm, title, cause_number, court, category,
            judgment_date, is_criminal, is_active, source, first_seen_version_id,
            last_seen_version_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, 'unreported_judgments', ?, ?)
        """,
        (
            f"RAW-{token_suffix}",
            f"NORM-{token_suffix}",
            f"Title {token_suffix}",
            f"Cause {token_suffix}",
            "Court",
            "Category",
            "2024-01-01",
            csv_version_id,
            csv_version_id,
        ),
    )
    return int(cursor.lastrowid)


def _insert_public_registers_case(conn, csv_version_id: int, token_suffix: str) -> int:
    cursor = conn.execute(
        """
        INSERT INTO cases (
            action_token_raw, action_token_norm, title, cause_number, court, category,
            judgment_date, is_criminal, is_active, source, first_seen_version_id,
            last_seen_version_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, 'public_registers', ?, ?)
        """,
        (
            f"PR-RAW-{token_suffix}",
            f"PRNORM-{token_suffix}",
            f"PR Title {token_suffix}",
            f"PR Cause {token_suffix}",
            "Public Court",
            "Public Category",
            "2024-01-01",
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


def test_get_run_coverage_ok(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    previous_version = _record_version()
    version_id = _record_version()
    run_id = db.create_run(trigger="test", mode="new", csv_version_id=version_id, params_json="{}")

    conn = db.get_connection()
    with conn:
        for i in range(100):
            _insert_unreported_case(conn, version_id, f"CASE-{i}")
            conn.execute(
                "UPDATE cases SET first_seen_version_id = ? WHERE action_token_norm = ?",
                (previous_version, f"NORM-CASE-{i}"),
            )
        planned_case_ids = [
            _insert_unreported_case(conn, version_id, f"PLANNED-{i}") for i in range(20)
        ]
        for case_id in planned_case_ids:
            _insert_download(conn, run_id=run_id, case_id=case_id, status="downloaded")

    coverage = db_reporting.get_run_coverage(run_id)
    assert coverage["cases_total"] == 120
    assert coverage["cases_planned"] == 20
    assert coverage["cases_downloaded"] == 20
    assert coverage["coverage_ratio"] == pytest.approx(1.0)
    assert coverage["run_health"] == "ok"


def test_get_run_coverage_partial(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    version_id = _record_version()
    run_id = db.create_run(trigger="test", mode="new", csv_version_id=version_id, params_json="{}")

    conn = db.get_connection()
    with conn:
        for i in range(20):
            case_id = _insert_unreported_case(conn, version_id, f"PARTIAL-{i}")
            status = "downloaded" if i < 12 else "failed" if i < 17 else "skipped"
            _insert_download(conn, run_id=run_id, case_id=case_id, status=status)

    coverage = db_reporting.get_run_coverage(run_id)
    assert coverage["cases_planned"] == 20
    assert coverage["cases_downloaded"] == 12
    assert coverage["cases_failed"] == 5
    assert coverage["cases_skipped"] == 3
    assert coverage["coverage_ratio"] == pytest.approx(0.6)
    assert coverage["run_health"] == "partial"


def test_get_run_coverage_scopes_by_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    version_id = _record_version()
    uj_run = db.create_run(
        trigger="test",
        mode="new",
        csv_version_id=version_id,
        params_json=json.dumps({"target_source": sources.UNREPORTED_JUDGMENTS}),
    )
    pr_run = db.create_run(
        trigger="test",
        mode="new",
        csv_version_id=version_id,
        params_json=json.dumps({"target_source": sources.PUBLIC_REGISTERS}),
    )

    conn = db.get_connection()
    with conn:
        uj_one = _insert_unreported_case(conn, version_id, "UJ-ONE")
        _insert_unreported_case(conn, version_id, "UJ-TWO")
        pr_one = _insert_public_registers_case(conn, version_id, "PR-ONE")
        _insert_public_registers_case(conn, version_id, "PR-TWO")

        _insert_download(conn, run_id=uj_run, case_id=uj_one, status="downloaded")
        _insert_download(conn, run_id=pr_run, case_id=pr_one, status="downloaded")

    uj_coverage = db_reporting.get_run_coverage(uj_run)
    assert uj_coverage["cases_total"] == 2
    assert uj_coverage["cases_downloaded"] == 1
    assert uj_coverage["cases_planned"] == 2
    assert uj_coverage["coverage_ratio"] == pytest.approx(0.5)

    pr_coverage = db_reporting.get_run_coverage(pr_run)
    assert pr_coverage["cases_total"] == 2
    assert pr_coverage["cases_downloaded"] == 1
    assert pr_coverage["cases_planned"] == 2
    assert pr_coverage["coverage_ratio"] == pytest.approx(0.5)


def test_get_run_coverage_honours_legacy_source_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    version_id = _record_version()
    run_id = db.create_run(
        trigger="test",
        mode="new",
        csv_version_id=version_id,
        params_json=json.dumps({"source": sources.PUBLIC_REGISTERS}),
    )

    conn = db.get_connection()
    with conn:
        pr_case = _insert_public_registers_case(conn, version_id, "LEGACY-PR")
        uj_case = _insert_unreported_case(conn, version_id, "LEGACY-UJ")
        _insert_download(conn, run_id=run_id, case_id=pr_case, status="downloaded")
        _insert_download(conn, run_id=run_id, case_id=uj_case, status="downloaded")

    coverage = db_reporting.get_run_coverage(run_id)
    assert coverage["cases_total"] == 1
    assert coverage["cases_downloaded"] == 1
    assert coverage["cases_planned"] == 1


def test_get_run_coverage_suspicious_when_no_attempts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    version_id = _record_version()
    run_id = db.create_run(trigger="test", mode="new", csv_version_id=version_id, params_json="{}")

    conn = db.get_connection()
    with conn:
        for i in range(50):
            _insert_unreported_case(conn, version_id, f"UNSEEN-{i}")

    coverage = db_reporting.get_run_coverage(run_id)
    assert coverage["cases_planned"] == 50
    assert coverage["cases_attempted"] == 0
    assert coverage["run_health"] == "suspicious"


def test_infer_run_source_handles_malformed_params(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BAILIIKC_USE_DB_REPORTING", "1")

    assert db_reporting._infer_run_source("not-json") == sources.DEFAULT_SOURCE
    assert db_reporting._infer_run_source(json.dumps(["not", "a", "dict"])) == sources.DEFAULT_SOURCE
    assert (
        db_reporting._infer_run_source(
            json.dumps({"target_source": "unknown-source-value"})
        )
        == sources.DEFAULT_SOURCE
    )


def test_run_health_api_returns_coverage_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setenv("BAILIIKC_USE_DB_REPORTING", "1")
    db.initialize_schema()

    version_id = _record_version()
    run_id = db.create_run(trigger="test", mode="new", csv_version_id=version_id, params_json="{}")

    conn = db.get_connection()
    with conn:
        case_one = _insert_unreported_case(conn, version_id, "API-ONE")
        case_two = _insert_unreported_case(conn, version_id, "API-TWO")
        _insert_download(conn, run_id=run_id, case_id=case_one, status="downloaded")
        _insert_download(conn, run_id=run_id, case_id=case_two, status="failed")

    coverage = db_reporting.get_run_coverage(run_id)
    db.update_run_coverage(run_id, coverage)

    main = _reload_main_module()
    client = main.app.test_client()

    runs_resp = client.get("/api/db/runs")
    assert runs_resp.status_code == 200
    runs_payload = runs_resp.get_json()
    assert runs_payload["runs"][0]["cases_downloaded"] == 1

    health_resp = client.get(f"/api/db/runs/{run_id}/health")
    assert health_resp.status_code == 200
    health_payload = health_resp.get_json()
    assert health_payload["ok"] is True
    assert health_payload["cases_attempted"] == 2
    assert health_payload["run_health"] in {"partial", "ok", "failed", "suspicious"}
