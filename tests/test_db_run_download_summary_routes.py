from __future__ import annotations

from pathlib import Path

import pytest

from app.scraper import config, db
from app.scraper.download_state import CaseDownloadState
from app.scraper.error_codes import ErrorCode
from tests.test_download_state import _configure_temp_paths, _create_run_and_case
from tests.test_runs_api_db import _reload_main_module


def _insert_case(csv_version_id: int, token_suffix: str) -> int:
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
                f"RAW-{token_suffix}",
                f"TOK-{token_suffix}",
                f"Title {token_suffix}",
                f"CASE-{token_suffix}",
                "Court",
                "Category",
                "2024-05-01",
                "unreported_judgments",
                csv_version_id,
                csv_version_id,
            ),
        )
    return int(cursor.lastrowid)


def _configure_metadata_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_dir = tmp_path / "data"
    monkeypatch.setattr(config, "METADATA_FILE", data_dir / "metadata.json")


def _seed_downloads_for_run(run_id: int, base_case_id: int) -> None:
    conn = db.get_connection()
    run_row = conn.execute("SELECT csv_version_id FROM runs WHERE id = ?", (run_id,)).fetchone()
    csv_version_id = int(run_row["csv_version_id"])

    failed_case_id = _insert_case(csv_version_id, "FAIL")
    skipped_case_id = _insert_case(csv_version_id, "SKIP")

    downloaded = CaseDownloadState.start(
        run_id=run_id,
        case_id=base_case_id,
        box_url="https://example.com/one.pdf",
    )
    downloaded.mark_downloaded(
        file_path="/tmp/one.pdf",
        file_size_bytes=1000,
        box_url="https://example.com/one.pdf",
    )

    failed = CaseDownloadState.start(
        run_id=run_id,
        case_id=failed_case_id,
        box_url="https://example.com/two.pdf",
    )
    failed.mark_failed(error_code=ErrorCode.NETWORK, error_message="network down")

    skipped = CaseDownloadState.start(
        run_id=run_id,
        case_id=skipped_case_id,
        box_url="https://example.com/three.pdf",
    )
    skipped.mark_skipped("already_downloaded")


def test_api_db_run_download_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    _configure_metadata_file(tmp_path, monkeypatch)
    db.initialize_schema()

    ids = _create_run_and_case()
    _seed_downloads_for_run(ids["run_id"], ids["case_id"])

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get(f"/api/db/runs/{ids['run_id']}/download-summary")
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["ok"] is True
    assert data["run_id"] == ids["run_id"]
    assert data["status_counts"]["downloaded"] == 1
    assert data["status_counts"]["failed"] == 1
    assert data["status_counts"]["skipped"] == 1
    assert data["fail_reasons"][ErrorCode.NETWORK] == 1
    assert data["skip_reasons"]["already_downloaded"] == 1


def test_api_db_latest_run_download_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    _configure_metadata_file(tmp_path, monkeypatch)
    db.initialize_schema()

    ids = _create_run_and_case()
    _seed_downloads_for_run(ids["run_id"], ids["case_id"])

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get("/api/db/runs/latest/download-summary")
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["ok"] is True
    assert data["run_id"] == ids["run_id"]
    assert data["status_counts"]["downloaded"] == 1


def test_api_db_run_download_summary_unknown_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    _configure_metadata_file(tmp_path, monkeypatch)
    db.initialize_schema()

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get("/api/db/runs/999/download-summary")
    assert resp.status_code == 404
    data = resp.get_json()
    assert data["ok"] is False
    assert data["error"] == "run_not_found"


def test_api_db_latest_download_summary_no_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    _configure_metadata_file(tmp_path, monkeypatch)
    db.initialize_schema()

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get("/api/db/runs/latest/download-summary")
    assert resp.status_code == 404
    data = resp.get_json()
    assert data["ok"] is False
    assert data["error"] == "no_runs"


def test_api_db_download_summary_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    _configure_metadata_file(tmp_path, monkeypatch)
    db.initialize_schema()
    monkeypatch.setenv("BAILIIKC_USE_DB_REPORTING", "0")

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get("/api/db/runs/latest/download-summary")
    assert resp.status_code == 404
    data = resp.get_json()
    assert data["ok"] is False
    assert data["error"] == "db_reporting_disabled"
