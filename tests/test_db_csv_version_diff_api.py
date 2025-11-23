from pathlib import Path

import pytest

from app.scraper import db, db_reporting
from tests.test_runs_api_db import _configure_temp_paths, _reload_main_module


def _seed_versions_and_cases() -> tuple[int, int]:
    """Create CSV versions plus cases modelling new and removed entries."""

    conn = db.get_connection()
    with conn:
        v1 = db.record_csv_version(
            fetched_at="2024-01-01T00:00:00Z",
            source_url="http://example.com/csv1",
            sha256="sha1",
            row_count=2,
            file_path="judgments_v1.csv",
        )
        v2 = db.record_csv_version(
            fetched_at="2024-01-02T00:00:00Z",
            source_url="http://example.com/csv2",
            sha256="sha2",
            row_count=2,
            file_path="judgments_v2.csv",
        )

        conn.execute(
            """
            INSERT INTO cases (
                action_token_raw, action_token_norm, title, cause_number, court,
                category, judgment_date, is_criminal, is_active, source,
                first_seen_version_id, last_seen_version_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, 'unreported_judgments', ?, ?)
            """,
            ("RAW-A", "NORM-A", "Case A", "FSD 1", "FSD", "Cat", "2024-01-01", v1, v2),
        )

        conn.execute(
            """
            INSERT INTO cases (
                action_token_raw, action_token_norm, title, cause_number, court,
                category, judgment_date, is_criminal, is_active, source,
                first_seen_version_id, last_seen_version_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, 'unreported_judgments', ?, ?)
            """,
            ("RAW-B", "NORM-B", "Case B", "FSD 2", "FSD", "Cat", "2024-01-02", v2, v2),
        )

        conn.execute(
            """
            INSERT INTO cases (
                action_token_raw, action_token_norm, title, cause_number, court,
                category, judgment_date, is_criminal, is_active, source,
                first_seen_version_id, last_seen_version_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 'unreported_judgments', ?, ?)
            """,
            ("RAW-C", "NORM-C", "Case C", "FSD 3", "FSD", "Cat", "2023-12-31", v1, v2),
        )

    return v1, v2


def test_get_case_diff_for_csv_version_returns_new_and_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    _, v2 = _seed_versions_and_cases()

    diff = db_reporting.get_case_diff_for_csv_version(v2)

    assert diff["csv_version_id"] == v2
    assert diff["new_count"] == 1
    assert diff["removed_count"] == 1

    new_tokens = {c["action_token_norm"] for c in diff["new_cases"]}
    removed_tokens = {c["action_token_norm"] for c in diff["removed_cases"]}

    assert new_tokens == {"NORM-B"}
    assert removed_tokens == {"NORM-C"}


def test_get_case_diff_for_csv_version_missing_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    with pytest.raises(db_reporting.CsvVersionNotFoundError):
        db_reporting.get_case_diff_for_csv_version(999)


def test_api_db_case_diff_for_csv_version_returns_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    _, v2 = _seed_versions_and_cases()

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get(f"/api/db/csv_versions/{v2}/case-diff")
    assert resp.status_code == 200

    payload = resp.get_json()
    assert payload["ok"] is True
    assert payload["csv_version_id"] == v2
    assert payload["new_count"] == 1
    assert payload["removed_count"] == 1
    assert {c["action_token_norm"] for c in payload["new_cases"]} == {"NORM-B"}
    assert {c["action_token_norm"] for c in payload["removed_cases"]} == {"NORM-C"}


def test_api_db_case_diff_for_csv_version_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get("/api/db/csv_versions/999/case-diff")
    assert resp.status_code == 404
    payload = resp.get_json()
    assert payload["ok"] is False
    assert payload["error"] == "csv_version_not_found"
