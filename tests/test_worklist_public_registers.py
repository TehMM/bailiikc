from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.scraper import db, sources, worklist
from tests.test_worklist_db_only import _configure_temp_paths


def _record_version(label: str) -> int:
    return db.record_csv_version(
        fetched_at=f"2024-01-01T00:00:00Z-{label}",
        source_url=f"http://example.com/{label}.csv",
        sha256=f"sha-{label}",
        row_count=1,
        file_path=f"/tmp/{label}.csv",
    )


def _insert_case(
    conn,
    *,
    token: str,
    title: str,
    source: str,
    first_seen: int,
    last_seen: int,
) -> int:
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
            token,
            token.replace(" ", ""),
            title,
            f"Cause {token}",
            "Court",
            "Category",
            "2024-01-01",
            source,
            first_seen,
            last_seen,
        ),
    )
    return int(cursor.lastrowid)


def test_worklists_are_source_scoped_for_public_registers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    previous_version = _record_version("prev")
    version_id = _record_version("current")

    conn = db.get_connection()
    with conn:
        pr_old = _insert_case(
            conn,
            token="PR-OLD",
            title="Public Old",
            source=sources.PUBLIC_REGISTERS,
            first_seen=previous_version,
            last_seen=version_id,
        )
        pr_new = _insert_case(
            conn,
            token="PR-NEW",
            title="Public New",
            source=sources.PUBLIC_REGISTERS,
            first_seen=version_id,
            last_seen=version_id,
        )
        _insert_case(
            conn,
            token="UJ-CASE",
            title="UJ Noise",
            source=sources.UNREPORTED_JUDGMENTS,
            first_seen=version_id,
            last_seen=version_id,
        )

    full_items = worklist.build_full_worklist(version_id, source=sources.PUBLIC_REGISTERS)
    assert {item.case_id for item in full_items} == {pr_old, pr_new}
    assert {item.source for item in full_items} == {sources.PUBLIC_REGISTERS}

    new_items = worklist.build_new_worklist(version_id, source="PUBLIC_REGISTERS")
    assert {item.case_id for item in new_items} == {pr_new}
    assert {item.source for item in new_items} == {sources.PUBLIC_REGISTERS}


def test_resume_worklist_filters_public_registers_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    version_id = _record_version("current")
    run_id = db.create_run(
        trigger="cli",
        mode="new",
        csv_version_id=version_id,
        params_json=json.dumps({"target_source": sources.PUBLIC_REGISTERS}),
    )
    conn = db.get_connection()
    with conn:
        pr_success = _insert_case(
            conn,
            token="PR-SUCCESS",
            title="PR Success",
            source=sources.PUBLIC_REGISTERS,
            first_seen=version_id,
            last_seen=version_id,
        )
        pr_failed = _insert_case(
            conn,
            token="PR-FAILED",
            title="PR Failed",
            source=sources.PUBLIC_REGISTERS,
            first_seen=version_id,
            last_seen=version_id,
        )
        uj_case = _insert_case(
            conn,
            token="UJ-FAILED",
            title="UJ Failed",
            source=sources.UNREPORTED_JUDGMENTS,
            first_seen=version_id,
            last_seen=version_id,
        )

        ts = "2024-01-02T00:00:00Z"
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
                    pr_success,
                    "downloaded",
                    1,
                    ts,
                    None,
                    None,
                    None,
                    None,
                    None,
                    ts,
                    ts,
                ),
                (
                    run_id,
                    pr_failed,
                    "failed",
                    1,
                    ts,
                    None,
                    None,
                    None,
                    "NETWORK",
                    "timeout",
                    ts,
                    ts,
                ),
                (
                    run_id,
                    uj_case,
                    "failed",
                    1,
                    ts,
                    None,
                    None,
                    None,
                    "NETWORK",
                    "timeout",
                    ts,
                    ts,
                ),
            ],
        )

    resume_items = worklist.build_resume_worklist(
        version_id, source=sources.PUBLIC_REGISTERS
    )
    assert {item.case_id for item in resume_items} == {pr_failed}
    assert {item.source for item in resume_items} == {sources.PUBLIC_REGISTERS}
