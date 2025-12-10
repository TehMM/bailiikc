from __future__ import annotations

from pathlib import Path

import pytest

from app.scraper import cases_index, config, db, sources
from app.scraper.download_state import CaseDownloadState
from app.scraper.run import handle_dl_bfile_from_ajax
from tests.test_csv_sync_and_db import _configure_temp_paths


def test_public_registers_download_links_case_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(config, "REPLAY_SKIP_NETWORK", True)
    db.initialize_schema()

    csv_version_id = db.record_csv_version(
        fetched_at="2024-01-01T00:00:00Z",
        source_url="http://example.com/public_registers.csv",
        sha256="deadbeef",
        row_count=1,
        file_path=str(tmp_path / "public_registers.csv"),
    )

    conn = db.get_connection()
    with conn:
        cursor = conn.execute(
            """
            INSERT INTO cases (
                action_token_raw, action_token_norm, title, subject, cause_number,
                court, category, judgment_date, sort_judgment_date, is_criminal,
                is_active, source, first_seen_version_id, last_seen_version_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
            """,
            (
                "Notaries Public NP-1",
                "NOTARIESPUBLICNP1",
                "Jane Doe",
                "Notaries Public - NP-1",
                "NP-1",
                "Public Register",
                "Notaries Public",
                "2024-01-01",
                "2024-01-01",
                0,
                sources.PUBLIC_REGISTERS,
                csv_version_id,
                csv_version_id,
            ),
        )
        case_id = int(cursor.lastrowid)

    cases_index.load_cases_index_from_db(
        source=sources.PUBLIC_REGISTERS, csv_version_id=csv_version_id
    )

    run_id = db.create_run(
        trigger="test",
        mode="new",
        csv_version_id=csv_version_id,
        params_json='{"target_source": "public_registers"}',
    )

    box_url = "http://example.com/file.pdf"
    state = CaseDownloadState.start(run_id=run_id, case_id=case_id, box_url=box_url)
    result, info = handle_dl_bfile_from_ajax(
        mode="new",
        fname="NOTARIESPUBLICNP1",
        box_url=box_url,
        downloads_dir=config.PDF_DIR,
        cases_by_action=cases_index.CASES_BY_ACTION,
        processed_this_run=set(),
        checkpoint=None,
        metadata={},
        http_client=None,
        case_context=None,
        fid=None,
        run_id=run_id,
        download_executor=None,
        source=sources.PUBLIC_REGISTERS,
    )

    assert result == "downloaded"
    state.mark_downloaded(
        file_path=info.get("file_path"),
        file_size_bytes=info.get("file_size_bytes"),
        box_url=info.get("box_url"),
    )

    row = conn.execute(
        "SELECT status, case_id FROM downloads WHERE run_id = ?", (run_id,)
    ).fetchone()
    assert row is not None
    assert row["case_id"] == case_id
    assert row["status"] == "downloaded"
