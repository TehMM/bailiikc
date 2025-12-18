from __future__ import annotations

from pathlib import Path

import pytest

from app.scraper import cases_index, config, csv_sync, db, db_reporting, run, sources
from app.scraper.download_executor import DownloadExecutor
from app.scraper.download_state import CaseDownloadState
from app.scraper.run import handle_dl_bfile_from_ajax
from tests.test_runs_api_db import _configure_temp_paths


def _insert_public_register_case(conn, csv_version_id: int, token_suffix: str) -> int:
    cursor = conn.execute(
        """
        INSERT INTO cases (
            action_token_raw, action_token_norm, title, subject, cause_number,
            court, category, judgment_date, sort_judgment_date, is_criminal,
            is_active, source, first_seen_version_id, last_seen_version_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1, ?, ?, ?)
        """,
        (
            f"PR-RAW-{token_suffix}",
            f"PRNORM-{token_suffix}",
            f"Public Title {token_suffix}",
            f"Public Subject {token_suffix}",
            f"PR-{token_suffix}",
            "Public Court",
            "Public Category",
            "2024-01-01",
            "2024-01-01",
            sources.PUBLIC_REGISTERS,
            csv_version_id,
            csv_version_id,
        ),
    )
    return int(cursor.lastrowid)


def test_run_scrape_records_public_register_download(
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
        file_path=str(tmp_path / "public-registers.csv"),
    )

    conn = db.get_connection()
    with conn:
        case_id = _insert_public_register_case(conn, csv_version_id, "ONE")

    sync_result = csv_sync.CsvSyncResult(
        version_id=csv_version_id,
        is_new_version=True,
        new_case_ids=[case_id],
        changed_case_ids=[],
        removed_case_ids=[],
        csv_path=str(tmp_path / "public-registers.csv"),
        row_count=1,
        source=sources.PUBLIC_REGISTERS,
    )

    monkeypatch.setattr(run, "csv_sync", run.csv_sync)
    monkeypatch.setattr(run.csv_sync, "sync_csv", lambda *_, **__: sync_result)

    def _fake_run_attempt(*, selectors, run_id, target_source, **kwargs):
        assert target_source == sources.PUBLIC_REGISTERS
        assert selectors.table_selector == "#public-registers"

        cases_index.load_cases_index_from_db(
            source=target_source, csv_version_id=csv_version_id
        )

        download_executor = DownloadExecutor(1)
        box_url = "https://example.com/PRNORM-ONE.pdf"
        case = cases_index.find_case_by_fname("PRNORM-ONE", source=target_source)
        case_context = {
            "case": case,
            "metadata_entry": None,
            "slug": "PRNORM-ONE",
            "raw": "PRNORM-ONE",
            "page_index": 0,
            "row_index": 0,
        }

        state = CaseDownloadState.start(run_id=run_id, case_id=case_id, box_url=box_url)
        try:
            result, info = handle_dl_bfile_from_ajax(
                mode="new",
                fname="PRNORM-ONE",
                box_url=box_url,
                downloads_dir=config.PDF_DIR,
                cases_by_action=cases_index.CASES_BY_ACTION,
                processed_this_run=set(),
                checkpoint=None,
                metadata={},
                http_client=None,
                case_context=case_context,
                fid=None,
                run_id=run_id,
                download_executor=download_executor,
                source=target_source,
            )
            assert result == "downloaded"
            state.mark_downloaded(
                file_path=info.get("file_path"),
                file_size_bytes=info.get("file_size_bytes"),
                box_url=info.get("box_url"),
            )
        finally:
            download_executor.shutdown()

        return {
            "base_url": kwargs.get("base_url", ""),
            "processed": 1,
            "downloaded": 1,
            "failed": 0,
            "skipped": 0,
            "inspected_rows": 1,
            "total_cases": 1,
            "log_file": str(kwargs.get("log_path")),
            "scrape_mode": kwargs.get("scrape_mode", "new"),
            "skip_reasons": {},
            "fail_reasons": {},
        }

    monkeypatch.setattr(run, "_run_scrape_attempt", _fake_run_attempt)

    summary = run.run_scrape(target_source=sources.PUBLIC_REGISTERS, scrape_mode="new")

    assert summary["downloaded"] == 1
    assert summary["target_source"] == sources.PUBLIC_REGISTERS

    downloads = conn.execute(
        "SELECT run_id, case_id, status, box_url_last FROM downloads"
    ).fetchall()
    assert len(downloads) == 1
    assert downloads[0]["run_id"] == summary["run_id"]
    assert downloads[0]["case_id"] == case_id
    assert downloads[0]["status"] == "downloaded"
    assert downloads[0]["box_url_last"] == "https://example.com/PRNORM-ONE.pdf"

    coverage = db_reporting.get_run_coverage(summary["run_id"])
    assert coverage["cases_total"] == 1
    assert coverage["cases_downloaded"] == 1
    assert coverage["run_health"] == "ok"
