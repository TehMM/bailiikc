import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.scraper import db, db_reporting
from app.scraper.download_state import CaseDownloadState
from app.scraper.error_codes import ErrorCode

from tests.test_download_state import _configure_temp_paths, _create_run_and_case


def _insert_case(csv_version_id: int, token_suffix: str = "EXTRA") -> int:
    """Insert a minimal case row for testing and return its identifier."""

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
                "2024-03-02",
                "unreported_judgments",
                csv_version_id,
                csv_version_id,
            ),
        )
    return int(cursor.lastrowid)


def test_summarise_downloads_for_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    ids = _create_run_and_case()
    run_id = ids["run_id"]

    run_row = db.get_connection().execute(
        "SELECT csv_version_id FROM runs WHERE id = ?", (run_id,)
    ).fetchone()
    csv_version_id = int(run_row["csv_version_id"])

    downloaded_case_id = ids["case_id"]
    failed_case_id = _insert_case(csv_version_id, token_suffix="FAIL")
    skipped_case_id = _insert_case(csv_version_id, token_suffix="SKIP")

    downloaded = CaseDownloadState.start(
        run_id=run_id,
        case_id=downloaded_case_id,
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
    failed.mark_failed(
        error_code=ErrorCode.NETWORK,
        error_message="network down",
    )

    skipped = CaseDownloadState.start(
        run_id=run_id,
        case_id=skipped_case_id,
        box_url="https://example.com/three.pdf",
    )
    skipped.mark_skipped("already_downloaded")

    summary = db_reporting.summarise_downloads_for_run(run_id)

    assert summary.run_id == run_id
    assert summary.status_counts["downloaded"] == 1
    assert summary.status_counts["failed"] == 1
    assert summary.status_counts["skipped"] == 1

    assert summary.fail_reasons[ErrorCode.NETWORK] == 1
    assert summary.skip_reasons["already_downloaded"] == 1
