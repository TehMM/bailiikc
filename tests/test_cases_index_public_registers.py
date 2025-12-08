from pathlib import Path

import pytest

from app.scraper import cases_index, db, sources
from tests.test_csv_sync_and_db import _configure_temp_paths


def test_load_cases_index_for_public_registers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    csv_version_id = db.record_csv_version(
        fetched_at="2024-01-01T00:00:00Z",
        source_url="http://example.com/public_registers.csv",
        sha256="deadbeef",
        row_count=2,
        file_path=str(tmp_path / "public_registers.csv"),
    )

    conn = db.get_connection()
    conn.execute(
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
    conn.execute(
        """
        INSERT INTO cases (
            action_token_raw, action_token_norm, title, subject, cause_number,
            court, category, judgment_date, sort_judgment_date, is_criminal,
            is_active, source, first_seen_version_id, last_seen_version_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
        """,
        (
            "JP JP-5",
            "JPJP5",
            "John Smith",
            "Justices of the Peace - JP-5",
            "JP-5",
            "Public Register",
            "Justices of the Peace",
            "2023-12-12",
            "2023-12-12",
            0,
            sources.PUBLIC_REGISTERS,
            csv_version_id,
            csv_version_id,
        ),
    )
    conn.commit()

    cases_index.load_cases_index_from_db(
        source=sources.PUBLIC_REGISTERS, csv_version_id=csv_version_id
    )

    assert len(cases_index.CASES_ALL) == 2
    assert cases_index.CASES_BY_SOURCE[sources.PUBLIC_REGISTERS]
    assert cases_index.AJAX_FNAME_INDEX == {}
    assert {case.title for case in cases_index.CASES_ALL} == {"Jane Doe", "John Smith"}
