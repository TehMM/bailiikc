from pathlib import Path

import pytest

from app.scraper import cases_index, db, run, sources
from tests.test_csv_sync_and_db import _configure_temp_paths


def test_resolve_ajax_case_context_uses_source_mapping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
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
        conn.execute(
            """
            INSERT INTO cases (
                action_token_raw, action_token_norm, title, subject, is_criminal,
                is_active, source, first_seen_version_id, last_seen_version_id
            ) VALUES (?, ?, ?, ?, 0, 1, ?, ?, ?)
            """,
            (
                "Notaries Public NP-1",
                "NOTARIESPUBLICNP1",
                "Jane Doe",
                "Notaries Public - NP-1",
                sources.PUBLIC_REGISTERS,
                csv_version_id,
                csv_version_id,
            ),
        )

    cases_index.load_cases_index_from_db(
        source=sources.PUBLIC_REGISTERS, csv_version_id=csv_version_id
    )

    case_context, canonical_token, db_token_norm, norm_fname = run.resolve_ajax_case_context(
        "NOTARIESPUBLICNP1",
        fid_param=None,
        pending_by_fname={},
        target_source=sources.PUBLIC_REGISTERS,
    )

    assert case_context is not None
    assert case_context["case"].action == "NOTARIESPUBLICNP1"
    assert canonical_token == "NOTARIESPUBLICNP1"
    assert norm_fname == "NOTARIESPUBLICNP1"

    resolved_case_id = db.get_case_id_by_token_norm(
        sources.PUBLIC_REGISTERS, db_token_norm
    )
    assert resolved_case_id is not None


