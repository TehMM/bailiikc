from pathlib import Path

import pytest

from app.scraper import cases_index, csv_sync, db, sources
from tests.test_csv_sync_and_db import _DummySession, _configure_temp_paths


def test_public_registers_csv_sync_populates_cases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    sample_csv = tmp_path / "public_registers.csv"
    sample_csv.write_text(
        "\n".join(
            [
                "Register Type,Name,Reference,Date",
                "Notaries Public,Jane Doe,NP-123,2024-01-01",
                "Justices of the Peace,John Smith,JP-5,2023-12-12",
            ]
        ),
        encoding="utf-8",
    )

    session = _DummySession(sample_csv.read_bytes())

    result = csv_sync.sync_csv(
        "http://example.com/public_registers.csv",
        session=session,
        source=sources.PUBLIC_REGISTERS,
    )

    assert result.source == sources.PUBLIC_REGISTERS
    assert result.row_count == 2
    assert Path(result.csv_path).is_file()

    conn = db.get_connection()
    rows = conn.execute(
        "SELECT title, subject, category, court, source, cause_number, sort_judgment_date FROM cases"
    ).fetchall()
    assert len(rows) == 2
    titles = {row["title"] for row in rows}
    assert {"Jane Doe", "John Smith"} == titles
    assert all(row["source"] == sources.PUBLIC_REGISTERS for row in rows)
    assert all(row["court"] == "Public Register" for row in rows)
    assert {"NP-123", "JP-5"} == {row["cause_number"] for row in rows}
    assert all(row["sort_judgment_date"] for row in rows)

    cases_index.CASES_BY_ACTION.clear()
    cases_index.load_cases_from_csv(result.csv_path, source=sources.PUBLIC_REGISTERS, csv_version_id=result.version_id)
    assert cases_index.CASES_BY_SOURCE[sources.PUBLIC_REGISTERS]


def test_public_registers_skips_rows_missing_name_and_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    sample_csv = tmp_path / "public_registers.csv"
    sample_csv.write_text(
        "\n".join(
            [
                "Register Type,Name,Reference,Date",
                "Notaries Public,,,2024-01-01",  # missing both name and reference
                "Justices of the Peace,Valid Name,JP-10,2024-02-02",
            ]
        ),
        encoding="utf-8",
    )

    session = _DummySession(sample_csv.read_bytes())

    result = csv_sync.sync_csv(
        "http://example.com/public_registers.csv",
        session=session,
        source=sources.PUBLIC_REGISTERS,
    )

    assert result.row_count == 2

    conn = db.get_connection()
    rows = conn.execute("SELECT action_token_norm, title FROM cases").fetchall()
    assert len(rows) == 1
    assert rows[0]["title"] == "Valid Name"
