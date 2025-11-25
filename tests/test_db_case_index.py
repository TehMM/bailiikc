import sys
from pathlib import Path
from typing import Optional

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.scraper import cases_index, config, csv_sync, db, db_case_index


class _DummyResponse:
    def __init__(self, content: bytes):
        self.content = content
        self.headers = {}

    def raise_for_status(self) -> None:  # pragma: no cover - simple stub
        return None


class _DummySession:
    def __init__(self, content: bytes):
        self._content = content

    def get(self, url: str, timeout: Optional[tuple[int, int]] = None) -> _DummyResponse:  # noqa: ARG002
        return _DummyResponse(self._content)


def _configure_temp_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "bailiikc.db"
    monkeypatch.setattr(config, "DATA_DIR", data_dir)
    monkeypatch.setattr(config, "DB_PATH", db_path)
    monkeypatch.setattr(config, "PDF_DIR", data_dir / "pdfs")
    monkeypatch.setattr(db, "DB_PATH", db_path)


@pytest.fixture()
def sample_csv_bytes() -> bytes:
    sample_csv = Path(__file__).parent / "data" / "judgments_sample.csv"
    return sample_csv.read_bytes()


@pytest.fixture()
def populated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sample_csv_bytes: bytes) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()
    session = _DummySession(sample_csv_bytes)
    csv_sync.sync_csv("http://example.com/judgments.csv", session=session)


@pytest.mark.usefixtures("populated_db")
def test_load_case_index_from_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    index = db_case_index.load_case_index_from_db()
    assert index

    expected_token = "FSD0151202511062025ATPLIFESCIENCE"
    assert expected_token in index

    entry = index[expected_token]
    for key in [
        "action_token_raw",
        "action_token_norm",
        "title",
        "cause_number",
        "court",
        "category",
        "judgment_date",
        "is_criminal",
        "source",
    ]:
        assert key in entry
        assert entry[key] != ""


@pytest.mark.usefixtures("populated_db")
def test_db_index_matches_csv_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sample_csv = Path(__file__).parent / "data" / "judgments_sample.csv"
    monkeypatch.setenv("BAILIIKC_USE_DB_CASES", "0")
    cases_index.load_cases_from_csv(str(sample_csv))
    index_csv = dict(cases_index.CASES_BY_ACTION)

    index_db = db_case_index.load_case_index_from_db()

    assert set(index_csv.keys()) == set(index_db.keys())

    for token in index_csv.keys():
        csv_case = index_csv[token]
        db_case = index_db[token]
        assert csv_case.action == db_case["action_token_norm"]
        assert csv_case.title == db_case["title"]
        assert csv_case.cause_number == db_case["cause_number"]
        assert csv_case.court == db_case["court"]
        assert csv_case.category == db_case["category"]
        assert csv_case.judgment_date == db_case["judgment_date"]
