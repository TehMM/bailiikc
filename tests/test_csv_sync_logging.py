import pytest

from app.scraper import csv_sync


def test_parse_judgment_date_logs_unparsed(monkeypatch: pytest.MonkeyPatch) -> None:
    messages: list[str] = []

    def fake_log_line(msg: str) -> None:
        messages.append(msg)

    monkeypatch.setattr(csv_sync, "log_line", fake_log_line)

    original = "nonsense-date-format"
    result = csv_sync.parse_judgment_date(original)

    assert result == original
    assert any("[CSV][WARN] Unable to normalise judgment date" in m for m in messages)
