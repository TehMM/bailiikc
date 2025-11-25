from app.scraper import logging_utils


def test_scraper_event_swallows_logging_errors(monkeypatch):
    calls = []

    def boom(message):  # noqa: ANN001
        calls.append(message)
        raise RuntimeError("boom")

    monkeypatch.setattr(logging_utils, "log_line", boom)

    logging_utils._scraper_event("plan", example="value")

    assert calls, "scraper event should attempt to log even if logging raises"
