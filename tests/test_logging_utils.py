from app.scraper import logging_utils


def test_scraper_event_label_and_phase(monkeypatch):
    events: list[str] = []
    monkeypatch.setattr(logging_utils, "log_line", lambda msg: events.append(msg))

    logging_utils._scraper_event("state", phase="download_executor", kind="summary")

    assert events
    line = events[-1]
    assert line.startswith("[SCRAPER][STATE]")
    assert "phase='download_executor'" in line
    assert "kind='summary'" in line
