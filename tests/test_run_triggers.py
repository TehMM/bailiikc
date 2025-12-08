import importlib
import sys
from pathlib import Path
from typing import Any, Dict

import pytest

from app.scraper import config, db, sources


def _configure_temp_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "bailiikc.db"

    monkeypatch.setattr(config, "DATA_DIR", data_dir)
    monkeypatch.setattr(config, "PDF_DIR", data_dir / "pdfs")
    monkeypatch.setattr(config, "LOG_DIR", data_dir / "logs")
    monkeypatch.setattr(config, "DOWNLOADS_LOG", data_dir / "downloads.jsonl")
    monkeypatch.setattr(config, "SUMMARY_FILE", data_dir / "last_summary.json")
    monkeypatch.setattr(config, "LOG_FILE", data_dir / "logs" / "latest.log")
    monkeypatch.setattr(config, "DB_PATH", db_path)
    monkeypatch.setattr(db, "DB_PATH", db_path)


def _reload_main_module():
    if "app.main" in sys.modules:
        del sys.modules["app.main"]
    return importlib.import_module("app.main")


class _DummyThread:
    """Thread stub that runs the target synchronously in tests."""

    def __init__(self, target, daemon: bool = True):
        self._target = target
        self.daemon = daemon

    def start(self) -> None:
        self._target()


def test_ui_scrape_uses_ui_trigger(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    main = _reload_main_module()

    calls: Dict[str, Any] = {}

    def fake_run_scrape(*args, **kwargs):
        calls["args"] = args
        calls["kwargs"] = kwargs
        return {"log_file": "dummy.log", "run_id": 1}

    monkeypatch.setattr(main, "run_scrape", fake_run_scrape)
    monkeypatch.setattr(main.threading, "Thread", _DummyThread)

    client = main.app.test_client()
    resp = client.post(
        "/scrape",
        data={
            "base_url": "https://example.com",
            "page_wait": "5",
            "per_download_delay": "0.5",
            "scrape_mode": "new",
            "new_limit": "10",
            "max_retries": "1",
            "resume_mode": "none",
        },
    )

    assert resp.status_code == 302
    assert "Location" in resp.headers

    assert "kwargs" in calls
    assert calls["kwargs"]["trigger"] == "ui"
    assert calls["kwargs"].get("target_source") == config.DEFAULT_SOURCE


def test_webhook_uses_webhook_trigger(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    monkeypatch.setattr(config, "WEBHOOK_SHARED_SECRET", "test-secret")

    main = _reload_main_module()

    calls: Dict[str, Any] = {}

    def fake_run_scrape(*args, **kwargs):
        calls["args"] = args
        calls["kwargs"] = kwargs
        return {"log_file": "dummy.log", "run_id": 1}

    monkeypatch.setattr(main, "run_scrape", fake_run_scrape)

    client = main.app.test_client()
    resp = client.post(
        "/webhook/changedetection",
        json={"target_source": "unreported_judgments", "mode": "new", "new_limit": 5},
        headers={"X-Webhook-Token": "test-secret"},
    )

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data.get("run_id") == 1

    assert "kwargs" in calls
    kwargs = calls["kwargs"]
    assert kwargs["trigger"] == "webhook"
    assert kwargs["scrape_mode"] == "new"
    assert kwargs["new_limit"] == 5
    assert kwargs["row_limit"] == 5
    assert kwargs["limit_pages"] == [0]
    assert kwargs["target_source"] == sources.UNREPORTED_JUDGMENTS
