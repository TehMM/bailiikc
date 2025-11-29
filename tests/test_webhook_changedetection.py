import importlib
import sys
from pathlib import Path

import pytest

from app.scraper import config, db


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


def test_webhook_disabled(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(config, "WEBHOOK_SHARED_SECRET", "")

    main = _reload_main_module()

    client = main.app.test_client()
    resp = client.post("/webhook/changedetection")

    assert resp.status_code in {403, 404}
    data = resp.get_json()
    assert data["ok"] is False
    assert data.get("error") == "webhook_disabled"


def test_webhook_rejects_invalid_token(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(config, "WEBHOOK_SHARED_SECRET", "secret-token")

    main = _reload_main_module()

    client = main.app.test_client()
    resp = client.post("/webhook/changedetection", headers={"X-Webhook-Token": "wrong"})

    assert resp.status_code == 403
    data = resp.get_json()
    assert data["ok"] is False
    assert data.get("error") == "invalid_token"


def test_webhook_rejects_invalid_payload(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(config, "WEBHOOK_SHARED_SECRET", "secret-token")

    main = _reload_main_module()

    client = main.app.test_client()
    resp = client.post(
        "/webhook/changedetection",
        json={"mode": "resume", "target_source": "unknown"},
        headers={"X-Webhook-Token": "secret-token"},
    )

    assert resp.status_code == 400
    data = resp.get_json()
    assert data["ok"] is False
    assert data.get("error") == "invalid_params"


def test_webhook_runs_scrape(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(config, "WEBHOOK_SHARED_SECRET", "secret-token")

    main = _reload_main_module()

    calls: dict[str, object] = {}

    def fake_run_scrape(*args, **kwargs):
        calls["kwargs"] = kwargs
        return {
            "run_id": 123,
            "csv_version_id": 9,
            "processed": 3,
            "downloaded": 2,
            "skipped": 0,
            "failed": 1,
        }

    monkeypatch.setattr(main, "run_scrape", fake_run_scrape)

    client = main.app.test_client()
    resp = client.post(
        "/webhook/changedetection?new_limit=99&mode=resume",
        json={
            "target_source": "unreported_judgments",
            "mode": "new",
            "new_limit": 3,
        },
        headers={"X-Webhook-Token": "secret-token"},
    )

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data.get("run_id") == 123
    assert data.get("mode") == "new"

    assert "kwargs" in calls
    kwargs = calls["kwargs"]
    assert kwargs["trigger"] == "webhook"
    assert kwargs["scrape_mode"] == "new"
    assert kwargs["new_limit"] == 3
    assert kwargs["row_limit"] == 3
    assert kwargs["limit_pages"] == [0]


def test_webhook_clamps_limit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(config, "WEBHOOK_SHARED_SECRET", "secret-token")
    monkeypatch.setattr(config, "WEBHOOK_NEW_LIMIT_MAX", 5)

    main = _reload_main_module()

    calls: dict[str, object] = {}

    def fake_run_scrape(*args, **kwargs):
        calls["kwargs"] = kwargs
        return {"run_id": 5, "csv_version_id": 2}

    monkeypatch.setattr(main, "run_scrape", fake_run_scrape)

    client = main.app.test_client()
    resp = client.post(
        "/webhook/changedetection",
        json={
            "target_source": "unreported_judgments",
            "mode": "new",
            "new_limit": 50,
        },
        headers={"X-Webhook-Token": "secret-token"},
    )

    assert resp.status_code == 200
    assert calls["kwargs"]["new_limit"] == main.WEBHOOK_LIMIT_MAX
    assert calls["kwargs"]["row_limit"] == main.WEBHOOK_LIMIT_MAX
