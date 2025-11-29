from __future__ import annotations

from pathlib import Path
import pytest

from app.scraper import config, db
from app.scraper import healthcheck
from tests.test_runs_api_db import _configure_temp_paths, _reload_main_module


def test_run_health_checks_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(config, "MIN_FREE_MB", 0)
    monkeypatch.setattr(healthcheck.consistency, "compare_latest_downloads_json_vs_db", lambda: {"ok": True})

    db.initialize_schema()

    result = healthcheck.run_health_checks(entrypoint="ui")
    assert result.ok is True
    assert result.checks["config"]["ok"] is True
    assert result.checks["filesystem"]["ok"] is True
    assert result.checks["database"]["ok"] is True


def test_run_health_checks_handles_invalid_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(config, "MIN_FREE_MB", -1)
    monkeypatch.setattr(healthcheck.consistency, "compare_latest_downloads_json_vs_db", lambda: {"ok": True})

    db.initialize_schema()

    result = healthcheck.run_health_checks(entrypoint="cli")
    assert result.ok is False
    assert result.checks["config"]["ok"] is False


def test_health_api_reports_status(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(config, "MIN_FREE_MB", 0)
    monkeypatch.setenv("BAILIIKC_USE_DB_REPORTING", "1")
    monkeypatch.setattr(healthcheck.consistency, "compare_latest_downloads_json_vs_db", lambda: {"ok": True})

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.get("/api/health")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    assert "filesystem" in payload["checks"]

    monkeypatch.setattr(db, "initialize_schema", lambda: (_ for _ in ()).throw(RuntimeError("db error")))

    resp_unhealthy = client.get("/api/health")
    assert resp_unhealthy.status_code == 503
    data_unhealthy = resp_unhealthy.get_json()
    assert data_unhealthy["ok"] is False
    assert data_unhealthy["checks"]["database"]["ok"] is False
