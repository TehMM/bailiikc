import json
from pathlib import Path

import pytest

from app.scraper import config, db, run, sources
from app.scraper.run_creation import create_run_with_source
from tests.test_runs_api_db import _configure_temp_paths, _reload_main_module


def test_webhook_accepts_explicit_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setenv("BAILIIKC_USE_DB_REPORTING", "1")
    monkeypatch.setattr(config, "WEBHOOK_SHARED_SECRET", "secret")

    db.initialize_schema()
    csv_version_id = db.record_csv_version(
        fetched_at="2024-01-01T00:00:00Z",
        source_url="http://example.com/csv",
        sha256="abc",
        row_count=1,
        file_path="judgments.csv",
    )

    def _fake_run_scrape(*_, **kwargs):
        run_id = create_run_with_source(
            trigger=kwargs.get("trigger", "webhook"),
            mode=kwargs.get("scrape_mode", "new"),
            csv_version_id=csv_version_id,
            target_source=kwargs.get("target_source"),
            extra_params={"start_message": kwargs.get("start_message")},
        )
        return {"run_id": run_id, "csv_version_id": csv_version_id}

    monkeypatch.setattr(run, "run_scrape", _fake_run_scrape)

    main = _reload_main_module()
    client = main.app.test_client()

    resp = client.post(
        "/webhook/changedetection",
        json={"target_source": sources.PUBLIC_REGISTERS, "new_limit": 1, "mode": "new"},
        headers={"X-Webhook-Token": "secret"},
    )

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    run_id = payload["run_id"]

    conn = db.get_connection()
    cursor = conn.execute("SELECT params_json FROM runs WHERE id = ?", (run_id,))
    row = cursor.fetchone()
    assert row is not None
    params = json.loads(row["params_json"])
    assert params["target_source"] == sources.PUBLIC_REGISTERS

