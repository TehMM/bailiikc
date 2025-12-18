import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from app.scraper import config, csv_sync, db, run, sources
from app.scraper.csv_sync import CsvSyncResult
from tests.test_download_state import _configure_temp_paths


class _StubSyncResult(CsvSyncResult):
    def __init__(self, version_id: int, source: str, csv_path: str = ""):
        super().__init__(
            version_id=version_id,
            is_new_version=True,
            new_case_ids=[],
            changed_case_ids=[],
            removed_case_ids=[],
            csv_path=csv_path,
            row_count=0,
            source=source,
        )


@pytest.fixture()
def temp_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    data_dir = config.DATA_DIR
    monkeypatch.setattr(config, "METADATA_FILE", data_dir / "metadata.json")
    monkeypatch.setattr(config, "CONFIG_FILE", data_dir / "config.txt")
    monkeypatch.setattr(config, "CHECKPOINT_PATH", data_dir / "state.json")
    monkeypatch.setattr(config, "RUN_STATE_FILE", data_dir / "run_state.json")
    monkeypatch.setattr(config, "LOG_FILE", data_dir / "logs" / "latest.log")


def test_run_scrape_persists_normalized_target_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, temp_config: None
) -> None:
    db.initialize_schema()

    csv_path = tmp_path / "judgments.csv"
    csv_path.write_text("header1\n", encoding="utf-8")
    version_id = db.record_csv_version(
        fetched_at="2024-03-01T00:00:00Z",
        source_url="http://example.com/csv",
        sha256="abc123",
        row_count=1,
        file_path=str(csv_path),
    )

    sync_calls: dict[str, Any] = {}

    def fake_sync_csv(source_url: str, session: Any, *, source: str) -> CsvSyncResult:  # noqa: ARG001
        sync_calls["source"] = source
        return _StubSyncResult(version_id=version_id, source=source, csv_path=str(csv_path))

    monkeypatch.setattr(csv_sync, "sync_csv", fake_sync_csv)
    monkeypatch.setattr(csv_sync, "build_http_session", lambda: SimpleNamespace())

    def fake_run_with_retries(func, max_retries: int):  # noqa: ANN001, D417
        return func()

    monkeypatch.setattr(run, "run_with_retries", fake_run_with_retries)
    monkeypatch.setattr(run, "_run_scrape_attempt", lambda **kwargs: {"log_file": "dummy"})

    summary = run.run_scrape(target_source="uj")
    assert summary

    cursor = db.get_connection().execute("SELECT params_json FROM runs LIMIT 1")
    row = cursor.fetchone()
    assert row is not None
    params = json.loads(row["params_json"])
    assert params["target_source"] == sources.UNREPORTED_JUDGMENTS
    assert sync_calls["source"] == sources.UNREPORTED_JUDGMENTS


def test_cli_entrypoint_passes_target_source(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, Any] = {}

    def fake_run_scrape(**kwargs: Any) -> dict[str, Any]:  # noqa: ANN401
        calls.update(kwargs)
        return {}

    monkeypatch.setattr(run, "run_scrape", fake_run_scrape)
    monkeypatch.setattr(run, "ensure_dirs", lambda: None)
    monkeypatch.setattr(run, "validate_runtime_config", lambda entrypoint, mode=None: None)  # noqa: ARG005

    run._cli_entrypoint(["--source", sources.UNREPORTED_JUDGMENTS])
    assert calls["target_source"] == sources.UNREPORTED_JUDGMENTS

    calls.clear()

    run._cli_entrypoint(["--source", sources.PUBLIC_REGISTERS])

    assert calls["target_source"] == sources.PUBLIC_REGISTERS


def test_run_scrape_uses_source_runtime(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, temp_config: None) -> None:
    db.initialize_schema()

    pr_base = "https://example.com/public-registers/"
    pr_csv = "https://example.com/public-registers.csv"

    monkeypatch.setenv("BAILIIKC_PR_BASE_URL", pr_base)
    monkeypatch.setenv("BAILIIKC_PR_CSV_URL", pr_csv)

    csv_path = tmp_path / "pr_judgments.csv"
    csv_path.write_text("header1\n", encoding="utf-8")
    version_id = db.record_csv_version(
        fetched_at="2024-03-01T00:00:00Z",
        source_url="http://example.com/csv",
        sha256="abc123",
        row_count=1,
        file_path=str(csv_path),
    )

    sync_calls: dict[str, Any] = {}
    attempt_calls: dict[str, Any] = {}

    def fake_sync_csv(source_url: str, session: Any, *, source: str) -> CsvSyncResult:  # noqa: ARG001
        sync_calls["source_url"] = source_url
        sync_calls["source"] = source
        return _StubSyncResult(version_id=version_id, source=source)

    monkeypatch.setattr(csv_sync, "sync_csv", fake_sync_csv)
    monkeypatch.setattr(csv_sync, "build_http_session", lambda: SimpleNamespace())

    def fake_run_with_retries(func, max_retries: int):  # noqa: ANN001, D417
        return func()

    def fake_run_attempt(**kwargs: Any) -> dict[str, Any]:  # noqa: ANN401
        attempt_calls.update(kwargs)
        return {"log_file": "dummy"}

    monkeypatch.setattr(run, "run_with_retries", fake_run_with_retries)
    monkeypatch.setattr(run, "_run_scrape_attempt", fake_run_attempt)

    summary = run.run_scrape(target_source=sources.PUBLIC_REGISTERS)
    assert summary

    assert sync_calls["source_url"] == pr_csv
    assert sync_calls["source"] == sources.PUBLIC_REGISTERS
    assert attempt_calls.get("base_url") == pr_base.strip()
    assert attempt_calls.get("csv_source") == pr_csv
    selectors = attempt_calls.get("selectors")
    assert selectors is not None
    assert getattr(selectors, "table_selector") == "#public-registers"


def test_run_scrape_passes_normalized_source_to_runtime(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, temp_config: None
) -> None:
    db.initialize_schema()

    csv_path = tmp_path / "pr_runtime.csv"
    csv_path.write_text("header1\n", encoding="utf-8")
    version_id = db.record_csv_version(
        fetched_at="2024-04-01T00:00:00Z",
        source_url="http://example.com/csv",
        sha256="def456",
        row_count=1,
        file_path=str(csv_path),
    )

    runtime = config.SourceRuntime(
        source=sources.PUBLIC_REGISTERS,
        base_url="https://example.com/runtime-pr/",
        csv_url="https://example.com/runtime-pr.csv",
        enable_ajax_capture=True,
        enable_box_downloads=True,
        builds_fname_index=False,
    )

    runtime_calls: dict[str, str] = {}
    sync_calls: dict[str, Any] = {}
    attempt_calls: dict[str, Any] = {}

    def fake_get_source_runtime(source: str | None) -> config.SourceRuntime:
        runtime_calls["source"] = source or ""
        return runtime

    def fake_sync_csv(source_url: str, session: Any, *, source: str) -> CsvSyncResult:  # noqa: ARG001
        sync_calls["source_url"] = source_url
        sync_calls["source"] = source
        return _StubSyncResult(version_id=version_id, source=source, csv_path=str(csv_path))

    monkeypatch.setattr(config, "get_source_runtime", fake_get_source_runtime)
    monkeypatch.setattr(csv_sync, "sync_csv", fake_sync_csv)
    monkeypatch.setattr(csv_sync, "build_http_session", lambda: SimpleNamespace())

    def fake_run_with_retries(func, max_retries: int):  # noqa: ANN001, D417
        return func()

    def fake_run_attempt(**kwargs: Any) -> dict[str, Any]:  # noqa: ANN401
        attempt_calls.update(kwargs)
        return {"log_file": "dummy"}

    monkeypatch.setattr(run, "run_with_retries", fake_run_with_retries)
    monkeypatch.setattr(run, "_run_scrape_attempt", fake_run_attempt)

    result = run.run_scrape(target_source="public-registers")
    assert result

    assert runtime_calls["source"] == sources.PUBLIC_REGISTERS
    assert sync_calls["source_url"] == runtime.csv_url
    assert sync_calls["source"] == sources.PUBLIC_REGISTERS
    assert attempt_calls.get("base_url") == runtime.base_url.strip()
    assert attempt_calls.get("csv_source") == str(csv_path)
