from pathlib import Path

import pytest

from app.scraper import config, run, sources
from tests.test_download_state import _configure_temp_paths


def _prepare_cli(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(run, "validate_runtime_config", lambda *_, **__: None)


def test_cli_defaults_to_default_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _prepare_cli(monkeypatch, tmp_path)

    captured: dict[str, str | None] = {}

    def _fake_run_scrape(*_, **kwargs):
        captured["target_source"] = kwargs.get("target_source")

    monkeypatch.setattr(run, "run_scrape", _fake_run_scrape)

    run._cli_entrypoint(["--base-url", str(config.DEFAULT_BASE_URL)])

    assert captured["target_source"] == sources.DEFAULT_SOURCE


def test_cli_accepts_public_registers_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepare_cli(monkeypatch, tmp_path)

    captured: dict[str, str | None] = {}

    def _fake_run_scrape(*_, **kwargs):
        captured["target_source"] = kwargs.get("target_source")

    monkeypatch.setattr(run, "run_scrape", _fake_run_scrape)

    run._cli_entrypoint(
        ["--base-url", str(config.DEFAULT_BASE_URL), "--source", sources.PUBLIC_REGISTERS]
    )

    assert captured["target_source"] == sources.PUBLIC_REGISTERS

