import pytest

from app.scraper import config, sources


def test_get_source_runtime_defaults_unreported(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BAILIIKC_UJ_BASE_URL", raising=False)
    monkeypatch.delenv("BAILIIKC_UJ_CSV_URL", raising=False)

    runtime = config.get_source_runtime(sources.UNREPORTED_JUDGMENTS)
    assert runtime.base_url == config.DEFAULT_BASE_URL
    assert runtime.csv_url == config.CSV_URL


def test_get_source_runtime_env_overrides_unreported(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BAILIIKC_UJ_BASE_URL", "https://example.com/uj")
    monkeypatch.setenv("BAILIIKC_UJ_CSV_URL", "https://example.com/uj.csv")

    runtime = config.get_source_runtime(sources.UNREPORTED_JUDGMENTS)
    assert runtime.base_url == "https://example.com/uj"
    assert runtime.csv_url == "https://example.com/uj.csv"


def test_get_source_runtime_public_registers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BAILIIKC_PR_BASE_URL", "https://example.com/pr")
    monkeypatch.setenv("BAILIIKC_PR_CSV_URL", "https://example.com/pr.csv")

    runtime = config.get_source_runtime(sources.PUBLIC_REGISTERS)
    assert runtime.base_url == "https://example.com/pr"
    assert runtime.csv_url == "https://example.com/pr.csv"


def test_get_source_runtime_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BAILIIKC_UJ_BASE_URL", raising=False)
    monkeypatch.delenv("BAILIIKC_UJ_CSV_URL", raising=False)
    monkeypatch.delenv("BAILIIKC_PR_BASE_URL", raising=False)
    monkeypatch.delenv("BAILIIKC_PR_CSV_URL", raising=False)

    runtime_default = config.get_source_runtime(None)
    runtime_uj_alias = config.get_source_runtime("uj")
    runtime_unreported_alias = config.get_source_runtime("unreported")

    assert runtime_default == runtime_uj_alias == runtime_unreported_alias
    assert runtime_default.base_url == config.DEFAULT_BASE_URL
    assert runtime_default.csv_url == config.CSV_URL
