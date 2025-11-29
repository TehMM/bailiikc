from app.scraper import config
from app.scraper.config_validation import validate_runtime_config
import pytest


def test_replay_skip_network_forbidden_for_ui(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "REPLAY_SKIP_NETWORK", True)
    with pytest.raises(ValueError):
        validate_runtime_config("ui", mode="new")


def test_replay_skip_network_allowed_for_replay(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "REPLAY_SKIP_NETWORK", True)
    validate_runtime_config("replay")


def test_min_free_mb_negative(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "MIN_FREE_MB", -5)
    with pytest.raises(ValueError):
        validate_runtime_config("cli")


def test_invalid_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "PLAYWRIGHT_NAV_TIMEOUT_SECONDS", 0)
    with pytest.raises(ValueError):
        validate_runtime_config("cli")


def test_executor_knob_clamped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "ENABLE_DOWNLOAD_EXECUTOR", True)
    monkeypatch.setattr(config, "MAX_PARALLEL_DOWNLOADS", 0)
    monkeypatch.setattr(config, "MAX_PENDING_DOWNLOADS", 0)

    validate_runtime_config("tests")

    assert config.MAX_PARALLEL_DOWNLOADS == 1
    assert config.MAX_PENDING_DOWNLOADS == 1
