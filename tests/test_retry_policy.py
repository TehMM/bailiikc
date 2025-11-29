from __future__ import annotations

import pytest

from app.scraper import retry_policy


@pytest.fixture
def event_recorder(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict]]:
    events: list[tuple[str, dict]] = []

    def _record(event_phase: str, **fields: object) -> None:
        events.append((event_phase, fields))

    monkeypatch.setattr(retry_policy, "_scraper_event", _record)
    return events


@pytest.mark.parametrize(
    "attempt, expected, kind",
    [
        (1, True, "retryable"),
        (2, True, "retryable"),
        (3, False, "capped"),
        (5, False, "capped"),
    ],
)
def test_download_other_retry_limits(
    attempt: int, expected: bool, kind: str, event_recorder: list[tuple[str, dict]]
) -> None:
    result = retry_policy.decide_retry("download_other", attempt)
    assert result is expected
    assert len(event_recorder) == 1
    phase, fields = event_recorder[0]
    assert phase == "state"
    assert fields["phase"] == "retry_decision"
    assert fields["error_code"] == "download_other"
    assert fields["attempt"] == attempt
    assert fields["max_attempts"] == 3
    assert fields["will_retry"] is expected
    assert fields["kind"] == kind


@pytest.mark.parametrize(
    "error_code",
    ["disk_full", "invalid_token", "click_timeout", "exists_ok"],
)
def test_non_retryable_error_codes(error_code: str, event_recorder: list[tuple[str, dict]]) -> None:
    assert error_code in retry_policy.NON_RETRYABLE_ERROR_CODES
    result = retry_policy.decide_retry(error_code, 1)
    assert result is False
    assert len(event_recorder) == 1
    _, fields = event_recorder[0]
    assert fields["kind"] == "non_retryable"
    assert fields["phase"] == "retry_decision"
    assert fields["will_retry"] is False
    assert fields["error_code"] == error_code


@pytest.mark.parametrize(
    "error_code, expected_kind",
    [
        ("", "missing_error_code"),
        (None, "missing_error_code"),
        ("unexpected_code", "no_retry_policy"),
        ("box_404", "no_retry_policy"),
    ],
)
def test_missing_or_unknown_error_codes(
    error_code: str | None, expected_kind: str, event_recorder: list[tuple[str, dict]]
) -> None:
    assert retry_policy.decide_retry(error_code, 1) is False
    assert len(event_recorder) == 1
    _, fields = event_recorder[0]
    assert fields["will_retry"] is False
    assert fields["kind"] == expected_kind
    assert fields["phase"] == "retry_decision"
