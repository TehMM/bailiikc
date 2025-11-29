import pytest

from app.scraper.error_codes import ErrorCode
from app.scraper import retry_policy


@pytest.fixture
def event_recorder(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict]]:
    events: list[tuple[str, dict]] = []

    def _record(event_phase: str, **fields: object) -> None:
        events.append((event_phase, fields))

    monkeypatch.setattr(retry_policy, "_scraper_event", _record)
    return events


def test_http_500_retryable(event_recorder: list[tuple[str, dict]]) -> None:
    assert retry_policy.decide_retry(
        attempt_index=1,
        max_attempts=3,
        error=None,
        error_code=ErrorCode.HTTP_5XX,
        http_status=500,
    )
    assert event_recorder[-1][1]["will_retry"] is True


def test_http_404_not_retryable(event_recorder: list[tuple[str, dict]]) -> None:
    assert (
        retry_policy.decide_retry(
            attempt_index=1,
            max_attempts=3,
            error=None,
            error_code=ErrorCode.HTTP_404,
            http_status=404,
        )
        is False
    )
    assert event_recorder[-1][1]["kind"] == "non_retryable"


def test_network_retryable(event_recorder: list[tuple[str, dict]]) -> None:
    assert retry_policy.decide_retry(
        attempt_index=1,
        max_attempts=2,
        error=None,
        error_code=ErrorCode.NETWORK,
    )
    assert event_recorder[-1][1]["will_retry"] is True


def test_malformed_pdf_not_retryable(event_recorder: list[tuple[str, dict]]) -> None:
    assert (
        retry_policy.decide_retry(
            attempt_index=1,
            max_attempts=2,
            error=None,
            error_code=ErrorCode.MALFORMED_PDF,
        )
        is False
    )


def test_missing_context_allows_single_retry(event_recorder: list[tuple[str, dict]]) -> None:
    # No error_code/http_status -> allow only one retry window
    assert retry_policy.decide_retry(attempt_index=1, max_attempts=3) is True
    assert retry_policy.decide_retry(attempt_index=3, max_attempts=3) is False
    assert event_recorder[-1][1]["kind"] == "capped"


def test_compute_backoff_seconds() -> None:
    assert retry_policy.compute_backoff_seconds(1) == 1.0
    assert retry_policy.compute_backoff_seconds(2) == 2.0
    assert retry_policy.compute_backoff_seconds(3) == 4.0
    assert retry_policy.compute_backoff_seconds(10) == 30.0
