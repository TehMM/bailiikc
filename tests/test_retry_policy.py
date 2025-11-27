from __future__ import annotations

import pytest

from app.scraper.retry_policy import NON_RETRYABLE_ERROR_CODES, decide_retry


@pytest.mark.parametrize(
    "attempt, expected",
    [
        (1, True),
        (2, True),
        (3, False),
        (5, False),
    ],
)
def test_download_other_retry_limits(attempt: int, expected: bool) -> None:
    assert decide_retry("download_other", attempt) is expected


@pytest.mark.parametrize(
    "error_code",
    ["disk_full", "invalid_token", "click_timeout", "exists_ok"],
)
def test_non_retryable_error_codes(error_code: str) -> None:
    assert error_code in NON_RETRYABLE_ERROR_CODES
    assert decide_retry(error_code, 1) is False


@pytest.mark.parametrize("error_code", ["", None, "unexpected_code", "box_404"])
def test_missing_or_unknown_error_codes(error_code: str | None) -> None:
    assert decide_retry(error_code, 1) is False
