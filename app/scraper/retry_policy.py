from __future__ import annotations

from typing import Optional

from .error_codes import ErrorCode
from .logging_utils import _scraper_event

RETRYABLE_ERROR_CODES = {
    ErrorCode.NETWORK,
    ErrorCode.HTTP_5XX,
    ErrorCode.BOX_RATE_LIMIT,
}

NON_RETRYABLE_ERROR_CODES = {
    ErrorCode.HTTP_401,
    ErrorCode.HTTP_403,
    ErrorCode.HTTP_404,
    ErrorCode.HTTP_4XX,
    ErrorCode.MALFORMED_PDF,
    ErrorCode.SITE_STRUCTURE,
    # Hard, run-scoped environmental failure.
    "disk_full",
    # Logical skips – never retried.
    "invalid_token",
    "csv_miss",
    "worklist_filtered",
    "seen_history",
    "already_downloaded",
    "in_run_dup",
    "exists_ok",
    # Click-level failures are bounded in-page and should not be retried globally.
    "click_timeout",
}


def compute_backoff_seconds(attempt_index: int) -> float:
    """Return a capped exponential backoff for the given attempt (1-based)."""

    return float(min(2 ** max(0, attempt_index - 1), 30))


def decide_retry(
    attempt_index: int,
    max_attempts: int,
    error: BaseException | None = None,
    *,
    error_code: Optional[str] = None,
    http_status: Optional[int] = None,
) -> bool:
    """Decide whether a failed attempt should be retried."""

    if attempt_index >= max_attempts:
        _scraper_event(
            "state",
            phase="retry_decision",
            kind="capped",
            attempt=attempt_index,
            max_attempts=max_attempts,
            error_code=error_code,
            http_status=http_status,
            will_retry=False,
        )
        return False

    code = (error_code or "").strip()
    if code in NON_RETRYABLE_ERROR_CODES:
        _scraper_event(
            "state",
            phase="retry_decision",
            kind="non_retryable",
            error_code=code,
            attempt=attempt_index,
            max_attempts=max_attempts,
            http_status=http_status,
            will_retry=False,
        )
        return False

    if code in RETRYABLE_ERROR_CODES:
        _scraper_event(
            "state",
            phase="retry_decision",
            kind="retryable",
            error_code=code,
            attempt=attempt_index,
            max_attempts=max_attempts,
            http_status=http_status,
            will_retry=True,
        )
        return True

    if http_status is not None and http_status >= 500:
        _scraper_event(
            "state",
            phase="retry_decision",
            kind="retryable",
            error_code=code or None,
            attempt=attempt_index,
            max_attempts=max_attempts,
            http_status=http_status,
            will_retry=True,
        )
        return True

    # Unknown context: be conservative and allow a single retry if available.
    fallback_retry = attempt_index < max_attempts - 1
    _scraper_event(
        "state",
        phase="retry_decision",
        kind="unknown" if code else "missing_error_code",
        error_code=code or None,
        attempt=attempt_index,
        max_attempts=max_attempts,
        http_status=http_status,
        will_retry=fallback_retry,
        error_repr=repr(error) if error is not None else None,
    )
    return fallback_retry


__all__ = ["decide_retry", "compute_backoff_seconds", "NON_RETRYABLE_ERROR_CODES"]
