from __future__ import annotations

from typing import Optional

from .logging_utils import _scraper_event

_ERROR_RETRY_LIMITS = {
    # Transient download failures (HTTP/network/Box) – bounded retries.
    "download_other": 3,
}


NON_RETRYABLE_ERROR_CODES = {
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


def decide_retry(error_code: Optional[str], attempt: int) -> bool:
    """
    Decide whether we should retry a failed download for the given
    error_code and current attempt count.

    `attempt` is the number of attempts already made for this
    (run_id, case_id) in the run at the time of the failure
    (i.e. the attempt_count recorded in the downloads row for
    the failed attempt).
    """
    code = (error_code or "").strip()
    if not code:
        _scraper_event(
            "state",
            phase="retry_decision",
            kind="missing_error_code",
            error_code=None,
            attempt=attempt,
            max_attempts=None,
            will_retry=False,
        )
        return False

    if code in NON_RETRYABLE_ERROR_CODES:
        _scraper_event(
            "state",
            phase="retry_decision",
            kind="non_retryable",
            error_code=code,
            attempt=attempt,
            max_attempts=None,
            will_retry=False,
        )
        return False

    max_attempts = _ERROR_RETRY_LIMITS.get(code)
    if max_attempts is None:
        _scraper_event(
            "state",
            phase="retry_decision",
            kind="no_retry_policy",
            error_code=code,
            attempt=attempt,
            max_attempts=None,
            will_retry=False,
        )
        return False

    should_retry = attempt < max_attempts

    _scraper_event(
        "state",
        phase="retry_decision",
        kind="retryable" if should_retry else "capped",
        error_code=code,
        attempt=attempt,
        max_attempts=max_attempts,
        will_retry=should_retry,
    )

    return should_retry


__all__ = ["decide_retry", "NON_RETRYABLE_ERROR_CODES"]
