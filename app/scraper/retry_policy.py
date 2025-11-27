from __future__ import annotations

from typing import Optional

from .logging_utils import _scraper_event

_ERROR_RETRY_LIMITS = {
    "download_other": 3,
}


NON_RETRYABLE_ERROR_CODES = {
    "disk_full",
    "invalid_token",
    "csv_miss",
    "worklist_filtered",
    "seen_history",
    "already_downloaded",
    "in_run_dup",
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
    code = (error_code or "").strip() or None
    if code is None:
        return False

    if code in NON_RETRYABLE_ERROR_CODES:
        return False

    max_attempts = _ERROR_RETRY_LIMITS.get(code)
    if max_attempts is None:
        return False

    should_retry = attempt < max_attempts

    _scraper_event(
        "state",
        phase="retry_decision",
        error_code=code,
        attempt=attempt,
        max_attempts=max_attempts,
        will_retry=should_retry,
    )

    return should_retry


__all__ = ["decide_retry", "NON_RETRYABLE_ERROR_CODES"]
