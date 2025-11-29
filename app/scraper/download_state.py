from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from . import db
from .error_codes import ErrorCode
from .logging_utils import _scraper_event


class DownloadStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    DOWNLOADED = "downloaded"
    SKIPPED = "skipped"
    FAILED = "failed"


def _now_iso() -> str:
    """Return the current UTC time formatted for DB logging."""

    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _safe_status(value: Any) -> DownloadStatus:
    try:
        return DownloadStatus(value)
    except Exception:
        return DownloadStatus.PENDING


@dataclass
class CaseDownloadState:
    run_id: int
    case_id: Optional[int]
    status: DownloadStatus
    attempt_count: int = 0
    download_id: Optional[int] = None

    @classmethod
    def _from_row(cls, *, run_id: int, case_id: Optional[int], row: Any) -> "CaseDownloadState":
        status_value = (
            row["status"]
            if row is not None and "status" in row.keys()
            else DownloadStatus.PENDING.value
        )
        status = _safe_status(status_value)
        attempt_count = (
            int(row["attempt_count"])
            if row is not None and "attempt_count" in row.keys()
            else 0
        )
        download_id = int(row["id"]) if row is not None and "id" in row.keys() else None
        return cls(
            run_id=run_id,
            case_id=case_id,
            status=status,
            attempt_count=attempt_count,
            download_id=download_id,
        )

    @classmethod
    def load(cls, *, run_id: int, case_id: Optional[int]) -> "CaseDownloadState":
        if case_id is None:
            return cls(
                run_id=run_id,
                case_id=case_id,
                status=DownloadStatus.PENDING,
                attempt_count=0,
                download_id=None,
            )

        row = db.ensure_download_row(run_id, case_id)
        return cls._from_row(run_id=run_id, case_id=case_id, row=row)

    @classmethod
    def start(
        cls,
        *,
        run_id: int,
        case_id: Optional[int],
        box_url: Optional[str],
    ) -> "CaseDownloadState":
        """Create or update a downloads row to represent a new attempt."""

        if case_id is None:
            return cls(
                run_id=run_id,
                case_id=case_id,
                status=DownloadStatus.PENDING,
                attempt_count=0,
                download_id=None,
            )

        row = db.ensure_download_row(run_id, case_id)
        state = cls._from_row(run_id=run_id, case_id=case_id, row=row)

        if not state._ensure_can_transition(DownloadStatus.IN_PROGRESS):
            return state

        attempt = state.attempt_count + 1
        db.update_download_status(
            run_id=run_id,
            case_id=case_id,
            status=DownloadStatus.IN_PROGRESS.value,
            attempt_count=attempt,
            last_attempt_at=_now_iso(),
            box_url_last=box_url,
        )

        state.status = DownloadStatus.IN_PROGRESS
        state.attempt_count = attempt

        _scraper_event(
            "state",
            run_id=run_id,
            case_id=case_id,
            download_id=state.download_id,
            from_status=row["status"] if row is not None else DownloadStatus.PENDING.value,
            to_status=state.status.value,
            attempt=attempt,
            box_url=box_url,
        )
        return state

    def _ensure_can_transition(self, target: DownloadStatus) -> bool:
        if self.status == DownloadStatus.DOWNLOADED and target != DownloadStatus.DOWNLOADED:
            _scraper_event(
                "error",
                run_id=self.run_id,
                case_id=self.case_id,
                download_id=self.download_id,
                current_status=self.status.value,
                attempted_status=target.value,
                error="invalid_transition_after_download",
            )
            return False
        return True

    def _mark_result(
        self,
        *,
        target_status: DownloadStatus,
        file_path: Optional[str] = None,
        file_size_bytes: Optional[int] = None,
        box_url: Optional[str] = None,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> None:
        if not self._ensure_can_transition(target_status):
            return

        if self.case_id is None:
            _scraper_event(
                "state",
                run_id=self.run_id,
                case_id=None,
                from_status=self.status.value,
                to_status=target_status.value,
                reason=reason or error_code or "no_case_id",
            )
            return

        prev = self.status
        self.status = target_status
        db.update_download_status(
            run_id=self.run_id,
            case_id=self.case_id,
            status=self.status.value,
            attempt_count=self.attempt_count,
            last_attempt_at=_now_iso(),
            file_path=file_path,
            file_size_bytes=file_size_bytes,
            box_url_last=box_url,
            error_code=error_code or reason,
            error_message=error_message,
        )

        payload = dict(
            run_id=self.run_id,
            case_id=self.case_id,
            download_id=self.download_id,
            from_status=prev.value,
            to_status=self.status.value,
            attempt=self.attempt_count,
        )
        if file_path is not None:
            payload["file_path"] = file_path
        if file_size_bytes is not None:
            payload["file_size_bytes"] = file_size_bytes
        if reason is not None:
            payload["reason"] = reason
        if error_code is not None:
            payload["error_code"] = error_code
        if error_message is not None:
            payload["error_message"] = error_message
        if box_url is not None:
            payload["box_url_last"] = box_url

        _scraper_event("state", **payload)

    def mark_downloaded(
        self,
        *,
        file_path: Optional[str],
        file_size_bytes: Optional[int],
        box_url: Optional[str],
    ) -> None:
        """Mark this case as successfully downloaded."""

        self._mark_result(
            target_status=DownloadStatus.DOWNLOADED,
            file_path=file_path,
            file_size_bytes=file_size_bytes,
            box_url=box_url,
        )

    def mark_skipped(self, reason: str) -> None:
        """Mark this case as skipped for a permanent reason."""

        self._mark_result(target_status=DownloadStatus.SKIPPED, reason=reason)

    def mark_failed(
        self,
        *,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Mark this case as failed for this attempt.

        ``error_code`` is optional for backwards compatibility. When omitted the
        failure will be recorded using ``internal_error`` to ensure DB rows and
        logs still capture a reason for the failure.
        """

        final_error_code = error_code or ErrorCode.INTERNAL

        self._mark_result(
            target_status=DownloadStatus.FAILED,
            error_code=final_error_code,
            error_message=error_message,
        )


__all__ = ["CaseDownloadState", "DownloadStatus"]
