from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from threading import Lock
from typing import Callable, Optional, Tuple

from . import config
from .logging_utils import _scraper_event

DownloadFn = Callable[[], Tuple[bool, Optional[str]]]


class DownloadExecutor:
    """
    Thin concurrency wrapper around Box downloads.

    IMPORTANT:
    - Default max_workers is 1, so behaviour stays effectively sequential.
    - We only parallelise HTTP downloads, not Playwright navigation.
    - Per-case state transitions are still protected by CaseDownloadState.
    """

    def __init__(self, max_workers: int) -> None:
        self._max_workers = max(1, max_workers)
        self._executor: Optional[ThreadPoolExecutor] = (
            ThreadPoolExecutor(max_workers=self._max_workers)
            if self._max_workers > 1 and config.ENABLE_DOWNLOAD_EXECUTOR
            else None
        )
        self._lock = Lock()
        self._in_flight: int = 0
        self._peak_in_flight: int = 0

    def submit(self, token: str, fn: DownloadFn) -> Tuple[bool, Optional[str]]:
        """
        Execute ``fn`` synchronously or via a thread pool based on configuration.

        Returns (ok, error_message) as expected by callers.
        """

        if self._executor is None:
            return fn()

        max_pending = max(1, config.MAX_PENDING_DOWNLOADS)
        with self._lock:
            if self._in_flight >= max_pending:
                _scraper_event(
                    "state",
                    phase="download_executor",
                    kind="queue_overflow",
                    token=token,
                    in_flight=self._in_flight,
                    max_pending=max_pending,
                )
                return fn()
            self._in_flight += 1
            self._peak_in_flight = max(self._peak_in_flight, self._in_flight)

        def _wrapped() -> Tuple[bool, Optional[str]]:
            try:
                return fn()
            finally:
                with self._lock:
                    self._in_flight -= 1

        future: Future[Tuple[bool, Optional[str]]] = self._executor.submit(_wrapped)
        return future.result()

    @property
    def peak_in_flight(self) -> int:
        with self._lock:
            return self._peak_in_flight

    def shutdown(self) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=True)
