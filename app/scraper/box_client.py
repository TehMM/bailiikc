from __future__ import annotations

import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import requests

from .error_codes import ErrorCode
from .logging_utils import _scraper_event
from .retry_policy import compute_backoff_seconds, decide_retry
from .utils import log_line

MIN_PDF_BYTES = 1024


@dataclass
class BoxDownloadResult:
    ok: bool
    status_code: Optional[int]
    bytes_written: int
    error_message: Optional[str]
    exception_repr: Optional[str]


class DownloadError(Exception):
    def __init__(self, error_code: str, message: str, *, http_status: int | None = None) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.http_status = http_status

    def __str__(self) -> str:  # pragma: no cover - inherited behaviour
        return str(self.args[0]) if self.args else ""


def _redact_url(url: str) -> str:
    try:
        parsed = urllib.parse.urlparse(url)
        return urllib.parse.urlunparse(parsed._replace(query=""))
    except Exception:
        return url


def _classify_http_status(status: Optional[int]) -> str:
    if status is None:
        return ErrorCode.INTERNAL
    if status == 401:
        return ErrorCode.HTTP_401
    if status == 403:
        return ErrorCode.HTTP_403
    if status == 404:
        return ErrorCode.HTTP_404
    if status == 429:
        return ErrorCode.BOX_RATE_LIMIT
    if 400 <= status < 500:
        return ErrorCode.HTTP_4XX
    if status >= 500:
        return ErrorCode.HTTP_5XX
    return ErrorCode.INTERNAL


def _validate_pdf_bytes(data: bytes) -> None:
    if not data.startswith(b"%PDF"):
        raise DownloadError(ErrorCode.MALFORMED_PDF, "Response is not a PDF")


def download_pdf(
    url: str,
    dest_path: Path,
    *,
    http_client: Optional[Any] = None,
    max_retries: int = 3,
    timeout: int = 120,
    token: Optional[str] = None,
) -> BoxDownloadResult:
    """Download a PDF from ``url`` into ``dest_path`` with retries."""

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    safe_url = _redact_url(url)
    last_error_message: Optional[str] = None
    last_status: Optional[int] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None

    for attempt in range(1, max_retries + 1):
        status: Optional[int] = None
        try:
            if http_client is not None:
                response = http_client(url, timeout=timeout)
                status = getattr(response, "status", None)
                if status is None:
                    status = getattr(response, "status_code", None)
                if status is not None and int(status) >= 400:
                    raise DownloadError(
                        _classify_http_status(int(status)),
                        f"HTTP {status}",
                        http_status=int(status),
                    )

                body = response.body() if hasattr(response, "body") else response.content
                if isinstance(body, str):
                    body_bytes = body.encode("utf-8")
                elif isinstance(body, (bytes, bytearray)):
                    body_bytes = bytes(body)
                else:
                    body_bytes = bytes(body)

                _validate_pdf_bytes(body_bytes)
                if len(body_bytes) < MIN_PDF_BYTES:
                    raise DownloadError(ErrorCode.MALFORMED_PDF, "PDF appears truncated")

                dest_path.write_bytes(body_bytes)
                bytes_written = len(body_bytes)
                _scraper_event(
                    "box",
                    phase="download",
                    token=token or safe_url,
                    status="ok",
                    http_status=status,
                    bytes=bytes_written,
                )
                log_line(
                    f"[SCRAPER][BOX] token={token or ''} url={safe_url} status={status or 'unknown'} bytes={bytes_written}"
                )
                return BoxDownloadResult(True, status, bytes_written, None, None)

            with requests.get(url, stream=True, timeout=timeout) as resp:
                status = resp.status_code
                resp.raise_for_status()
                first_chunk = True
                with dest_path.open("wb") as handle:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if not chunk:
                            continue
                        if first_chunk:
                            _validate_pdf_bytes(chunk)
                            first_chunk = False
                        handle.write(chunk)

            file_size = dest_path.stat().st_size
            if file_size < MIN_PDF_BYTES:
                raise DownloadError(ErrorCode.MALFORMED_PDF, "PDF appears truncated")

            _scraper_event(
                "box",
                phase="download",
                token=token or safe_url,
                status="ok",
                http_status=status,
                bytes=file_size,
            )
            log_line(
                f"[SCRAPER][BOX] token={token or ''} url={safe_url} status={status or 'unknown'} bytes={file_size}"
            )
            return BoxDownloadResult(True, status, file_size, None, None)

        except DownloadError as exc:
            last_error_message = str(exc)
            status = status or exc.http_status
            last_status = status
            error_code = exc.error_code
            error_message = last_error_message
            should_retry = decide_retry(
                attempt_index=attempt,
                max_attempts=max_retries,
                error=exc,
                error_code=error_code,
                http_status=status,
            )
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error_message = str(exc)
            error_code = ErrorCode.NETWORK
            error_message = last_error_message
            should_retry = decide_retry(
                attempt_index=attempt,
                max_attempts=max_retries,
                error=exc,
                error_code=error_code,
                http_status=status,
            )
        except requests.HTTPError as exc:
            last_error_message = str(exc)
            status = getattr(exc.response, "status_code", status)
            last_status = status
            error_code = _classify_http_status(status)
            error_message = last_error_message
            should_retry = decide_retry(
                attempt_index=attempt,
                max_attempts=max_retries,
                error=exc,
                error_code=error_code,
                http_status=status,
            )
        except Exception as exc:  # noqa: BLE001
            last_error_message = str(exc)
            error_code = ErrorCode.INTERNAL
            error_message = last_error_message
            should_retry = decide_retry(
                attempt_index=attempt,
                max_attempts=max_retries,
                error=exc,
                error_code=error_code,
                http_status=status,
            )

        dest_path.unlink(missing_ok=True)
        backoff = compute_backoff_seconds(attempt)
        _scraper_event(
            "state",
            phase="download_retry",
            token=token or safe_url,
            attempt=attempt,
            max_attempts=max_retries,
            error_code=error_code,
            http_status=status,
            will_retry=should_retry,
            backoff_seconds=backoff if should_retry else None,
            error_message=error_message,
        )
        log_line(
            f"[SCRAPER][BOX] token={token or ''} url={safe_url} status={status or 'error'} error={error_message}"
        )
        log_line(f"[AJAX] Download attempt {attempt} for {safe_url} failed: {error_message}")

        if not should_retry:
            raise DownloadError(error_code or ErrorCode.INTERNAL, error_message or "download failed", http_status=status)

        time.sleep(backoff)

    raise DownloadError(
        error_code or ErrorCode.INTERNAL,
        last_error_message or "download failed",
        http_status=last_status,
    )


__all__ = [
    "BoxDownloadResult",
    "download_pdf",
    "DownloadError",
    "MIN_PDF_BYTES",
    "_validate_pdf_bytes",
]
