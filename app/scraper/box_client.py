from __future__ import annotations

from dataclasses import dataclass
import time
import urllib.parse
from pathlib import Path
from typing import Any, Optional

import requests

from .utils import log_line


@dataclass
class BoxDownloadResult:
    ok: bool
    status_code: Optional[int]
    bytes_written: int
    error_message: Optional[str]
    exception_repr: Optional[str]


def _redact_url(url: str) -> str:
    try:
        parsed = urllib.parse.urlparse(url)
        return urllib.parse.urlunparse(parsed._replace(query=""))
    except Exception:
        return url


def download_pdf(
    url: str,
    dest_path: Path,
    *,
    http_client: Optional[Any] = None,
    max_retries: int = 3,
    timeout: int = 120,
    token: Optional[str] = None,
) -> BoxDownloadResult:
    """Download a PDF from ``url`` into ``dest_path`` with retries.

    Behaviour matches ``queue_or_download_file`` in ``run.py``: same retry
    pattern, same ``%PDF`` validation, same empty-file handling. This helper
    only refactors the logic into a reusable place.
    """

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    safe_url = _redact_url(url)
    last_error: Optional[str] = None
    last_exception_repr: Optional[str] = None
    last_status: Optional[int] = None

    for attempt in range(1, max_retries + 1):
        status: Optional[int] = None
        try:
            if http_client is not None:
                response = http_client(url, timeout=timeout)
                status = getattr(response, "status", None)
                if status is None:
                    status = getattr(response, "status_code", None)
                if status is not None and int(status) >= 400:
                    raise RuntimeError(f"HTTP {status}")

                body = response.body() if hasattr(response, "body") else response.content
                if isinstance(body, str):
                    body_bytes = body.encode("utf-8")
                elif isinstance(body, (bytes, bytearray)):
                    body_bytes = bytes(body)
                else:
                    body_bytes = bytes(body)

                if not body_bytes.startswith(b"%PDF"):
                    raise RuntimeError("Response is not a PDF")

                dest_path.write_bytes(body_bytes)
                bytes_written = len(body_bytes)
                log_line(
                    f"[SCRAPER][BOX] token={token or ''} url={safe_url} status={status or 'unknown'} bytes={bytes_written}"
                )
                return BoxDownloadResult(True, status, bytes_written, None, None)

            with requests.get(url, stream=True, timeout=timeout) as resp:
                resp.raise_for_status()
                status = resp.status_code
                with dest_path.open("wb") as handle:
                    first_chunk = True
                    for chunk in resp.iter_content(chunk_size=8192):
                        if not chunk:
                            continue
                        if first_chunk:
                            if not chunk.startswith(b"%PDF"):
                                raise RuntimeError("Response is not a PDF")
                            first_chunk = False
                        handle.write(chunk)

            file_size = dest_path.stat().st_size
            if file_size <= 0:
                raise RuntimeError("Empty download")

            log_line(
                f"[SCRAPER][BOX] token={token or ''} url={safe_url} status={status or 'unknown'} bytes={file_size}"
            )
            return BoxDownloadResult(True, status, file_size, None, None)

        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            last_exception_repr = repr(exc)
            last_status = status
            log_line(f"[SCRAPER][BOX] token={token or ''} url={safe_url} status={status or 'error'} error={exc}")
            log_line(f"[AJAX] Download attempt {attempt} for {safe_url} failed: {exc}")
            dest_path.unlink(missing_ok=True)
            time.sleep(min(2 ** attempt, 5))

    return BoxDownloadResult(False, last_status, 0, last_error, last_exception_repr)
