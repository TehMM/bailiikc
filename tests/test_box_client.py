from pathlib import Path

import pytest

from app.scraper import box_client
from app.scraper.error_codes import ErrorCode


class _FakeResponseBase:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
        return False


def _valid_pdf_bytes() -> bytes:
    return b"%PDF-1.4\n" + b"0" * box_client.MIN_PDF_BYTES


def test_download_pdf_with_http_client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    messages = []
    monkeypatch.setattr(box_client, "log_line", lambda msg: messages.append(str(msg)))

    def fake_http_client(url, timeout=None):  # noqa: ANN001
        class Resp:
            status = 200

            def body(self):
                return _valid_pdf_bytes()

        return Resp()

    dest = tmp_path / "file.pdf"
    result = box_client.download_pdf(
        "https://example.com/file.pdf?token=secret",
        dest,
        http_client=fake_http_client,
        token="TOK",
    )

    assert result.ok is True
    assert result.status_code == 200
    assert result.bytes_written == dest.stat().st_size
    assert dest.read_bytes().startswith(b"%PDF-")
    assert any("[SCRAPER][BOX]" in msg for msg in messages)
    assert all("token=secret" not in msg for msg in messages)


def test_download_pdf_with_requests(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    messages = []
    monkeypatch.setattr(box_client, "log_line", lambda msg: messages.append(str(msg)))

    class Resp(_FakeResponseBase):
        status_code = 200

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size=8192):  # noqa: ANN001
            yield _valid_pdf_bytes()

    monkeypatch.setattr(box_client.requests, "get", lambda *_, **__: Resp())

    dest = tmp_path / "file.pdf"
    result = box_client.download_pdf("https://example.com/file.pdf", dest)

    assert result.ok is True
    assert result.status_code == 200
    assert result.bytes_written == dest.stat().st_size
    assert dest.read_bytes().startswith(b"%PDF-")
    assert any("[SCRAPER][BOX]" in msg for msg in messages)


def test_download_pdf_http_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    messages = []
    monkeypatch.setattr(box_client, "log_line", lambda msg: messages.append(str(msg)))

    import requests

    class Resp(_FakeResponseBase):
        status_code = 500

        def raise_for_status(self):
            raise requests.HTTPError("500")

        def iter_content(self, chunk_size=8192):  # noqa: ANN001
            yield _valid_pdf_bytes()

    monkeypatch.setattr(box_client.requests, "get", lambda *_, **__: Resp())

    dest = tmp_path / "file.pdf"
    with pytest.raises(box_client.DownloadError) as excinfo:
        box_client.download_pdf("https://example.com/file.pdf", dest, max_retries=1)

    assert excinfo.value.error_code == ErrorCode.HTTP_5XX
    assert any("[AJAX]" in msg for msg in messages)
    assert dest.exists() is False


@pytest.mark.parametrize(
    "chunks, expected_error_code",
    [([b"HTML"], ErrorCode.MALFORMED_PDF), ([b"%PDF"], ErrorCode.MALFORMED_PDF)],
)

def test_download_pdf_invalid_content(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, chunks, expected_error_code: str
) -> None:
    messages = []
    monkeypatch.setattr(box_client, "log_line", lambda msg: messages.append(str(msg)))

    class Resp(_FakeResponseBase):
        status_code = 200

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size=8192):  # noqa: ANN001
            yield from chunks

    monkeypatch.setattr(box_client.requests, "get", lambda *_, **__: Resp())

    dest = tmp_path / "file.pdf"
    with pytest.raises(box_client.DownloadError) as excinfo:
        box_client.download_pdf("https://example.com/file.pdf", dest, max_retries=1)

    assert excinfo.value.error_code == expected_error_code
    assert dest.exists() is False
    assert any("failed" in msg.lower() for msg in messages)
