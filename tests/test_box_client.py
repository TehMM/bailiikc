from pathlib import Path

import pytest

from app.scraper import box_client


class _FakeResponseBase:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
        return False


def test_download_pdf_with_http_client(monkeypatch, tmp_path: Path):
    messages = []
    monkeypatch.setattr(box_client, "log_line", lambda msg: messages.append(str(msg)))

    def fake_http_client(url, timeout=None):  # noqa: ANN001
        class Resp:
            status = 200

            def body(self):
                return b"%PDF-1.4 test"

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
    assert result.bytes_written == len(b"%PDF-1.4 test")
    assert dest.read_bytes() == b"%PDF-1.4 test"
    assert any("[SCRAPER][BOX]" in msg for msg in messages)
    assert all("token=secret" not in msg for msg in messages)


def test_download_pdf_with_requests(monkeypatch, tmp_path: Path):
    messages = []
    monkeypatch.setattr(box_client, "log_line", lambda msg: messages.append(str(msg)))

    class Resp(_FakeResponseBase):
        status_code = 200

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size=8192):  # noqa: ANN001
            yield b"%PDF-"
            yield b"content"

    monkeypatch.setattr(box_client.requests, "get", lambda *_, **__: Resp())

    dest = tmp_path / "file.pdf"
    result = box_client.download_pdf("https://example.com/file.pdf", dest)

    assert result.ok is True
    assert result.status_code == 200
    assert result.bytes_written == dest.stat().st_size
    assert dest.read_bytes() == b"%PDF-content"
    assert any("[SCRAPER][BOX]" in msg for msg in messages)


def test_download_pdf_http_error(monkeypatch, tmp_path: Path):
    messages = []
    monkeypatch.setattr(box_client, "log_line", lambda msg: messages.append(str(msg)))

    import requests

    class Resp(_FakeResponseBase):
        status_code = 500

        def raise_for_status(self):
            raise requests.HTTPError("500")

        def iter_content(self, chunk_size=8192):  # noqa: ANN001
            yield b"%PDF-"

    monkeypatch.setattr(box_client.requests, "get", lambda *_, **__: Resp())

    dest = tmp_path / "file.pdf"
    result = box_client.download_pdf("https://example.com/file.pdf", dest, max_retries=1)

    assert result.ok is False
    assert result.status_code is None
    assert result.bytes_written == 0
    assert dest.exists() is False
    assert any("[AJAX]" in msg for msg in messages)


@pytest.mark.parametrize(
    "chunks, expected_error",
    [([b"HTML"], "Response is not a PDF"), ([], "Empty download")],
)
def test_download_pdf_invalid_content(monkeypatch, tmp_path: Path, chunks, expected_error):
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
    result = box_client.download_pdf("https://example.com/file.pdf", dest, max_retries=1)

    assert result.ok is False
    assert result.error_message == expected_error
    assert result.bytes_written == 0
    assert dest.exists() is False
    assert any(expected_error in msg for msg in messages)
