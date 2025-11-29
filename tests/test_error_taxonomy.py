from pathlib import Path

from app.scraper import box_client, run
from app.scraper.error_codes import ErrorCode


def test_queue_or_download_file_maps_download_error(monkeypatch, tmp_path: Path) -> None:
    destination = tmp_path / "file.pdf"

    def _raise_download_error(*_args, **_kwargs):  # noqa: ANN001
        raise box_client.DownloadError(ErrorCode.HTTP_404, "not found", http_status=404)

    monkeypatch.setattr(run.box_client, "download_pdf", _raise_download_error)

    ok, info = run.queue_or_download_file("https://example.com/file.pdf", destination)

    assert ok is False
    assert info is not None
    assert info["error_code"] == ErrorCode.HTTP_404
    assert info["http_status"] == 404
