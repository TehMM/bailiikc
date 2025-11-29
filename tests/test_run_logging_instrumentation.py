from pathlib import Path

from app.scraper import run


def test_box_logging(monkeypatch, tmp_path: Path):
    messages = []
    monkeypatch.setattr(run.box_client, "log_line", lambda msg: messages.append(str(msg)))

    def fake_http_client(url, timeout=None):  # noqa: ANN001
        class Resp:
            status = 200

            def body(self):
                return b"%PDF-1.4\n" + b"0" * run.box_client.MIN_PDF_BYTES

        return Resp()

    dest = tmp_path / "file.pdf"
    success, info = run.queue_or_download_file(
        "https://example.com/file.pdf?token=secret",
        dest,
        http_client=fake_http_client,
        token="TOK",
    )

    assert success is True
    assert isinstance(info, dict)
    assert any("[SCRAPER][BOX]" in msg for msg in messages)
    assert all("token=secret" not in msg for msg in messages)
