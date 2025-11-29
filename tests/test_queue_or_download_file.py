from app.scraper import config, run


def test_replay_skip_network_stubs_pdf(tmp_path, monkeypatch):
    destination = tmp_path / "file.pdf"

    monkeypatch.setattr(config, "REPLAY_SKIP_NETWORK", True)

    def fail_download(*_args, **_kwargs):  # pragma: no cover - should never be called
        raise AssertionError("download_pdf should not be called when skipping network")

    monkeypatch.setattr(run.box_client, "download_pdf", fail_download)

    ok, info = run.queue_or_download_file(
        "https://example.com/file.pdf", destination, token="token-123"
    )

    assert ok is True
    assert isinstance(info, dict)
    assert destination.exists()
    assert destination.read_bytes().startswith(b"%PDF-1.4")
    assert destination.stat().st_size >= run.box_client.MIN_PDF_BYTES
