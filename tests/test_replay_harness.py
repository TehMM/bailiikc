import json
from pathlib import Path

from app.scraper import config, replay_harness
from app.scraper.replay_harness import ReplayConfig, load_dl_bfile_fixtures


def test_load_dl_bfile_fixtures(tmp_path: Path):
    fixtures_path = tmp_path / "fixtures.jsonl"
    fixtures_path.write_text(json.dumps({"foo": "bar"}) + "\n" + json.dumps("skip") + "\n")

    loaded = list(load_dl_bfile_fixtures(fixtures_path))

    assert loaded == [{"foo": "bar"}]


def test_run_replay_uses_handler(monkeypatch, tmp_path: Path):
    fixtures_path = tmp_path / "run_1_dl_bfile.jsonl"
    fixtures_path.write_text(
        json.dumps(
            {
                "fname": "TESTTOKEN",
                "box_url": "https://box.example.com/file",
                "mode": "new",
                "case_context": {"foo": "bar"},
                "run_id": 99,
            }
        )
        + "\n"
    )

    called = {}

    def fake_handle_dl_bfile_from_ajax(**kwargs):  # noqa: ANN001
        called.update(kwargs)
        return "downloaded", {}

    original_pdf_dir = config.PDF_DIR
    original_skip_network = config.REPLAY_SKIP_NETWORK
    monkeypatch.setattr(replay_harness, "handle_dl_bfile_from_ajax", fake_handle_dl_bfile_from_ajax)
    monkeypatch.setattr(config, "REPLAY_SKIP_NETWORK", False)

    output_root = tmp_path / "replay_out"
    summary = replay_harness.run_replay(
        ReplayConfig(
            fixtures_path=fixtures_path,
            dry_run=True,
            output_root=output_root,
            run_id=123,
        )
    )

    assert summary["processed"] == 1
    assert called["fname"] == "TESTTOKEN"
    assert called["downloads_dir"].is_dir()
    assert called["case_context"] == {"foo": "bar"}
    assert config.PDF_DIR == original_pdf_dir
    assert config.REPLAY_SKIP_NETWORK == original_skip_network
