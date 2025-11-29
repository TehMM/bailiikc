import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.scraper import db
from app.scraper.download_state import CaseDownloadState
from app.scraper.error_codes import ErrorCode
from app.scraper import run_summary_cli

from tests.test_download_state import _configure_temp_paths, _create_run_and_case


def test_run_summary_cli_prints_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    ids = _create_run_and_case()
    run_id = ids["run_id"]

    state = CaseDownloadState.start(
        run_id=run_id,
        case_id=ids["case_id"],
        box_url="https://example.com/box",
    )
    state.mark_failed(
        error_code=ErrorCode.NETWORK,
        error_message="boom",
    )

    exit_code = run_summary_cli.main(["--run-id", str(run_id)])
    assert exit_code == 0

    out = capsys.readouterr().out
    assert f"Run {run_id}" in out
    assert "failed" in out
    assert ErrorCode.NETWORK in out


def test_run_summary_cli_errors_for_unknown_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    _configure_temp_paths(tmp_path, monkeypatch)
    db.initialize_schema()

    with pytest.raises(SystemExit) as excinfo:
        run_summary_cli.main(["--run-id", "12345"])

    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    assert "Run 12345 does not exist" in err
