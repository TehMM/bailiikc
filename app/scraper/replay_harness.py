"""Offline replay harness for dl_bfile fixtures.

This module replays captured dl_bfile events without Playwright. It mutates
process-wide config values (e.g., PDF_DIR, DOWNLOADS_LOG) during execution, so
it is **not** intended to run concurrently in-process with a live scraper.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import requests

from . import config
from . import cases_index
from .logging_utils import _scraper_event
from .config_validation import validate_runtime_config
from .run import handle_dl_bfile_from_ajax
from .utils import load_json_lines, log_line


@dataclass
class ReplayConfig:
    fixtures_path: Path
    dry_run: bool = True
    output_root: Optional[Path] = None
    run_id: Optional[int] = None


def load_dl_bfile_fixtures(fixtures_path: Path) -> Iterable[Dict[str, Any]]:
    for item in load_json_lines(fixtures_path):
        if not isinstance(item, dict):
            continue
        yield item


def _prepare_output_root(config_obj: ReplayConfig) -> Path:
    if config_obj.output_root:
        return Path(config_obj.output_root)
    suffix = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return config.DATA_DIR / "replay_runs" / f"replay_{suffix}"


def run_replay(config_obj: ReplayConfig) -> Dict[str, Any]:
    validate_runtime_config("replay")
    fixtures = list(load_dl_bfile_fixtures(config_obj.fixtures_path))
    summary: Dict[str, Any] = {
        "fixtures": len(fixtures),
        "processed": 0,
    }

    _scraper_event(
        "replay",
        phase="start",
        fixtures=str(config_obj.fixtures_path),
        dry_run=config_obj.dry_run,
    )

    cases_by_action = cases_index.CASES_BY_ACTION if cases_index.CASES_BY_ACTION else {}
    if not cases_by_action:
        log_line("[REPLAY] Cases index is empty; proceeding with fixture case_context only.")

    sandbox_root = _prepare_output_root(config_obj)
    downloads_dir = sandbox_root / ("dry_run_pdfs" if config_obj.dry_run else "pdfs")
    sandbox_root.mkdir(parents=True, exist_ok=True)
    downloads_dir.mkdir(parents=True, exist_ok=True)
    sandbox_downloads_log = sandbox_root / "downloads.jsonl"
    sandbox_downloads_log.parent.mkdir(parents=True, exist_ok=True)

    original_pdf_dir = config.PDF_DIR
    original_downloads_log = config.DOWNLOADS_LOG
    original_skip_network = config.REPLAY_SKIP_NETWORK

    session = requests.Session()
    http_client = lambda url, timeout: session.get(url, timeout=timeout)  # noqa: E731

    use_db = config_obj.run_id is not None

    try:
        config.PDF_DIR = downloads_dir
        config.DOWNLOADS_LOG = sandbox_downloads_log
        if config_obj.dry_run:
            config.REPLAY_SKIP_NETWORK = True

        for item in fixtures:
            if not isinstance(item, dict):
                continue
            fname = str(item.get("fname") or item.get("canonical_token") or "").strip()
            box_url = str(item.get("box_url") or "").strip()
            mode = str(item.get("mode") or "new")
            if not fname or not box_url:
                continue

            case_context = item.get("case_context") if isinstance(item.get("case_context"), dict) else None
            run_id = config_obj.run_id if use_db else None
            metadata: Dict[str, Any] = {}

            log_line(f"[REPLAY] Replaying fixture for fname={fname} box_url={box_url}")
            handle_dl_bfile_from_ajax(
                mode=mode,
                fname=fname,
                box_url=box_url,
                downloads_dir=downloads_dir,
                cases_by_action=cases_by_action,
                processed_this_run=None,
                checkpoint=None,
                metadata=metadata,
                http_client=http_client,
                case_context=case_context,
                fid=item.get("fid"),
                run_id=run_id,
                download_executor=None,
            )
            summary["processed"] = summary.get("processed", 0) + 1
    finally:
        config.PDF_DIR = original_pdf_dir
        config.DOWNLOADS_LOG = original_downloads_log
        config.REPLAY_SKIP_NETWORK = original_skip_network

    _scraper_event(
        "replay",
        phase="end",
        fixtures=str(config_obj.fixtures_path),
        dry_run=config_obj.dry_run,
    )
    return summary


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Replay dl_bfile fixtures offline.")
    parser.add_argument("fixtures", help="Path to run_XXX_dl_bfile.jsonl")
    parser.add_argument("--dry-run", action="store_true", default=False)
    args = parser.parse_args()

    cfg = ReplayConfig(fixtures_path=Path(args.fixtures), dry_run=args.dry_run)
    run_replay(cfg)
