"""Helpers for persisting and restoring scraper checkpoints."""

from __future__ import annotations

import glob
import json
import os
import re
import time
from typing import Dict, Optional

from . import config
from .utils import log_line


CKPT_PATH = os.environ.get("RUN_STATE_PATH", str(config.RUN_STATE_FILE))
LOG_DIR = str(config.LOG_DIR)
RE_SAVED = re.compile(r"\[AJAX\]\s+Saved fname=(?P<fname>[A-Z0-9_%-]+).*?-> .*?\.pdf", re.I)
RE_CLICKED = re.compile(r"Clicked download button index (?P<idx>\d+) on page (?P<page>\d+)", re.I)


def load_checkpoint() -> Optional[Dict]:
    """Load the persisted checkpoint JSON if present."""

    if not os.path.exists(CKPT_PATH):
        return None
    try:
        with open(CKPT_PATH, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return None


def save_checkpoint(**kwargs) -> None:
    """Persist the current checkpoint data to disk."""

    os.makedirs(os.path.dirname(CKPT_PATH), exist_ok=True)
    state = load_checkpoint() or {}
    state.update(kwargs)
    state["saved_at_ts"] = time.time()
    with open(CKPT_PATH, "w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False, indent=2)


def clear_checkpoint() -> None:
    """Remove the checkpoint file if it exists."""

    try:
        os.remove(CKPT_PATH)
    except FileNotFoundError:
        pass


def derive_checkpoint_from_logs() -> Optional[Dict]:
    """Infer a resume position by parsing the newest scrape log file."""

    paths = sorted(glob.glob(os.path.join(LOG_DIR, "scrape_*.log")))
    if not paths:
        return None

    last_log = paths[-1]
    last_saved_fname = None
    last_clicked = None
    try:
        with open(last_log, "r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                match_saved = RE_SAVED.search(line)
                if match_saved:
                    last_saved_fname = match_saved.group("fname")

                match_clicked = RE_CLICKED.search(line)
                if match_clicked:
                    last_clicked = (int(match_clicked.group("page")), int(match_clicked.group("idx")))
    except Exception as exc:  # noqa: BLE001
        log_line(f"[STATE] Failed to derive checkpoint from logs: {exc}")
        return None

    if last_saved_fname:
        if last_clicked:
            page, idx = last_clicked
            return {
                "dt_page_index": max(0, page),
                "button_index": max(0, idx + 1),
                "last_fname": last_saved_fname,
            }
        return {"dt_page_index": None, "button_index": None, "last_fname": last_saved_fname}

    if last_clicked:
        page, idx = last_clicked
        return {"dt_page_index": max(0, page), "button_index": max(0, idx)}

    return None


__all__ = [
    "load_checkpoint",
    "save_checkpoint",
    "clear_checkpoint",
    "derive_checkpoint_from_logs",
]
