"""Run telemetry and analytics helpers."""

from __future__ import annotations

import json
import os
import time
import uuid
from collections import defaultdict
from typing import Any, Dict, List, Optional

from . import config

RUNS_DIR = os.environ.get("RUNS_DIR", str(config.DATA_DIR / "runs"))
EXPORTS_DIR = os.environ.get("EXPORTS_DIR", str(config.DATA_DIR / "exports"))
MAX_EXPORTS = int(os.environ.get("EXPORTS_KEEP_MAX", "5"))


def _ts() -> str:
    return time.strftime("%Y%m%d_%H%M%S")


class RunTelemetry:
    """Collect per-run telemetry for analytics and export."""

    def __init__(self, mode: str) -> None:
        self.run_id = f"{_ts()}_{uuid.uuid4().hex[:8]}"
        self.mode = mode
        self.started_at = time.time()
        self.entries: List[Dict[str, Any]] = []
        self.summary: Dict[str, Any] = defaultdict(int)
        os.makedirs(RUNS_DIR, exist_ok=True)
        os.makedirs(EXPORTS_DIR, exist_ok=True)

    def add(self, status: str, reason: str, meta: Dict[str, Any]) -> None:
        self.entries.append(
            {
                "status": status,
                "reason": reason,
                **meta,
            }
        )
        self.summary[f"count_{status}"] += 1

    def finalize(self, extra: Optional[Dict[str, Any]] = None) -> str:
        payload = {
            "run_id": self.run_id,
            "mode": self.mode,
            "started_at": self.started_at,
            "ended_at": time.time(),
            "summary": dict(self.summary),
            "entries": self.entries,
            **(extra or {}),
        }
        path = os.path.join(RUNS_DIR, f"run_{self.run_id}.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        return path


def list_runs() -> List[str]:
    if not os.path.isdir(RUNS_DIR):
        return []
    return sorted(
        [os.path.join(RUNS_DIR, p) for p in os.listdir(RUNS_DIR) if p.endswith(".json")]
    )


def latest_run_json() -> Optional[str]:
    runs = list_runs()
    return runs[-1] if runs else None


def prune_old_exports() -> None:
    files = sorted(
        [os.path.join(EXPORTS_DIR, p) for p in os.listdir(EXPORTS_DIR) if p.endswith(".xlsx")]
    )
    while len(files) > MAX_EXPORTS:
        old = files.pop(0)
        try:
            os.remove(old)
        except Exception:  # noqa: BLE001
            continue


__all__ = [
    "RunTelemetry",
    "list_runs",
    "latest_run_json",
    "prune_old_exports",
]
