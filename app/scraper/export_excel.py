"""Excel export helpers for run telemetry."""

from __future__ import annotations

import json
import os
from typing import Optional

import pandas as pd

from .telemetry import EXPORTS_DIR, RUNS_DIR, prune_old_exports


def _latest_run_json_path() -> Optional[str]:
    """Return the most recent run telemetry JSON path, if any.

    This inspects ``RUNS_DIR`` for ``*.json`` files and returns the last
    entry in sorted order. This mirrors how run telemetry JSONs are written
    and is used by the Excel export path to select the run to export.
    """

    if not os.path.isdir(RUNS_DIR):
        return None

    runs = sorted(
        [os.path.join(RUNS_DIR, path) for path in os.listdir(RUNS_DIR) if path.endswith(".json")]
    )
    return runs[-1] if runs else None


def export_latest_run_to_excel(dest_path: Optional[str] = None) -> str:
    """Create an Excel workbook from the most recent telemetry payload."""

    run_path = _latest_run_json_path()
    if not run_path:
        raise FileNotFoundError("No run telemetry available to export")

    with open(run_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    df = pd.DataFrame(payload.get("entries", []))
    if df.empty:
        df = pd.DataFrame([{"info": "No entries in latest run"}])

    downloaded = df[df["status"] == "downloaded"].copy() if not df.empty else pd.DataFrame()
    skipped = df[df["status"] == "skipped"].copy() if not df.empty else pd.DataFrame()
    failed = df[df["status"] == "failed"].copy() if not df.empty else pd.DataFrame()

    def safe_pivot(frame, by):
        if frame.empty:
            return pd.DataFrame()
        return frame.groupby(by).size().reset_index(name="count").sort_values("count", ascending=False)

    summary_status = df.groupby("status").size().reset_index(name="count") if not df.empty else pd.DataFrame()
    summary_court = safe_pivot(df, ["court", "status"]) if not df.empty else pd.DataFrame()
    summary_cat = safe_pivot(df, ["category", "status"]) if not df.empty else pd.DataFrame()

    os.makedirs(EXPORTS_DIR, exist_ok=True)
    if not dest_path:
        basename = f"cases_{payload['run_id']}.xlsx"
        dest_path = os.path.join(EXPORTS_DIR, basename)

    with pd.ExcelWriter(dest_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="All")
        downloaded.to_excel(writer, index=False, sheet_name="Downloaded")
        skipped.to_excel(writer, index=False, sheet_name="Skipped")
        failed.to_excel(writer, index=False, sheet_name="Failed")
        summary_status.to_excel(writer, index=False, sheet_name="Summary_Status")
        if not summary_court.empty:
            summary_court.to_excel(writer, index=False, sheet_name="Summary_Court")
        if not summary_cat.empty:
            summary_cat.to_excel(writer, index=False, sheet_name="Summary_Category")

    prune_old_exports()
    return dest_path


__all__ = ["export_latest_run_to_excel"]
