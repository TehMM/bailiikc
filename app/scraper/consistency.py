from __future__ import annotations

"""Diagnostics for comparing JSON and DB views of downloaded cases."""

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from app.scraper import db_reporting
from app.scraper.download_rows import build_download_rows, load_download_records


class CaseIssueType(str, Enum):
    MISSING_IN_DB = "missing_in_db"
    MISSING_IN_JSON = "missing_in_json"
    FIELD_MISMATCH = "field_mismatch"


@dataclass
class CaseDiff:
    """Represents a discrepancy for a single case between JSON and DB views."""

    key: str
    issue_type: CaseIssueType
    json_row: Optional[Dict[str, Any]]
    db_row: Optional[Dict[str, Any]]
    details: str


def _build_index(
    rows: List[Dict[str, Any]], source: str, errors: List[str]
) -> Dict[str, Dict[str, Any]]:
    by_key: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("actions_token") or "").strip()
        if not key:
            errors.append(f"{source}: row missing actions_token")
            continue
        if key in by_key:
            errors.append(f"{source}: duplicate actions_token '{key}'")
            continue
        by_key[key] = row
    return by_key


def _values_match(value_json: Any, value_db: Any) -> bool:
    if isinstance(value_json, (int, float)) and isinstance(value_db, (int, float)):
        return abs(float(value_json) - float(value_db)) <= 0.1
    return value_json == value_db


def _compare_fields(
    key: str,
    json_row: Dict[str, Any],
    db_row: Dict[str, Any],
    fields: List[str],
) -> Optional[CaseDiff]:
    mismatched: List[str] = []

    for field in fields:
        if not _values_match(json_row.get(field), db_row.get(field)):
            mismatched.append(field)

    if not mismatched:
        return None

    if len(mismatched) == 1:
        field = mismatched[0]
        details = (
            f"field {field} differs: json='{json_row.get(field)}', db='{db_row.get(field)}'"
        )
    else:
        details = f"multiple field mismatches: {', '.join(sorted(mismatched))}"

    return CaseDiff(
        key=key,
        issue_type=CaseIssueType.FIELD_MISMATCH,
        json_row=json_row,
        db_row=db_row,
        details=details,
    )


def compare_latest_downloads_json_vs_db() -> Dict[str, Any]:
    """Compare JSON-based and DB-based downloaded-cases views for the latest run."""

    errors: List[str] = []

    try:
        json_rows = build_download_rows(load_download_records())
    except Exception as exc:  # noqa: BLE001
        errors.append(f"error loading JSON downloads: {exc}")
        json_rows = []

    run_id = db_reporting.get_latest_run_id()
    try:
        db_rows = db_reporting.get_download_rows_for_run(
            run_id=run_id, status_filter="downloaded"
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"error loading DB downloads: {exc}")
        db_rows = []

    json_index = _build_index(json_rows, "json", errors)
    db_index = _build_index(db_rows, "db", errors)

    json_keys = set(json_index.keys())
    db_keys = set(db_index.keys())

    case_diffs: List[CaseDiff] = []

    for key in sorted(json_keys - db_keys):
        case_diffs.append(
            CaseDiff(
                key=key,
                issue_type=CaseIssueType.MISSING_IN_DB,
                json_row=json_index.get(key),
                db_row=None,
                details="present in JSON but missing in DB",
            )
        )

    for key in sorted(db_keys - json_keys):
        case_diffs.append(
            CaseDiff(
                key=key,
                issue_type=CaseIssueType.MISSING_IN_JSON,
                json_row=None,
                db_row=db_index.get(key),
                details="present in DB but missing in JSON",
            )
        )

    fields_to_compare = [
        "actions_token",
        "title",
        "court",
        "category",
        "judgment_date",
        "cause_number",
        "saved_path",
        "filename",
        "size_kb",
    ]

    for key in sorted(json_keys & db_keys):
        diff = _compare_fields(key, json_index[key], db_index[key], fields_to_compare)
        if diff:
            case_diffs.append(diff)

    report = {
        "ok": not case_diffs and len(json_rows) == len(db_rows),
        "run_id": run_id,
        "json_count": len(json_rows),
        "db_count": len(db_rows),
        "case_diffs": [asdict(diff) for diff in case_diffs],
        "errors": errors,
    }

    return report


if __name__ == "__main__":
    import argparse
    import json
    import sys

    parser = argparse.ArgumentParser(
        description="Compare JSON-based and DB-based downloaded-cases views for the latest run."
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Compare for the latest run (default behaviour).",
    )

    parser.parse_args()

    comparison = compare_latest_downloads_json_vs_db()
    json.dump(comparison, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")

    if not comparison.get("ok", False):
        sys.exit(1)
