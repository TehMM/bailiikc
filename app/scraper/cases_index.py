from __future__ import annotations

import csv
import html
import io
import re
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

from . import config
from .utils import log_line


ACTION_SPLIT_RE = re.compile(r"^([A-Z]+[0-9]+[0-9]{8})([A-Z0-9]+)?$")
TOKEN_SPLIT_RE = re.compile(r"[|,;/\\\s]+")


def normalize_action_token(raw: str) -> str:
    """Normalise an Actions token to an uppercase alphanumeric string."""

    if raw is None:
        return ""

    token = html.unescape(str(raw))
    token = urllib.parse.unquote_plus(token)
    token = token.replace("\u00a0", " ")
    token = re.sub(r"\s+", " ", token).strip()
    if not token:
        return ""

    token = token.upper()
    token = re.sub(r"[^A-Z0-9]+", "", token)
    return token


@dataclass(frozen=True)
class CaseRow:
    """Lightweight representation of a case row from the judgments CSV."""

    action: str
    code: str
    suffix: str
    title: str
    subject: str = ""
    court: str = ""
    category: str = ""
    judgment_date: str = ""
    cause_number: str = ""
    extra: Dict[str, str] = field(default_factory=dict)


CASES_BY_ACTION: Dict[str, CaseRow] = {}
AJAX_FNAME_INDEX: Dict[str, CaseRow] = {}
CASES_ALL: List[CaseRow] = []


def _resolve_csv_stream(csv_path: str) -> Tuple[Optional[Iterable[str]], Optional[str]]:
    """Return an iterable of CSV lines and the path description used."""

    if not csv_path:
        return None, None

    normalized = csv_path.strip()
    # Direct URL support for convenience during tests/debugging.
    if normalized.lower().startswith(("http://", "https://")):
        try:
            response = requests.get(
                normalized,
                headers=config.COMMON_HEADERS,
                timeout=120,
            )
            response.raise_for_status()
            text = response.content.decode("utf-8-sig")
            return io.StringIO(text), normalized
        except Exception as exc:  # noqa: BLE001
            log_line(f"[CSV] Failed to download {normalized}: {exc}")
            return None, None

    candidates = [
        Path(normalized),
        Path(__file__).resolve().parent.parent / normalized,
        Path(__file__).resolve().parent / normalized,
        config.DATA_DIR / Path(normalized).name,
    ]
    for candidate in candidates:
        try:
            if candidate.is_file():
                return candidate.open("r", encoding="utf-8-sig", newline=""), str(candidate)
        except Exception as exc:  # noqa: BLE001
            log_line(f"[CSV] Failed to open {candidate}: {exc}")
    return None, None


def load_cases_from_csv(csv_path: str) -> None:
    """Populate the global case index from the provided CSV path."""

    stream, description = _resolve_csv_stream(csv_path)
    if not stream and csv_path != config.CSV_URL:
        log_line("[CSV] Primary CSV path %r unavailable; attempting %s" % (csv_path, config.CSV_URL))
        stream, description = _resolve_csv_stream(config.CSV_URL)
    CASES_BY_ACTION.clear()
    AJAX_FNAME_INDEX.clear()
    CASES_ALL.clear()

    if not stream:
        log_line(
            f"[CSV] Unable to load cases; no CSV found at {csv_path!r}. "
            "Callers should ensure the judgments CSV is available."
        )
        return

    log_line(f"[CSV] Loading cases from {description}")

    reader = csv.DictReader(stream)
    loaded = 0
    skipped_blank = 0

    for row in reader:
        actions_raw = html.unescape((row.get("Actions") or row.get("Action") or "").strip())
        if not actions_raw:
            skipped_blank += 1
            continue

        title = (row.get("Title") or row.get("Case Title") or row.get("Subject") or "").strip()
        subject = (row.get("Subject") or title or "").strip()
        court = (row.get("Court") or row.get("Court file") or "").strip()
        category = (row.get("Category") or "").strip()
        judgment_date = (row.get("Judgment Date") or row.get("Date") or "").strip()
        cause_number = (
            row.get("Cause Number")
            or row.get("Cause number")
            or row.get("Cause No.")
            or row.get("Cause")
            or ""
        ).strip()

        raw_tokens = [tok.strip() for tok in TOKEN_SPLIT_RE.split(actions_raw) if tok.strip()]
        if not raw_tokens:
            skipped_blank += 1
            continue

        row_extra = {k: (v or "").strip() for k, v in row.items()}
        row_extra["_raw_actions"] = actions_raw

        for token in raw_tokens:
            normalized = normalize_action_token(token)
            if not normalized:
                continue

            match = ACTION_SPLIT_RE.match(normalized)
            if match:
                code = match.group(1)
                suffix = match.group(2) or ""
            else:
                code = normalized
                suffix = ""

            case_title = title or subject or normalized
            case = CaseRow(
                action=normalized,
                code=code,
                suffix=suffix,
                title=case_title,
                subject=subject or case_title,
                court=court,
                category=category,
                judgment_date=judgment_date,
                cause_number=cause_number,
                extra=row_extra,
            )
            existing = CASES_BY_ACTION.get(normalized)
            if existing and existing.extra != case.extra:
                log_line(
                    f"[CSV] Duplicate action token {normalized} encountered; keeping first occurrence."
                )
                continue

            CASES_BY_ACTION[normalized] = case
            AJAX_FNAME_INDEX[normalized] = case
            CASES_ALL.append(case)
            loaded += 1

    log_line(
        f"[CSV] Loaded {loaded} case token(s) from CSV; "
        f"skipped {skipped_blank} row(s) without usable Actions entries."
    )


def find_case_by_fname(fname: str, *, strict: bool = False) -> Optional[CaseRow]:
    """Locate the CaseRow that best matches the AJAX fname token."""

    if not fname:
        return None

    candidate = normalize_action_token(fname)
    if not candidate:
        return None

    if not AJAX_FNAME_INDEX:
        log_line("[CSV] Case index is empty; call load_cases_from_csv() first.")
        return None

    direct = AJAX_FNAME_INDEX.get(candidate)
    if direct or strict:
        return direct

    matches: List[Tuple[int, int, str, CaseRow]] = []
    for action, case in AJAX_FNAME_INDEX.items():
        if candidate in action:
            start = action.find(candidate)
            overlap = len(candidate)
            matches.append((overlap, start, action, case))
            continue
        if action in candidate:
            start = candidate.find(action)
            overlap = len(action)
            matches.append((overlap, start, action, case))

    if not matches:
        return None

    matches.sort(key=lambda item: (-item[0], item[1], item[2]))
    best_overlap, best_start, best_action, best_case = matches[0]

    other_actions = [m[2] for m in matches[1:5]]
    if other_actions:
        log_line(
            f"[AJAX] fname {candidate} partial match candidates: {([best_action] + other_actions)}"
        )

    log_line(
        f"[AJAX] fname {candidate} resolved via partial match to Actions={best_action}; "
        f"overlap={best_overlap} start={best_start}"
    )
    return best_case


if __name__ == "__main__":  # pragma: no cover - manual verification aid
    import json
    from textwrap import dedent

    sample_csv = dedent(
        """
        Year,Court file,Date,Title,Actions
        2025,FSD 0151 OF 2025 (JAJ),2025-Nov-06,Re ATP Life Science Ventures LP - Judgment,FSD0151202511062025ATPLIFESCIENCE
        2025,FSD 0237 OF 2025 (DDJ),2025-Nov-05,Strata 647 v Dixon,G0237202311052025STRATA647DAPAAHDIXON
        2024,General,2024-Jun-01,Example Embedded Token,"1J1CB5JDVWQJ1DE60AG13020A37E6E68EADE88BE7AE51E57A648"
        """
    ).strip()

    stream = io.StringIO(sample_csv)
    CASES_BY_ACTION.clear()
    AJAX_FNAME_INDEX.clear()
    CASES_ALL.clear()
    reader = csv.DictReader(stream)
    for row in reader:
        actions_raw = row.get("Actions") or ""
        tokens = [
            normalize_action_token(part)
            for part in TOKEN_SPLIT_RE.split(actions_raw)
            if part.strip()
        ]
        row_extra = {k: (v or "").strip() for k, v in row.items()}
        for token in filter(None, tokens):
            match = ACTION_SPLIT_RE.match(token)
            if match:
                code = match.group(1)
                suffix = match.group(2) or ""
            else:
                code = token
                suffix = ""
            case = CaseRow(
                action=token,
                code=code,
                suffix=suffix,
                title=(row.get("Title") or "").strip(),
                subject=(row.get("Subject") or row.get("Title") or "").strip(),
                court=(row.get("Court file") or "").strip(),
                category=(row.get("Category") or "").strip(),
                judgment_date=(row.get("Date") or "").strip(),
                extra=row_extra,
            )
            CASES_BY_ACTION[token] = case
            AJAX_FNAME_INDEX[token] = case
            CASES_ALL.append(case)

    print("Exact lookup:", find_case_by_fname("FSD0151202511062025ATPLIFESCIENCE"))
    print("Embedded lookup:", find_case_by_fname("AG13020"))
    print(
        "Sample handle:",
        json.dumps(
            {
                "exact": find_case_by_fname("G0237202311052025STRATA647DAPAAHDIXON").title,
                "partial": find_case_by_fname("ag13020").action,
            },
            indent=2,
        ),
    )
