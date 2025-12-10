from __future__ import annotations

"""Helpers for building and querying the judgments case index.

The main entry points are:
    - ``load_cases_from_csv(csv_path: str)``: Populate the in-memory index by
      parsing the judgments CSV. This is the legacy, default path used by the
      scraper.
    - ``load_cases_index_from_db(...)``: Populate the same index from the
      SQLite ``cases`` table (used when BAILIIKC_USE_DB_CASES=1).
    - ``find_case_by_fname(fname: str)``: Locate a case by AJAX filename token.

Index shape (global module state):
    CASES_BY_ACTION: Dict[str, CaseRow]
        Mapping from normalised action token (``action_token_norm``) to a
        ``CaseRow`` describing the case. Keys are produced by
        ``normalize_action_token``.
    AJAX_FNAME_INDEX: Dict[str, CaseRow]
        Alias of ``CASES_BY_ACTION`` used for AJAX filename lookups.
    CASES_ALL: List[CaseRow]
        Flat list of all CaseRow entries loaded from the source.

Each ``CaseRow`` contains at least the following fields used by callers:
    - action: str (normalised token used as the key)
    - code: str (core code portion of the token)
    - suffix: str (token suffix after the core code)
    - title: str
    - subject: str
    - court: str
    - category: str
    - judgment_date: str
    - cause_number: str
    - extra: Dict[str, str] (raw CSV columns and helpers)

By default, callers use ``load_cases_index_from_db``. When the environment
variable ``BAILIIKC_USE_DB_CASES`` is set to ``"0"``, the in-memory index falls
back to CSV via ``load_cases_from_csv``. This behaviour is controlled by
``should_use_db_index`` and remains backwards compatible for deployments that
still prefer the legacy CSV path.
"""

import csv
import html
import io
import os
import re
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from . import config, db_case_index, sources
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
    sort_judgment_date: str = ""
    cause_number: str = ""
    extra: Dict[str, str] = field(default_factory=dict)


CASES_BY_ACTION: Dict[str, CaseRow] = {}
AJAX_FNAME_INDEX: Dict[str, CaseRow] = {}
CASES_ALL: List[CaseRow] = []
CASES_BY_SOURCE: Dict[str, List[CaseRow]] = {}


def _reset_indexes() -> None:
    CASES_BY_ACTION.clear()
    AJAX_FNAME_INDEX.clear()
    CASES_ALL.clear()
    CASES_BY_SOURCE.clear()


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


def load_cases_from_csv(
    csv_path: str,
    *,
    source: str = sources.DEFAULT_SOURCE,
    csv_version_id: Optional[int] = None,
) -> None:
    """Populate the global case index from the provided CSV path.

    The legacy CSV parsing path remains the default. If
    ``should_use_db_index()`` returns True, this function delegates to
    ``load_cases_index_from_db`` instead of parsing the CSV.
    """

    if should_use_db_index():
        load_cases_index_from_db(source=source, csv_version_id=csv_version_id)
        return

    source_norm = sources.normalize_source(source)
    stream, description = _resolve_csv_stream(csv_path)
    if not stream:
        if source_norm == sources.UNREPORTED_JUDGMENTS and csv_path != config.CSV_URL:
            log_line(
                "[CSV] Primary CSV path %r unavailable for source=%r; attempting %s"
                % (csv_path, source_norm, config.CSV_URL)
            )
            stream, description = _resolve_csv_stream(config.CSV_URL)

        if not stream:
            log_line(
                f"[CSV] Unable to load cases; no CSV found at {csv_path!r} for source={source_norm!r}. "
                "Callers should ensure the source-specific CSV is available."
            )
            return

    _reset_indexes()
    CASES_BY_SOURCE[source_norm] = []

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
            if source_norm == sources.UNREPORTED_JUDGMENTS:
                AJAX_FNAME_INDEX[normalized] = case
            CASES_ALL.append(case)
            CASES_BY_SOURCE[source_norm].append(case)
            loaded += 1

    log_line(
        f"[CSV] Loaded {loaded} case token(s) from CSV; "
        f"skipped {skipped_blank} row(s) without usable Actions entries."
    )


def load_cases_index_from_db(
    *,
    source: str = sources.DEFAULT_SOURCE,
    only_active: bool = True,
    csv_version_id: Optional[int] = None,
) -> Dict[str, "CaseRow"]:
    """Populate the global case index using the SQLite ``cases`` table.

    Returns the populated ``CASES_BY_ACTION`` mapping for convenience.
    """

    _reset_indexes()

    source_norm = sources.normalize_source(source)

    records = db_case_index.load_case_index_from_db(
        source=source_norm, only_active=only_active, csv_version_id=csv_version_id
    )

    loaded = 0
    CASES_BY_SOURCE[source_norm] = []
    for token, record in records.items():
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

        case = CaseRow(
            action=normalized,
            code=code,
            suffix=suffix,
            title=(record.get("title") or "").strip(),
            subject=(record.get("subject") or record.get("title") or "").strip(),
            court=(record.get("court") or "").strip(),
            category=(record.get("category") or "").strip(),
            judgment_date=(record.get("judgment_date") or "").strip(),
            sort_judgment_date=(record.get("sort_judgment_date") or "").strip(),
            cause_number=(record.get("cause_number") or "").strip(),
            extra={
                "_source": record.get("source", ""),
                "_is_active": str(record.get("is_active", "")),
            },
        )

        existing = CASES_BY_ACTION.get(normalized)
        if existing:
            log_line(
                f"[DB] Duplicate action token {normalized} encountered; keeping first occurrence."
            )
            continue

        CASES_BY_ACTION[normalized] = case
        if source_norm == sources.UNREPORTED_JUDGMENTS:
            AJAX_FNAME_INDEX[normalized] = case
        CASES_ALL.append(case)
        CASES_BY_SOURCE[source_norm].append(case)
        loaded += 1

    log_line(
        f"[DB] Loaded {loaded} case token(s) from DB for source={source_norm}; "
        f"only_active={only_active} csv_version_id={csv_version_id}"
    )
    return CASES_BY_ACTION


def should_use_db_index() -> bool:
    """Return True when DB-backed case indexing should be used."""

    return config.use_db_cases()


def find_case_by_fname(
    fname: str,
    *,
    strict: bool = False,
    source: Optional[str] = None,
) -> Optional[CaseRow]:
    """Locate the CaseRow that best matches a fname token.

    When ``source`` is ``None``, this uses the legacy AJAX fname index loaded
    from the CSV path (unreported_judgments behaviour). When ``source`` is
    provided, lookup is restricted to that logical source using the DB-backed
    index populated via ``load_cases_index_from_db``.
    """

    if not fname:
        return None

    candidate = normalize_action_token(fname)
    if not candidate:
        return None

    if source is None:
        if not AJAX_FNAME_INDEX:
            log_line(
                "[INDEX] Case index is empty; load cases from CSV or DB before resolving fname tokens."
            )
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

    source_norm = sources.normalize_source(source)
    if source_norm == sources.UNREPORTED_JUDGMENTS:
        return find_case_by_fname(candidate, strict=strict, source=None)

    cases_for_source = CASES_BY_SOURCE.get(source_norm) or []
    if not cases_for_source:
        log_line(
            f"[MAPPING][WARN] Case index empty for source={source_norm}; call load_cases_index_from_db() first."
        )
        return None

    for case in cases_for_source:
        action = getattr(case, "action", "") or ""
        if action == candidate or getattr(case, "code", None) == candidate:
            return case

    if strict:
        return None

    for case in cases_for_source:
        action = getattr(case, "action", "") or ""
        if candidate and action.startswith(candidate):
            return case

    log_line(
        f"[MAPPING][WARN] No case found for fname={candidate!r} source={source_norm!r}"
    )
    return None


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
