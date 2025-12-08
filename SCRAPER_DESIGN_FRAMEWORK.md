SCRAPER DESIGN FRAMEWORK

Project: bailiikc – Cayman Islands Judicial Judgments Scraper
Status: Single source of truth for architecture & behaviour

⚠️ MUST READ:
Any time you (human or Codex) write, refactor, or debug code in this repo, you MUST:

Read this document in full.

Keep changes consistent with the architecture and invariants defined here.

Update this document whenever you add a major feature or change behaviour.

0. Purpose & Scope

This repository scrapes judgments from judicial.ky to build a high-quality dataset of:

PDFs of judgments (stored on disk).

Rich metadata (normalised, stored in a database).

This is an ingestion service that feeds later AI / RAG pipelines. It does not perform text extraction, summarisation, or embedding.

Current primary source:

https://judicial.ky/judgments/unreported-judgments/
(WordPress-powered page, AJAX → Box.com signed URLs, CSV index.)

Future secondary source (must be designed for, but not yet implemented):

https://judicial.ky/public-registers/

The scraper supports:

Manual one-off runs (from UI or CLI).

New-case-only runs via webhook (e.g. ChangeDetection.io).

Precise resume after crash or partial completion.

1. Tech Stack & Environment
1.1 Language & Core Libraries

Python 3.11+ (PEP 8 style, type hints where sensible).

HTTP & parsing:

requests for HTTP (CSV, Box URLs, etc.).

BeautifulSoup (if/when static HTML parsing is needed).

Browser automation:

Selenium + Chrome/Chromium (headless) for dynamic JS (WordPress nonce / AJAX flows).

Web framework:

Flask for UI/API (separate thin layer; scraper engine is independent).

Database:

SQLite (file-based DB living under /app/data/bailiikc.db).

Utilities:

logging for structured logs.

dataclasses for parameter objects.

1.2 Advanced Tools (for future extension, not used in core scraper)

Jina / Firecrawl: for large-scale text extraction and enrichment.

AgentQL / Multion: for complex or exploratory flows (e.g., login flows, multi-step forms).

Design constraint: Core scraping pipeline must not depend on these; instead, we expose clean extension points where they could be plugged in later.

1.3 Deployment assumptions

Running on a Hetzner VPS under Coolify.

Data directory is a mounted volume:

/app/data – persistent

/app/data/pdfs – downloaded PDFs

/app/data/logs – text and JSON logs

/app/data/csv – cached CSV versions

/app/data/bailiikc.db – SQLite database

The config.py module defines these paths and must be the only place where constants like /app/data are hard-coded.

2. High-Level Architecture
2.1 Layers

Scraper Engine (core business logic)
Stateless from the caller’s POV; all state stored in DB + filesystem.

Judicial Client (site-specific adapter)
Knows how to:

Acquire WordPress nonce + cookies.

Call admin-ajax.php with dl_bfile and fname to get signed Box.com URLs.

CSV Sync Module
Responsible for:

Fetching the remote judgments.csv.

Validating and versioning it.

Updating the cases table in SQLite.

Storage Layer (DB + Files)
Encapsulates:

Schema migrations.

Basic CRUD for cases, runs, downloads.

Presentation Layer

Flask web app (existing app/main.py) calling the engine.

CLI (argparse) entrypoint for ad hoc runs.

Key Modules

- **app/scraper/worklist.py**: DB-backed helpers for building per-run worklists
  (lists of cases to process) from the SQLite ``cases``/``csv_versions``
  tables. Provides full/new/resume worklists; resume derives retries from
  prior runs/downloads. Wiring into the scraper control flow for resume mode
  remains optional/flagged.

3. Data Storage & Schema
3.1 Directory Layout

/app/data/

bailiikc.db – SQLite database (source of truth).

csv/ – historical CSV copies (judgments_<timestamp>_<hash>.csv).

pdfs/ – all downloaded PDFs (nested subfolders allowed).

logs/

latest.log – human-readable text logs.

Optional: .jsonl structured logs.

Important:
JSON/JSONL files (old metadata.json, downloads.jsonl, state.json, etc.) are considered legacy. As we migrate, they remain as transitional artefacts only.

3.2 SQLite Schema (authoritative)

Codex MUST implement (and keep in sync) the following baseline schema.

Table: csv_versions

CREATE TABLE IF NOT EXISTS csv_versions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fetched_at      TEXT NOT NULL,        -- ISO8601 UTC
    source_url      TEXT NOT NULL,
    etag            TEXT,
    last_modified   TEXT,
    sha256          TEXT NOT NULL,
    row_count       INTEGER NOT NULL,
    valid           INTEGER NOT NULL,     -- 1 = valid, 0 = invalid
    error_message   TEXT,
    file_path       TEXT NOT NULL
);


Table: cases

CREATE TABLE IF NOT EXISTS cases (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    action_token_raw      TEXT NOT NULL,
    action_token_norm     TEXT NOT NULL,
    title                 TEXT,
    cause_number          TEXT,
    court                 TEXT,
    category              TEXT,
    judgment_date         TEXT,           -- "YYYY-MM-DD"
    is_criminal           INTEGER NOT NULL DEFAULT 0,
    is_active             INTEGER NOT NULL DEFAULT 1,
    source                TEXT NOT NULL,  -- 'unreported_judgments', 'public_registers', ...
    first_seen_version_id INTEGER NOT NULL,
    last_seen_version_id  INTEGER NOT NULL,
    FOREIGN KEY(first_seen_version_id) REFERENCES csv_versions(id),
    FOREIGN KEY(last_seen_version_id) REFERENCES csv_versions(id)
);
CREATE INDEX IF NOT EXISTS idx_cases_token_norm
    ON cases(action_token_norm);
CREATE INDEX IF NOT EXISTS idx_cases_source
    ON cases(source);

**Sources**

The scraper can target multiple logical sources of case data. These are
represented by stable string identifiers stored in ``cases.source`` and in
``runs.params_json.target_source``. At present, the only active source is:

* ``unreported_judgments`` – the Cayman "Unreported Judgments" list.

A future source (``public_registers``) is reserved but not yet wired into the
live scraper. For each run, ``runs.params_json.target_source`` determines which
subset of ``cases`` is considered when computing coverage and building
DB-backed worklists.

Entrypoints (CLI, webhook, UI) normalise requested sources via
``sources.normalize_source``; unknown or empty values fall back to the default
``unreported_judgments`` to avoid polluting the database while only a single
live source is supported.


Table: runs

CREATE TABLE IF NOT EXISTS runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL,        -- ISO8601 UTC
    ended_at        TEXT,                 -- nullable until completed
    trigger         TEXT NOT NULL,        -- 'ui', 'webhook', 'cli', 'cron'
    mode            TEXT NOT NULL,        -- 'full', 'new', 'resume'
    csv_version_id  INTEGER NOT NULL,
    params_json     TEXT NOT NULL,        -- JSON string of ScrapeParams
    status          TEXT NOT NULL,        -- 'running', 'completed', 'failed', 'aborted'
    error_summary   TEXT,
    FOREIGN KEY(csv_version_id) REFERENCES csv_versions(id)
);
CREATE INDEX IF NOT EXISTS idx_runs_started_at
    ON runs(started_at DESC);


Table: downloads

CREATE TABLE IF NOT EXISTS downloads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER NOT NULL,
    case_id         INTEGER NOT NULL,
    status          TEXT NOT NULL,     -- 'pending', 'in_progress', 'downloaded', 'failed', 'skipped'
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT,              -- nullable until attempted
    file_path       TEXT,              -- relative path under /app/data/pdfs
    file_size_bytes INTEGER,
    box_url_last    TEXT,
    error_code      TEXT,
    error_message   TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES runs(id),
    FOREIGN KEY(case_id) REFERENCES cases(id)
);
CREATE INDEX IF NOT EXISTS idx_downloads_run_case
    ON downloads(run_id, case_id);
CREATE INDEX IF NOT EXISTS idx_downloads_status
    ON downloads(status);


Optional Table: events (for detailed structured logs; nice-to-have)

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      INTEGER,
    case_id     INTEGER,
    event_type  TEXT NOT NULL,
    payload_json TEXT,
    created_at  TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES runs(id),
    FOREIGN KEY(case_id) REFERENCES cases(id)
);


Rule:
Any logic that currently depends on metadata.json, downloads.jsonl, or ad-hoc JSON files must gradually be migrated to reading/writing these tables instead.

4. CSV Sync Module
4.1 Responsibilities

Module: app/scraper/csv_sync.py (to be created / refactored from cases_index.py).

Responsibilities:

Fetch judgments.csv from the remote URL.

Detect changes via ETag / Last-Modified / SHA256.

Validate structure and basic semantics.

Store each version on disk + record it in csv_versions.

Diff against existing cases rows to:

Create new case rows.

Update changed case metadata.

Mark disappeared cases as inactive.

4.2 Normalisation rules

Define a single function:

def normalize_action_token(token: str) -> str:
    token = token.strip().upper()
    # Remove non-alphanumerics; adjust via migrations if judicial.ky changes format.
    return re.sub(r"[^A-Z0-9]+", "", token)


NEVER silently copy-paste or re-implement token normalisation.

If the site’s format changes, we update this function and run DB migrations.

4.3 Interface
@dataclass
class CsvSyncResult:
    version_id: int
    is_new_version: bool
    new_case_ids: list[int]
    changed_case_ids: list[int]
    removed_case_ids: list[int]

def sync_csv(source_url: str, session: requests.Session) -> CsvSyncResult:
    ...


Rules:

session must be reused (not a new client per call).

If the remote CSV cannot be fetched or is invalid:

Insert csv_versions row with valid = 0 and error_message.

Do not modify cases.

The scraper engine must then:

Either use the last known-good version (most recent valid=1), or

Abort the run cleanly with a clear error.

4.4 Case index backends (CSV vs DB)

- The canonical in-memory case index (CASES_BY_ACTION, AJAX_FNAME_INDEX,
  CASES_ALL) is still built from the CSV by default.
- An experimental DB-backed index exists for validation and manual testing:
  when ``BAILIIKC_USE_DB_CASES=1`` is set, the index is populated from the
  SQLite ``cases`` table instead of parsing the CSV.
- Behaviour must remain identical between backends; equivalence tests verify
  that titles, cause numbers, courts, categories, judgment dates, and action
  tokens match for the same CSV snapshot.

4.4 “New cases” definition

A case is new in this CSV version if:

first_seen_version_id = :current_version_id
AND is_active = 1
AND is_criminal = 0


This definition is used for:

“New cases only” mode.

Webhook-triggered runs.

5. Scraper Engine
5.1 Parameter & summary types

Create a dedicated engine module: app/scraper/engine.py (initially refactor from run.py).

from dataclasses import dataclass
from typing import Literal, Optional, List, Dict

Mode = Literal["full", "new", "resume"]
ResumeStrategy = Literal["none", "retry_failed_only", "redo_in_progress", "auto"]

@dataclass
class ScrapeParams:
    mode: Mode
    target_source: str            # 'unreported_judgments', later 'public_registers'
    per_download_delay: float
    max_retries: int
    concurrency: int              # start with 1 (sequential), design for future >1
    resume_strategy: ResumeStrategy
    dry_run: bool = False

@dataclass
class RunSummary:
    run_id: int
    cases_total: int
    cases_targeted: int
    downloads_completed: int
    downloads_failed: int
    downloads_skipped: int
    error_counts: Dict[str, int]
    started_at: str               # ISO8601
    ended_at: str                 # ISO8601

5.2 Engine entrypoint
def run_scrape(params: ScrapeParams) -> RunSummary:
    """
    High-level orchestration:
    1. Sync CSV (or reuse last good version).
    2. Create a 'runs' record.
    3. Resolve target cases based on mode + resume strategy.
    4. For each case, ensure there is a 'downloads' row and process it.
    5. Commit and return RunSummary.
    """

Current state: the legacy `run.py` integrates CSV sync + SQLite so that every run records a `runs` row and every per-case attempt updates `downloads` and `cases`. The scraper still uses JSON/state files to decide which cases to process and how to resume, but SQLite now also backs optional case indexing and reporting (via `db_case_index` and `db_reporting`), while JSON remains the primary control surface.


Pseudocode:

def run_scrape(params: ScrapeParams) -> RunSummary:
    http_session = build_http_session()  # shared requests.Session with UA, timeouts

    csv_result = csv_sync.sync_csv(config.CSV_URL, http_session)
    version_id = csv_result.version_id
    # The concrete CSV file path from sync_csv (csv_result.csv_path) is reused
    # to build the in-memory case index so the scrape operates on the exact
    # payload recorded in ``csv_versions``. When DB-backed indices are enabled,
    # the path is still captured for observability while the index is loaded
    # from SQLite instead.

    run_id = db.create_run(
        trigger=current_trigger(),      # 'ui', 'webhook', 'cli', ...
        mode=params.mode,
        csv_version_id=version_id,
        params=params,
    )

    try:
        cases = resolve_target_cases(run_id, params, csv_result)
        for case in cases:
            process_case_download(run_id, case, params, http_session)
            if params.per_download_delay > 0:
                sleep_with_jitter(params.per_download_delay)
        summary = db.build_run_summary(run_id)
        db.mark_run_completed(run_id)
        return summary
    except Exception as exc:
        db.mark_run_failed(run_id, error_summary=str(exc))
        raise


Key rule:
Engine does not know about Flask, templates, or HTTP request objects. It only uses DB, filesystem, and JudicialClient.

6. Judicial Client (site-specific adapter)
6.1 Interface

Define a protocol / abstract base:

class JudicialClient:
    def fetch_signed_box_url(self, case: "CaseRow") -> str:
        """
        Given a case (with action_token_norm or equivalent), 
        return a signed Box.com URL for the PDF.

        Must raise a structured exception on errors.
        """


CaseRow is a small typed representation from the DB (e.g., NamedTuple or dataclass).

6.2 Selenium-based implementation

Module: app/scraper/selenium_client.py (refactor existing).

Responsibilities:

Start a headless Chrome/Chromium with sensible options.

Visit unreported-judgments base URL.

Extract:

WordPress nonce.

Required cookies.

For each case:

Issue the equivalent of the dl_bfile AJAX call with fname (or other required parameters).

Decode the response (JSON or HTML).

Extract a direct Box.com URL using robust pattern matching.

General pseudo-logic:

class SeleniumJudicialClient(JudicialClient):
    def __init__(self, base_url: str, page_wait: int):
        ...

    def fetch_signed_box_url(self, case: CaseRow) -> str:
        # 1. Ensure browser and nonce are ready.
        # 2. Execute JS or direct POST to admin-ajax.php with action='dl_bfile'.
        # 3. Parse payload to find a URL matching 'https://...box.com/...'
        # 4. Validate URL format; raise if none found.


Important invariants:

Client must be reusable across cases within a run: do not restart the browser per case unless forced by errors.

On repeated failures (e.g. nonces expire), client can be restarted a limited number of times (backoff).

7. Download Processing & Resume Semantics
7.1 Status transitions in downloads

Each (run_id, case_id) row goes through:

pending → initial.

in_progress → when we start processing.

downloaded → when file is saved successfully.

failed → after max_retries or unrecoverable error.

skipped → consciously not attempted (e.g. filtered).

On crash, some rows may be stuck at in_progress. On next run, depending on resume_strategy, these should be treated as pending again if they’re stale.

7.2 Download processing function

Pseudocode:

def process_case_download(run_id, case, params, http_session):
    row = db.ensure_download_row(run_id, case.id)

    if row.status == "downloaded":
        return

    if params.resume_strategy in ("retry_failed_only", "auto") and row.status == "failed":
        pass  # retry
    elif params.resume_strategy in ("redo_in_progress", "auto") and row.status == "in_progress":
        # if stale, treat as pending
        if is_stale(row.last_attempt_at):
            pass
        else:
            return
    elif row.status not in ("pending", "in_progress", "failed"):
        return  # skip

    attempt = row.attempt_count + 1
    db.update_download_status(run_id, case.id, status="in_progress", attempt_count=attempt)

    try:
        if params.dry_run:
            # no actual HTTP; just simulate success
            db.update_download_success(run_id, case.id, file_path=None, file_size=0)
            return

        box_url = judicial_client.fetch_signed_box_url(case)
        file_path, file_size = download_pdf(box_url, case, http_session)
        db.update_download_success(run_id, case.id, file_path=file_path, file_size=file_size, box_url=box_url)

    except DownloadError as de:
        db.update_download_failure(run_id, case.id, error_code=de.code, error_message=str(de))
        if attempt >= params.max_retries:
            # leave as 'failed'; do not retry further in this run
            return
        else:
            # recursive or loop-based retry within process_case_download is allowed
            ...
    except Exception as exc:
        db.update_download_failure(run_id, case.id, error_code="unknown", error_message=str(exc))
        ...

7.3 Resume strategies

Codex must implement consistent semantics:

none

Ignore previous runs; compute target cases afresh.

New run_id and new downloads rows.

retry_failed_only

For a specific previous run (or all runs; precise rule to be defined in code comments), select cases where last download status is failed.

redo_in_progress

Treat stale in_progress as pending again.

auto

Combination:

Retry both stale in_progress and failed.

Skip downloaded and skipped.

“Stale” means: last_attempt_at older than a configurable threshold (e.g. 1 hour).

8. Disk & Performance Considerations
8.1 Disk capacity

We do not silently hard-stop on disk usage.

Instead:

Before a run:

Estimate approximate remaining space requirement (e.g. remaining_cases * average_pdf_size).

If free space is suspiciously low:

Record a warning in logs and DB.

Optionally limit number of cases to process.

During a run:

Regularly log free space.

If writes fail (IOError, OSError), mark relevant downloads as failed with error_code="disk_insufficient" and abort run cleanly.

8.2 Rate limiting & delays

ScrapeParams.per_download_delay defines base delay between cases.

Implement small random jitter:

def sleep_with_jitter(base: float):
    jitter = random.uniform(-0.2 * base, 0.2 * base)
    time.sleep(max(0.0, base + jitter))


For severe errors (e.g. repeated 503/429 from AJAX), use exponential backoff with upper bound.

8.3 Concurrency

Initial implementation: concurrency = 1 (sequential).

Design for future concurrency via concurrent.futures or asyncio, but ensure DB writes are transactional and safe.

9. Error Handling & Logging
9.1 Error code taxonomy

For downloads.error_code we use a restricted set of codes:

network_timeout

network_error

ajax_403

ajax_500

ajax_parse_error

csv_case_not_found

disk_insufficient

selenium_launch_failed

selenium_element_not_found

invalid_box_url

unknown

Codex must map exceptions to one of these codes (or unknown).

9.2 Logging

Text log: latest.log with human-readable entries.

Optional JSON log (events table or .jsonl).

Each major step logs:

CSV sync start/end.

New version or reuse existing.

Run start/end with ID and mode.

Download start/finish per case with result.

10. Web / CLI Interface
10.1 Flask

app/main.py remains the Flask entrypoint, but must:

Treat the scraper as a black box (run_scrape).

Not know about internal DB tables; only call read-only helpers for displaying run results.

Endpoints (existing names can be reused):

GET / – Dashboard (forms -> ScrapeParams).

POST /start – Start manual run.

POST /resume – Start a resume run with selected strategy.

GET /report – Latest run summary + case list of successfully downloaded cases. By default this reads the legacy JSON logs; when ``BAILIIKC_USE_DB_REPORTING=1`` it instead pulls rows from SQLite via ``db_reporting`` while keeping the row shape identical (``actions_token``, ``title``, ``subject``, ``court``, ``category``, ``judgment_date``, ``sort_judgment_date``, ``cause_number``, ``downloaded_at``, ``saved_path``, ``filename``, ``size_kb``).

GET /api/runs/latest, /api/downloaded-cases – UI/reporting endpoints. ``/api/runs/latest`` is DB-backed via ``db_reporting`` (uses run summary + aggregated download stats). ``/api/downloaded-cases`` reads legacy JSON by default but switches to the DB path when ``BAILIIKC_USE_DB_REPORTING=1`` while still returning only downloaded rows.

GET /api/db/runs/latest, /api/db/downloaded-cases – SQLite-backed reporting endpoints powered by ``db_reporting`` helpers. These remain available explicitly; ``/api/db/runs/latest`` is a raw summary row view without aggregates.

## Roadmap / PR Sequencing (DB, Reporting, Worklists)

This section tracks the planned incremental steps from the current state
towards a fully DB-first, worklist-driven scraper with RAG-friendly outputs.
It MUST be kept up to date when plans change.

- **PR13 – Run list API and csv_sync tidy-ups**
  - Add `_parse_judgment_date` logging for unparsed date formats.
  - Implement `db_reporting.list_recent_runs(limit)` and `GET /api/db/runs`.
  - Fix Known Limitations docs around CSV fetch vs versioning.

- **PR14 – DB-backed `/report` and `/api/downloaded-cases` behind flag**
  - Make the DB reporting path for `/report` and `/api/downloaded-cases` fully
    supported behind `BAILIIKC_USE_DB_REPORTING=1`.
  - Add tests to ensure JSON vs DB-backed reporting produce compatible shapes.

- **PR15 – JSON↔DB consistency checker (diagnostic only)**
  - Add helper(s) to cross-check JSON logs against SQLite for a given run.
  - Provide a CLI or dev-only API to surface diffs and build confidence in
    DB-backed reporting and control flow.

- **PR16 – DB-only worklist planner (no wiring yet)**
  - Implement `app.scraper.worklist` helpers that derive which cases to process
    for a run from the SQLite `cases` table (scoped to a given `csv_version_id`
    and `source`).
  - Provide `build_full_worklist(...)` and `build_new_worklist(...)` plus a
    dispatcher `build_worklist(mode, csv_version_id, source)`.
  - Support `"full"` (all active, non-criminal cases for the version) and
    `"new"` (cases where `first_seen_version_id == csv_version_id`) modes.
  - Define a `"resume"` mode stub that raises `NotImplementedError` for now;
    DB-backed resume semantics are implemented later in PR19.
  - Keep this isolated from `run.py` and cover it with unit tests.

- **PR17 – Wire new-only mode to DB worklist behind a flag**
  - For `scrape_mode="new"` and `BAILIIKC_USE_DB_WORKLIST_FOR_NEW=1`, drive
    scraping from the DB-backed worklist (`worklist.build_new_worklist`) instead
    of the legacy CSV-only planner.
  - Guarded by `config.use_db_worklist_for_new()`; when disabled, behaviour
    matches the legacy CSV path.
  - Covered by tests that monkeypatch the worklist builder and assert the
    planner consumes the DB items.

- **PR18 – Wire full mode to DB worklist behind a flag**
  - Extend the worklist-driven control flow to `mode="full"` behind
    `BAILIIKC_USE_DB_WORKLIST_FOR_FULL=1`, using
    `worklist.build_full_worklist(csv_version_id, source)`.
  - Guarded by `config.use_db_worklist_for_full()`; legacy CSV behaviour is
    preserved when the flag is off.
  - Tests confirm the flag routing and worklist consumption.

- **PR19 – DB-first resume semantics (implemented in worklist + planning helpers)**
  - Formalise and implement resume semantics using `runs` and `downloads`
    status/error codes.
  - Integrate with the worklist builder for `mode="resume"`; run.py wiring
    remains optional/flagged and is activated in PR21 for flagged resume
    runs.

- **PR20 – Promote DB worklists and reporting to default (implemented)**
  - Enable DB worklists and DB reporting by default, keeping legacy JSON as an
    emergency fallback only.
  - Update docs to reflect DB-first architecture.

- **PR21 – Wire DB resume worklists into run.py (flagged, implemented)**
  - When `scrape_mode="resume"` and `BAILIIKC_USE_DB_WORKLIST_FOR_RESUME=1`,
    run.py now plans via `worklist.build_resume_worklist` and filters
    pagination clicks to the DB-derived tokens.
  - Defaults continue to run `new`/`full` as before; resume remains opt-in.

- **PR21+ – public-registers and RAG pipeline**
  - Extend CSV sync and case indexing to `public-registers` as a second
    source.
  - Design and expose AI/RAG-oriented export endpoints and (optionally)
    text extraction staging hooks.

## Scraper Hardening Roadmap (Playwright, Box, Robustness)

This section tracks planned work specifically on the scraping mechanism
(Playwright, Box dl_bfile requests, retries, timeouts, observability). It
complements the DB/worklist roadmap above.

- **PR-S1 – Instrument the current scraper without changing behaviour**
  - Add structured logging in `run.py` and the Playwright client for per-run
    and per-case phases (navigation, table detection, dl_bfile request,
    response handling).
  - Ensure these logs feed into run telemetry JSON and the `runs`/`downloads`
    tables where appropriate.
  - Implement a stable `[SCRAPER][PHASE] key=value` vocabulary across
    navigation (`NAV`), planning (`PLAN`), pagination (`TABLE`), per-token
    choices (`DECISION`), Box downloads (`BOX`), checkpoint/resume decisions
    (`STATE`), and fatal errors (`ERROR`) so log parsing can answer "what
    happened to token X" or "why did run Y stop".

- **PR-S2 – Isolate Box/AJAX interaction into a `box_client` abstraction**
  - Extract the dl_bfile request/response logic into a dedicated module
    (`box_client`) with a small `BoxDownloadResult` dataclass.
  - Make `run.py` and the Playwright client call this abstraction rather than
    inlining request construction/parsing. (Implemented: `download_pdf`
    centralises Box HTTP handling and `[SCRAPER][BOX]` logging; `run.py`
    delegates its download helper to this module.)

- **PR-S3 – Explicit per-case download state machine**
  - Introduce a `CaseDownloadState` helper that wraps the `downloads` row for a `(run_id, case_id)` pair and centralises all status transitions for that case within a run.
  - Use a constrained `DownloadStatus` enum with the following values:
    - `pending`: initial state for a case/run before any attempt has started.
    - `in_progress`: a Playwright/Box attempt is currently being made for this case in this run.
    - `downloaded`: a PDF has been successfully saved for this case in this run (terminal state).
    - `skipped`: this case was deliberately not attempted (e.g. worklist filter, already downloaded) or treated as a permanent skip for this run.
    - `failed`: this case encountered an error in this run (e.g. network, disk full) and may be retried by higher-level logic.
  - `CaseDownloadState.start(run_id, case_id, box_url)`:
    - Ensures a `downloads` row exists for the `(run_id, case_id)` pair.
    - Increments `attempt_count` and sets `status="in_progress"`, `last_attempt_at` and `box_url_last` for this attempt.
    - Emits a `[SCRAPER][STATE]` event capturing `from_status`, `to_status`, `attempt`, `run_id`, `case_id`, and `box_url`.
  - `CaseDownloadState.mark_downloaded(...)`, `.mark_skipped(reason)`, and `.mark_failed(error_code, error_message)`:
    - Perform guarded transitions to `downloaded`, `skipped`, and `failed` respectively, updating `status`, `attempt_count`, timestamps and error fields in the `downloads` row.
    - Emit a `[SCRAPER][STATE]` event for each transition with `from_status`, `to_status`, `attempt`, `run_id`, `case_id`, and any relevant file/error metadata.
  - Transitions are restricted:
    - `downloaded` is treated as terminal; attempting to move from `downloaded` to any other status logs a `[SCRAPER][ERROR] invalid_transition_after_download` event and leaves the DB row unchanged.
    - Other transitions (e.g. `pending → in_progress`, `in_progress → downloaded|skipped|failed`) are allowed; callers never write `downloads.status` directly.
  - The scraper core (`run.py`) must use `CaseDownloadState` whenever it:
    - Starts a Box/Playwright attempt for a case.
    - Decides to skip a case (e.g. worklist filter, already downloaded, invalid token).
    - Records a failed attempt due to scraping or IO errors.

- **PR-S4 – Centralised retry/backoff policy**
  - Introduce a small `retry_policy` helper with a single entrypoint
    `decide_retry(error_code: Optional[str], attempt: int) -> bool` that
    owns all logic for “should we click again for this case in this run?”.
  - Standardise a small set of scraper-level `error_code` values:
    - Retryable (bounded by attempt caps):
      - `download_other` – generic download failure (network, unexpected HTTP, etc).
    - Non-retryable (fail closed):
      - `disk_full` – out of space before/during download.
      - Logical skips: `invalid_token`, `csv_miss`, `worklist_filtered`, `seen_history`,
        `already_downloaded`, `in_run_dup`, `exists_ok`.
      - Click failures already retried locally: `click_timeout`.
  - Policy:
    - `disk_full` is always non-retryable for the run; the scraper sets a
      `stop_reason` and halts NEW/FULL rather than trying again.
    - Logical skip reasons (`invalid_token`, `csv_miss`, `worklist_filtered`,
      `seen_history`, `already_downloaded`, `in_run_dup`, `exists_ok`) are never retried.
    - `click_timeout` failures are logged to the DB (when a case_id is known) and
      captured in `failed_items` but treated as non-retryable at the policy layer.
    - `download_other` may be retried up to a small cap (3 attempts per
      `(run_id, case_id)`), treating transient HTTP/network issues as
      retryable but bounded. The current implementation performs a single
      retry sweep per run while enforcing the cap via the authoritative
      `CaseDownloadState.attempt_count` value.
    - `decide_retry` emits a structured `[SCRAPER][STATE] phase="retry_decision"`
      event with a `kind` field (`non_retryable`, `retryable`, `capped`, etc.) so
      decisions remain reconstructable even for unknown error codes.
  - Integration points:
    - Extend the `failed_items` entries collected in `run.py` to include
      `case_id`, `error_code`, and `attempt` (derived from
      `CaseDownloadState.attempt_count` when the failure occurred).
    - Update `retry_failed_downloads(...)` to:
      - For each failed item, call `decide_retry(error_code, attempt)`.
      - Skip non-retryable items and emit `[SCRAPER][DECISION]` events with
        `decision=skip_retry`, `reason=<error_code>` and `attempt`.
      - For retryable items, re-click the corresponding download button on
        the DataTable page; subsequent Box/Playwright handling and DB
        updates proceed via the existing `CaseDownloadState` and
        `handle_dl_bfile_from_ajax` flow.
    - Ensure that all retry decisions (both “retry” and “do not retry”) are
      reflected in `[SCRAPER][STATE]` / `[SCRAPER][DECISION]` logs so we can
      reconstruct the rationale for each case.

- **PR-S5 – Timeouts, page lifecycle, and Playwright robustness**
  - Add explicit, configurable timeouts for navigation, selectors, and
    downloads (see `PLAYWRIGHT_*` knobs in `app/scraper/config.py`). New config
    names derive from `BAILIIKC_*` env vars: `PLAYWRIGHT_NAV_TIMEOUT_SECONDS`
    (`BAILIIKC_NAV_TIMEOUT_SECONDS`, default 25s),
    `PLAYWRIGHT_SELECTOR_TIMEOUT_SECONDS` (`BAILIIKC_SELECTOR_TIMEOUT_SECONDS`,
    default 20s), and `PLAYWRIGHT_DOWNLOAD_TIMEOUT_SECONDS`
    (`BAILIIKC_DOWNLOAD_TIMEOUT_SECONDS`, default 120s). Click pacing
    timeouts remain configurable via dedicated knobs:
    `PLAYWRIGHT_CLICK_TIMEOUT_MS`, `PLAYWRIGHT_POST_CLICK_SLEEP_SECONDS`,
    `PLAYWRIGHT_RETRY_PAGE_SETTLE_SECONDS`, and
    `PLAYWRIGHT_RETRY_AFTER_SWEEP_SECONDS`. All values can be tuned via the
    environment without code changes.
  - Wrap Playwright usage in helpers that ensure proper cleanup and clear
    error reporting when pages/timeouts misbehave.

- **PR-S6 – Concurrency and resource controls (if needed)**
  - Concurrency remains single-browser/single-page by default, but download
    capacity is now explicit via a `DownloadExecutor` wrapper around Box HTTP
    fetches. The executor clamps worker counts to at least 1 and is disabled
    when `BAILIIKC_ENABLE_DOWNLOAD_EXECUTOR=0`.
  - Resource knobs (all env-driven, defined in `config.py`):
    - `BAILIIKC_MAX_PARALLEL_DOWNLOADS` → `MAX_PARALLEL_DOWNLOADS` (default 1).
    - `BAILIIKC_MAX_PENDING_DOWNLOADS` → `MAX_PENDING_DOWNLOADS` (default 100
      queued items before falling back to synchronous execution).
    - `BAILIIKC_ENABLE_DOWNLOAD_EXECUTOR` gates the executor entirely; when off
      or when `MAX_PARALLEL_DOWNLOADS <= 1`, downloads execute inline.
  - Telemetry for peak in-flight downloads is emitted via `[SCRAPER][STATE]`
    lines with `phase=download_executor` and fields `peak_in_flight` and
    `max_parallel` to keep observability aligned with the existing log format.
  - Queue saturation emits a `[SCRAPER][STATE]` `queue_overflow` event and
    forces synchronous execution rather than dropping work, preserving
    per-case serialisation and DB invariants.

- **PR-S7 – Offline replay harness for scraper logic**
  - Introduce fixture capture during `dl_bfile` handling when
    `BAILIIKC_RECORD_REPLAY_FIXTURES=1`, writing JSONL under
    `/app/data/replay_fixtures/run_<id>_dl_bfile.jsonl` with the payload, Box
    URL, mode, tokens, and case context snapshot for each observed response.
  - Add `app/scraper/replay_harness.py` to consume those fixtures offline,
    reusing `handle_dl_bfile_from_ajax` with sandboxed paths. A `ReplayConfig`
    selects dry-run vs. sandbox output roots; dry-run forces
    `REPLAY_SKIP_NETWORK` to avoid real HTTP. `[SCRAPER][REPLAY]` events mark
    start/end and download stubs using `phase` fields like `start`, `end`, and
    `download_stub`.
  - `BAILIIKC_REPLAY_SKIP_NETWORK` short-circuits Box downloads in replay mode
    (and can be set manually) while preserving the downstream state machine and
    logging flow.

10.2 Webhook (ChangeDetection.io)

POST /webhook/changedetection:

- Authentication: shared secret `BAILIIKC_WEBHOOK_SHARED_SECRET` is required. Token may be supplied via `X-Webhook-Token` header or `?token=` query string. When the secret is unset, the route responds with `{ok: false, error: "webhook_disabled"}` and 404 to avoid accidental exposure. Invalid tokens return 403 with `{ok: false, error: "invalid_token"}` and a `[SCRAPER][ERROR] phase=webhook` event.
- Payload: accepts JSON or form/query parameters. Required fields are `mode="new"` and `target_source="unreported_judgments"`; `new_limit` is optional and defaults to `min(SCRAPE_NEW_LIMIT, WEBHOOK_NEW_LIMIT_MAX)`. `new_limit` must be >=1 and is clamped to `WEBHOOK_NEW_LIMIT_MAX` (default 50), emitting a `[SCRAPER][STATE] phase=webhook kind=limit_clamped` event when clamped.
- Behaviour: executes a synchronous “new” scrape with `trigger="webhook"`, `resume_mode="none"`, and `limit_pages=[0]` to bound runtime. `run_scrape` records the run row and returns the summary used in the HTTP response.
- Response: on success returns 200 JSON `{ok, entrypoint:"webhook", mode:"new", target_source:"unreported_judgments", run_id, csv_version_id, summary:{processed, downloaded, skipped, failed}}`. Invalid payloads respond with 400 `{ok: false, error: "invalid_params", details:[...]}`. Config validation failures respond with `{ok: false, error: "config_invalid"}`.

11. General Web-Scraping Conventions (Project-Wide)

Codex must also respect these global rules:

Always set a realistic User-Agent header (current one is fine; update as needed).

Respect the target site’s ToS and robots.txt unless project owner explicitly decides otherwise.

Use requests with sensible timeouts, not default infinite waits.

Catch and log requests.Timeout and other network errors explicitly.

Introduce random jitter into delays to avoid obvious scraping patterns.

Config safety and guardrails:

- `app.scraper.config_validation.validate_runtime_config(entrypoint, mode)` enforces runtime invariants before any scrape runs. Entry points (CLI, UI, webhook, replay) must call this to block dangerous combinations.
- `REPLAY_SKIP_NETWORK` is only allowed for `entrypoint in {"replay", "tests"}`; enabling it elsewhere raises a `ValueError` with a `[SCRAPER][ERROR] phase=config` log.
- Download executor knobs (`ENABLE_DOWNLOAD_EXECUTOR`, `MAX_PARALLEL_DOWNLOADS`, `MAX_PENDING_DOWNLOADS`) are clamped to sane minimums rather than crashing. Invalid timeouts or negative `MIN_FREE_MB` values raise clear configuration errors.

11. Run coverage & health

- Extend the `runs` table with coverage counters (`cases_total`, `cases_planned`, `cases_attempted`, `cases_downloaded`, `cases_failed`, `cases_skipped`), `coverage_ratio`, and `run_health` (ok/partial/failed/suspicious).
- At the end of each run, derive counts from the `cases` table (active `unreported_judgments` rows scoped to the run’s `csv_version_id`) and `downloads` rows for that run. When DB worklists are enabled, worklist sizes drive `cases_planned`; otherwise, fall back to distinct `downloads.case_id` counts.
- Compute `coverage_ratio = cases_downloaded / max(cases_planned, 1)` and classify `run_health`:
  - No planned cases: suspicious if `cases_total > 0` and `cases_downloaded == 0`, otherwise ok.
  - Planned cases present: suspicious if `cases_attempted == 0`; ok when `coverage_ratio >= 0.95` and `cases_failed == 0`; partial when `coverage_ratio >= 0.6`; failed when `coverage_ratio < 0.1` with failures recorded; otherwise partial.
- Persist coverage and `run_health` back to the `runs` row and expose them via reporting helpers and APIs (`/api/db/runs`, `/api/db/runs/<run_id>/health`).
- Download diagnostics must validate `run_id` early: `summarise_downloads_for_run` should raise `RunNotFoundError` when the run row is missing so HTTP endpoints can return a 404 (`/api/db/runs/<run_id>/download-summary`, `/api/db/runs/<run_id>/health`).

11.1 Health & diagnostics

- Add `app.scraper.healthcheck.run_health_checks(entrypoint)` that validates configuration, ensures filesystem readiness (`ensure_dirs`, `disk_has_room`), checks SQLite connectivity/schema, and optionally includes JSON-vs-DB consistency diagnostics. Logging via `_scraper_event` with `phase="health"` must be best-effort.
- Provide a CLI entrypoint (`python -m app.scraper.healthcheck`) that prints check results and exits non-zero when mandatory checks fail.
- Expose `GET /api/health` returning `{ok, checks}` with HTTP 200 when healthy or 503 on failure. Treat the consistency check as non-fatal for the HTTP entrypoint.

12. Extension Points (AI / Jina / Firecrawl / AgentQL / Multion)

For future RAG/AI processing:

We will add a separate pipeline that:

Reads cases and downloads from SQLite.

Reads PDFs from /app/data/pdfs.

Writes extracted text, embeddings, etc. into new tables / JSON files.

To support this:

Keep file paths stable and deterministic (e.g. based on action_token_norm).

Consider adding a case_text or extraction_status table later – but do not bake AI logic into the scraper.

Advanced tools (Jina, Firecrawl, AgentQL, Multion) will be used there, not here.

13. Maintenance Rules

Any major change (new module, new table, new endpoint) must be reflected here.

If you change the DB schema:

Update the CREATE TABLE definitions.

Document any migration script / logic.

When debugging:

Before patching code, update or annotate this document with what’s broken and what will change.

After patching, confirm the new behaviour matches this spec (or update the spec accordingly).

## Error taxonomy, retries, and PDF validation

- Download failures use a small internal taxonomy defined in `app.scraper.error_codes.ErrorCode`; codes are persisted to `downloads.error_code` and emitted via `_scraper_event`.
- Box/PDF downloads validate `%PDF` magic bytes and a minimum byte length to avoid storing truncated artefacts. Replay stub PDFs are padded to satisfy the same check.
- Retry decisions are centralised in `retry_policy.decide_retry`, which considers `error_code` and HTTP status. Retryable cases (network issues, Box rate limits, HTTP 5xx) use capped exponential backoff from `compute_backoff_seconds`.
