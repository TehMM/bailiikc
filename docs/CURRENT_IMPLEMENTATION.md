# Current Scraper Implementation

## Overview
The existing scraper targets the Cayman Islands Judicial website’s unreported judgments page. It pulls the published judgments CSV on each run, interprets the `Actions` column tokens, and drives a Playwright-based flow (with legacy Selenium helpers unused in the current path) that issues the official `dl_bfile` AJAX requests to Box.com. Downloads, metadata, and resume information remain stored on disk using JSON/CSV files; SQLite is now used in parallel for observability (CSV versioning, per-run records, and per-case download tracking) without changing scraper control flow. DB-backed reporting helpers power the `/api/db/...` endpoints and, when `BAILIIKC_USE_DB_REPORTING=1`, also feed `/report` and `/api/downloaded-cases` with only downloaded cases while keeping the JSON row shape intact. Legacy JSON files remain the default source for reporting when the flag is not set.

## Key Modules
- **app/main.py**: Flask web UI with forms for scrape/resume/reset actions, webhook endpoint, and routes for reports, exports, and file serving. Launches background scrape threads that call `run_scrape`. `/report` and `/api/downloaded-cases` read JSON logs by default but switch to SQLite (still only downloaded rows) when `BAILIIKC_USE_DB_REPORTING=1`. Also exposes `/api/db/runs/latest` and `/api/db/downloaded-cases` endpoints backed by SQLite.
- **main.py (repo root)**: Thin entrypoint that imports `app.main` (which initialises directories and schema) and starts the Flask app; suitable for local development or generic hosting environments.
- **app/scraper/config.py**: Central constants for data paths, URLs, defaults, and HTTP headers. Defines `/app/data` layout, scrape defaults, and helper predicates for mode detection.
- **app/scraper/run.py**: Primary scraper engine using Playwright. Loads the judgments CSV, builds in-memory case indices, coordinates page navigation and AJAX monitoring, downloads PDFs, and writes metadata/logs/state. Contains checkpoint logic and resume handling.
- **app/scraper/cases_index.py**: CSV loader and normaliser. Parses `Actions` tokens, builds `CASES_BY_ACTION`, `AJAX_FNAME_INDEX`, and `CASES_ALL` for lookup during scraping.
- **app/scraper/downloader.py**: Legacy Selenium-based downloader helpers (currently unused by Playwright flow). Handles AJAX nonce acquisition, Box URL fetching, PDF streaming, duplicate detection, and metadata updates.
- **app/scraper/selenium_client.py**: Selenium utilities for nonce extraction and AJAX POST execution to retrieve Box URLs. Used by the legacy downloader path.
- **app/scraper/utils.py**: Shared utilities: directory setup, logging configuration, filename sanitisation, PDF path building, metadata persistence, ZIP generation, JSON helpers, and duplicate detection.
- **app/scraper/state.py**: Checkpoint persistence and derivation from logs (`state.json`, scrape log parsing) to support resume-on-crash flows.
- **app/scraper/telemetry.py**: Lightweight telemetry writer producing per-run JSON files and helpers for locating the latest run and pruning exports.
- **app/scraper/export_excel.py**: Builds Excel workbooks from telemetry JSON for download via the API.
- **app/scraper/db_reporting.py**: Read-only helpers for querying runs and downloads from SQLite to support DB-backed reporting.

## Current Scrape Workflow
1. **UI submission**: `app/main.py` renders forms and reads user input (base URL, waits, limits, resume options). On submit, it saves defaults, optionally resets state, and starts a background thread that calls `run_scrape` with the collected parameters.
2. **CSV load and case index**: `run.py` invokes `load_cases_index(config.CSV_URL)` to fetch and parse `judgments.csv`, populating global indices (`CASES_BY_ACTION`, etc.).
3. **Playwright session**: The scraper launches Chromium, loads the target page, scrolls to trigger DataTables loading, and navigates through pages/rows. It monitors `admin-ajax.php` responses to capture `dl_bfile` payloads and Box URLs.
4. **Download handling**: When a Box URL is observed, `handle_dl_bfile_from_ajax` streams the PDF (via Playwright’s `context.request`), writes files under `/app/data/pdfs`, updates in-memory metadata, and appends entries to `downloads.jsonl`. Filename fallbacks and duplicate checks rely on helpers from `utils.py`.
5. **Resume/state**: `Checkpoint` objects inside `run.py`, plus `state.py` helpers and scrape logs, maintain progress (`state.json`, `run_state.json`, latest scrape log). Resume modes decide whether to reuse these checkpoints or restart.
6. **Reporting**: After the run, summaries/log paths are stored in JSON files. The Flask report page reads `downloads.jsonl`, `last_summary.json`, and current logs to show tables, filters, and download links by default. When `BAILIIKC_USE_DB_REPORTING=1`, `/report` and `/api/downloaded-cases` instead read downloaded rows from SQLite via `db_reporting` but return the same row structure (`actions_token`, `title`, `subject`, `court`, `category`, `judgment_date`, `sort_judgment_date`, `cause_number`, `downloaded_at`, `saved_path`, `filename`, `size_kb`). Telemetry is also written to per-run JSON for Excel export.

## Current Data & State Files
- **metadata.json**: Primary metadata store with `downloads` list; updated on each successful download.
- **downloads.jsonl**: Append-only log of download attempts with actions token, titles, sizes, timestamps, and saved paths (used for the report table).
- **state.json**: Persisted checkpoint referenced by resume logic.
- **run_state.json**: Additional run progress tracking (written by `save_checkpoint`).
- **last_summary.json**: Summary of the most recent run (counts, mode, log file path) for display in the UI.
- **scrape_log.txt / scrape_*.log**: Human-readable scrape logs stored under `/app/data/logs`, tailed by the UI for live updates.

## SQLite usage (logging by default)
- **CSV sync**: Each run syncs `judgments.csv` via `csv_sync.sync_csv`, recording a `csv_versions` row and upserting `cases`.
- **Runs table**: `run_scrape` now inserts into `runs` with trigger, mode, parameters, and the CSV version used. Completion and failures are marked at the end of the run. `db_reporting.get_latest_run_id` and `get_run_summary` provide read-only access for reporting APIs.
- **Downloads table**: Per-case attempts are logged during scraping with statuses (`pending`, `in_progress`, `downloaded`, `skipped`, `failed`), attempt counts, timestamps, optional file info, and error details. `/api/db/downloaded-cases` reads from this table to offer DataTables-compatible payloads without touching the legacy JSON files, and `/report` plus `/api/downloaded-cases` can switch to this data (still filtered to downloaded cases) when `BAILIIKC_USE_DB_REPORTING=1`.
- **Case index backend**: The scraper still builds its in-memory case index from the CSV by default. Setting `BAILIIKC_USE_DB_CASES=1` makes `cases_index` load from the SQLite `cases` table instead; this mode is intended for validation and should be behaviourally identical to the CSV path.
- **Behaviour**: JSON files remain the source of truth for scraper control/resume. SQLite is write-only for observability in the current implementation except when explicitly opted into the DB-backed case index.

## Known Limitations / Fragility
- The judgments CSV is fetched fresh each run without caching/versioning; network hiccups can affect availability.
- Resume relies on JSON checkpoints and log parsing, leading to complexity when recovering mid-run.
- Disk space guardrails are basic (free-space checks via `utils.disk_has_room`), and filename length issues require fallbacks.
- The Playwright AJAX capture flow is delicate; nonce/session handling and Box URL extraction were tuned through trial and error and should remain untouched for now.
