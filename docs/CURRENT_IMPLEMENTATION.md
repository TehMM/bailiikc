# Current Scraper Implementation

## Overview
The existing scraper targets the Cayman Islands Judicial website’s unreported judgments page. It pulls the published judgments CSV on each run, interprets the `Actions` column tokens, and drives a Playwright-based flow (with legacy Selenium helpers unused in the current path) that issues the official `dl_bfile` AJAX requests to Box.com. Downloads, metadata, and resume information remain stored on disk using JSON/CSV files; SQLite now tracks CSV versions, runs, cases, and per-case download attempts and is the default backend for reporting and worklists. Environment flags can force legacy JSON/CSV paths when needed. Scraper control flow still uses JSON/state for checkpoints and resume, but DB-backed resume worklists now exist for targeting retries from prior runs. DB-backed reporting helpers power the `/api/db/...` endpoints and, when DB reporting is enabled (default), also feed `/report` and `/api/downloaded-cases` with only downloaded cases while keeping the JSON row shape intact. Legacy JSON files remain available as an explicit fallback when the flag is set to `0`.

## Key Modules
- **app/main.py**: Flask web UI with forms for scrape/resume/reset actions, webhook endpoint, and routes for reports, exports, and file serving. Launches background scrape threads that call `run_scrape`. `/report` and `/api/downloaded-cases` read JSON logs by default but switch to SQLite (still only downloaded rows) when `BAILIIKC_USE_DB_REPORTING=1`. `/api/runs/latest` is DB-backed via `db_reporting` for run metadata and download aggregates. The app also exposes `/api/db/runs/latest`, `/api/db/downloaded-cases`, and `/api/db/runs/<run_id>/downloaded-cases` endpoints backed by SQLite.
- **main.py (repo root)**: Thin entrypoint that imports `app.main` (which initialises directories and schema) and starts the Flask app; suitable for local development or generic hosting environments.
- **app/scraper/config.py**: Central constants for data paths, URLs, defaults, and HTTP headers. Defines `/app/data` layout, scrape defaults, and helper predicates for mode detection.
- **app/scraper/run.py**: Primary scraper engine using Playwright. Loads the judgments CSV, builds in-memory case indices, coordinates page navigation and AJAX monitoring, downloads PDFs, and writes metadata/logs/state. Contains checkpoint logic and resume handling. When `scrape_mode="resume"` and `BAILIIKC_USE_DB_WORKLIST_FOR_RESUME=1`, resume planning draws from the DB-backed worklist; with the flag disabled, legacy checkpoint/log-driven behaviour remains unchanged.
- **app/scraper/box_client.py**: Shared Box download helper that streams PDFs, enforces `%PDF` magic bytes, handles retries/backoff, and logs `[SCRAPER][BOX]` events. Used by `run.py` and any future Box consumers.
- **app/scraper/replay_harness.py**: Offline replay entrypoint that consumes captured `dl_bfile` fixtures (`/app/data/replay_fixtures/run_<id>_dl_bfile.jsonl`), replays them through `handle_dl_bfile_from_ajax`, and writes output to sandboxed directories for dry-run or test-only validation. Invokes config validation to keep replay-only flags scoped.
- **app/scraper/config_validation.py**: Central guardrail for runtime configuration that enforces safe combinations (e.g., forbidding `REPLAY_SKIP_NETWORK` outside replay/tests, clamping executor knobs to sensible minimums, and rejecting invalid timeout or disk thresholds). Entry points (UI, CLI, webhook, replay) call `validate_runtime_config(entrypoint, mode)` before running.
- **app/scraper/cases_index.py**: CSV loader and normaliser. Parses `Actions` tokens, builds `CASES_BY_ACTION`, `AJAX_FNAME_INDEX`, and `CASES_ALL` for lookup during scraping.
- **app/scraper/downloader.py**: Legacy Selenium-based downloader helpers (currently unused by Playwright flow). Handles AJAX nonce acquisition, Box URL fetching, PDF streaming, duplicate detection, and metadata updates.
- **app/scraper/selenium_client.py**: Selenium utilities for nonce extraction and AJAX POST execution to retrieve Box URLs. Used by the legacy downloader path.
- **app/scraper/utils.py**: Shared utilities: directory setup, logging configuration, filename sanitisation, PDF path building, metadata persistence, ZIP generation, JSON helpers, and duplicate detection.
- **app/scraper/state.py**: Checkpoint persistence and derivation from logs (`state.json`, scrape log parsing) to support resume-on-crash flows.
- **app/scraper/telemetry.py**: Lightweight telemetry writer producing per-run JSON files for Excel export plus pruning utilities for generated workbooks.
- **app/scraper/export_excel.py**: Builds Excel workbooks from telemetry JSON for download via the API.
- **app/scraper/db_reporting.py**: Read-only helpers for querying runs and downloads from SQLite to support DB-backed reporting.
- **app/scraper/healthcheck.py**: Lightweight health diagnostics for config/filesystem/DB connectivity. Exposes `run_health_checks` for CLI and backs `/api/health`.
- **app/scraper/worklist.py**: DB-backed helpers for assembling per-run worklists (lists of cases to process) from the SQLite ``cases`` table for a given ``csv_version_id``. Supports ``build_full_worklist``, ``build_new_worklist``, and ``build_resume_worklist`` (derived from prior runs/downloads) plus a dispatcher ``build_worklist(...)``. The scraper uses these helpers for ``mode="new"`` and ``mode="full"`` when the corresponding flags are enabled; resume wiring is now active for `scrape_mode="resume"` when the DB resume worklist flag is on.

## Current Scrape Workflow
1. **UI submission**: `app/main.py` renders forms and reads user input (base URL, waits, limits, resume options). On submit, it saves defaults, optionally resets state, calls `validate_runtime_config("ui", mode=...)`, and starts a background thread that calls `run_scrape` with the collected parameters.
2. **CSV load and case index**: `run.py` syncs `judgments.csv` via `csv_sync.sync_csv`, recording a `csv_versions` row plus a concrete CSV file path and row count. That path is then passed into `load_cases_index` so the in-memory indices (`CASES_BY_ACTION`, etc.) are built from the exact payload tied to the run. With `BAILIIKC_USE_DB_CASES=1` (default), the CSV path is still recorded for observability while the index is built from SQLite instead; setting the flag to `0` forces the legacy CSV-driven index.
3. **Playwright session**: The scraper launches Chromium, loads the target page, scrolls to trigger DataTables loading, and navigates through pages/rows. It monitors `admin-ajax.php` responses to capture `dl_bfile` payloads and Box URLs. Navigation, selector waits, and HTTP download timeouts are centralised via `PLAYWRIGHT_NAV_TIMEOUT_SECONDS`, `PLAYWRIGHT_SELECTOR_TIMEOUT_SECONDS`, and `PLAYWRIGHT_DOWNLOAD_TIMEOUT_SECONDS` (derived from `BAILIIKC_*` env vars) to avoid scattered magic numbers. Fixture capture for offline replay writes JSONL lines under `replay_fixtures` when `BAILIIKC_RECORD_REPLAY_FIXTURES=1`.
4. **Download handling**: When a Box URL is observed, `handle_dl_bfile_from_ajax` streams the PDF (via Playwright’s `context.request`), writes files under `/app/data/pdfs`, updates in-memory metadata, and appends entries to `downloads.jsonl`. Filename fallbacks and duplicate checks rely on helpers from `utils.py`. Download execution is routed through `DownloadExecutor`, which enforces `MAX_PARALLEL_DOWNLOADS`/`MAX_PENDING_DOWNLOADS` but defaults to synchronous behaviour when set to 1 or when disabled.
5. **Resume/state**: `Checkpoint` objects inside `run.py`, plus `state.py` helpers and scrape logs, maintain progress (`state.json`, `run_state.json`, latest scrape log). Resume modes decide whether to reuse these checkpoints or restart.
6. **Reporting**: After the run, summaries/log paths are stored in JSON files. The Flask report page reads downloaded rows from SQLite via `db_reporting` by default while preserving the historical row structure (`actions_token`, `title`, `subject`, `court`, `category`, `judgment_date`, `sort_judgment_date`, `cause_number`, `downloaded_at`, `saved_path`, `filename`, `size_kb`). Setting `BAILIIKC_USE_DB_REPORTING=0` forces the legacy JSON path, which reads `downloads.jsonl`, `last_summary.json`, and current logs to show tables, filters, and download links. Telemetry is also written to per-run JSON for Excel export.

## Current Data & State Files
- **metadata.json**: Primary metadata store with `downloads` list; updated on each successful download.
- **downloads.jsonl**: Append-only log of download attempts with actions token, titles, sizes, timestamps, and saved paths (used for the report table).
- **state.json**: Persisted checkpoint referenced by resume logic.
- **run_state.json**: Additional run progress tracking (written by `save_checkpoint`).
- **last_summary.json**: Summary of the most recent run (counts, mode, log file path) for display in the UI.
- **scrape_log.txt / scrape_*.log**: Human-readable scrape logs stored under `/app/data/logs`, tailed by the UI for live updates.

## SQLite usage (logging by default)
- **CSV sync**: Each run syncs `judgments.csv` via `csv_sync.sync_csv`, recording a `csv_versions` row and upserting `cases`. `first_seen_version_id` and `last_seen_version_id` encode when each case first and last appears, while `is_active` marks removals within the feed.
- **Runs table**: `run_scrape` inserts a row into `runs` for each scrape attempt. The `trigger` column records the entrypoint (`"ui"` for web UI runs, `"webhook"` for ChangeDetection.io webhook runs, `"cli"` for direct programmatic invocations), while `mode` captures the effective scrape mode (`"full"`, `"new"`, or `"resume"`). Completion and failures are marked at the end of the run, and coverage columns (`cases_total`, `cases_planned`, `cases_attempted`, `cases_downloaded`, `cases_failed`, `cases_skipped`, `coverage_ratio`) plus `run_health` are populated from the `cases`/`downloads` tables once a run finishes. `db_reporting.get_latest_run_id` and `get_run_summary` provide read-only access for reporting APIs, and the returned summaries now bubble `run_id`/`csv_version_id` back to callers (e.g., the webhook response body).
- **Run list (DB-backed)**: `db_reporting.list_recent_runs(limit)` reads from the `runs` table and returns the most recent rows ordered by `started_at` DESC. `GET /api/db/runs` exposes this as JSON with `{ok, count, runs}`, where each run entry includes `id`, `trigger`, `mode`, `csv_version_id`, `status`, `started_at`, `ended_at`, `error_summary`, coverage counts, `coverage_ratio`, and `run_health`. `GET /api/db/runs/<run_id>/health` returns the coverage/health payload for a single run (404 when the run is unknown). An optional `?limit=` query parameter controls how many rows are returned (bounded server-side).
- **Downloads table**: Per-case attempts are logged during scraping with statuses (`pending`, `in_progress`, `downloaded`, `skipped`, `failed`), attempt counts, timestamps, optional file info, and error details. `/api/db/downloaded-cases` reads from this table to offer DataTables-compatible payloads without touching the legacy JSON files, `/api/db/runs/<run_id>/downloaded-cases` exposes the successful rows for a specific run (powered by `db_reporting.get_downloaded_cases_for_run`), and `/report` plus `/api/downloaded-cases` can switch to this data (still filtered to downloaded cases) when `BAILIIKC_USE_DB_REPORTING=1`.
- **Downloaded cases per run (DB-backed)**: `db_reporting.get_downloaded_cases_for_run(run_id)` joins `downloads` and `cases` to return the successful rows for the given `run_id` as dictionaries. `GET /api/db/runs/<run_id>/downloaded-cases` returns `{ok: true, run_id, count, downloads}` (with `<run_id>` as the path parameter) and responds with 404 when the run does not exist.
- **CSV version case diff (DB-backed)**: `db_reporting.get_case_diff_for_csv_version(version_id)` derives which cases are new at a version (`first_seen_version_id == version_id`) and which were removed at that version (`last_seen_version_id == version_id` and `is_active = 0`) for `source = 'unreported_judgments'`. `GET /api/db/csv_versions/<version_id>/case-diff` returns `{ok: true, csv_version_id, new_count, removed_count, new_cases, removed_cases}` (with `<version_id>` as the path parameter) and responds with 404 when the version does not exist or is invalid.
- **Case index backend**: With `BAILIIKC_USE_DB_CASES=1` (default), `cases_index` builds the in-memory index from the SQLite `cases` table that mirrors the CSV feed. Setting the flag to `"0"` forces the legacy CSV-driven index. Behaviour should be identical in both modes; the DB path is now the primary backend.
- **Download executor knobs**: `BAILIIKC_MAX_PARALLEL_DOWNLOADS` (default `1`) and `BAILIIKC_MAX_PENDING_DOWNLOADS` (default `100`) bound how many Box downloads may be in-flight. `BAILIIKC_ENABLE_DOWNLOAD_EXECUTOR=0` forces inline execution regardless of other limits. Peak in-flight totals are logged via `[SCRAPER][STATE]` lines with `phase=download_executor` for observability. `validate_runtime_config` clamps invalid executor counts to at least 1 when the executor is enabled.

## Health & monitoring

- **CLI healthcheck**: `python -m app.scraper.healthcheck` runs configuration validation (using the `entrypoint` passed in), ensures data/log/PDF directories exist with sufficient free space, confirms the SQLite schema is reachable, and emits a `[SCRAPER][STATE|ERROR] phase=health` event summarising results. Exit code is `0` when all mandatory checks pass and `1` otherwise; the optional JSON-vs-DB consistency check is treated as a warning in non-CLI entrypoints.
- **HTTP health endpoint**: `GET /api/health` wraps `run_health_checks(entrypoint="ui")` and returns `{ok, checks}` with HTTP `200` when healthy and `503` when any mandatory check fails. Checks include `config`, `filesystem` (paths and `min_free_mb`), `database` connectivity/schema, and an optional `consistency` diagnostic payload.
- **Replay controls**: `BAILIIKC_RECORD_REPLAY_FIXTURES=1` captures `dl_bfile` JSONL fixtures during live runs. `BAILIIKC_REPLAY_SKIP_NETWORK=1` stubs Box downloads for offline replay; the replay harness also forces this in dry-run mode while writing to sandboxed output roots under `/app/data/replay_runs/`. **Note:** Setting `BAILIIKC_REPLAY_SKIP_NETWORK=1` during a live scrape writes stub PDFs containing only a `%PDF-1.4` header and should be reserved for offline replay/testing. `validate_runtime_config` rejects live UI/CLI/webhook runs when this flag is set.
- **BAILIIKC_USE_DB_WORKLIST_FOR_NEW**: when set to `"1"` (default), `scrape_mode="new"` uses the DB-backed worklist from `app.scraper.worklist.build_new_worklist(csv_version_id, source)` derived from the SQLite `cases` table. Setting the flag to `"0"` forces the legacy CSV-driven planner.
- **BAILIIKC_USE_DB_WORKLIST_FOR_FULL**: when set to `"1"` (default), `scrape_mode="full"` uses the DB-backed worklist from `build_full_worklist(...)`. Setting the flag to `"0"` keeps the legacy CSV-only path.
- **BAILIIKC_USE_DB_WORKLIST_FOR_RESUME**: when set to `"1"` (default), resume mode draws planned retries from `build_resume_worklist(...)` inside `run.py` and filters pagination clicks accordingly. Setting the flag to `"0"` preserves legacy JSON/log-driven resume behaviour.
- **DB worklist helpers**: `app.scraper.worklist.build_full_worklist(csv_version_id, source)`, `build_new_worklist(...)`, and `build_resume_worklist(...)` derive the set of cases to process for a given CSV version from the SQLite `cases` and `downloads` tables. A dispatcher `build_worklist(mode, csv_version_id, source)` chooses the appropriate helper for `"full"`, `"new"`, or `"resume"`. When DB worklist flags are enabled, `run.py` uses these helpers for planning, including `"resume"` when explicitly requested; setting flags to `"0"` preserves the legacy CSV-driven planner.
- **Behaviour**: JSON files remain the source of truth for scraper control/resume. SQLite is read for reporting (/api/runs/latest, /api/db/ endpoints, and optional DB-backed /report or /api/downloaded-cases) while continuing to mirror run and download activity during a scrape.
- **Consistency checker (JSON vs DB)**: `app.scraper.consistency.compare_latest_downloads_json_vs_db()` computes a diagnostic report comparing the JSON-based and DB-based views of downloaded cases for the latest run. This is exposed as a CLI via `python -m app.scraper.consistency` and is intended for internal validation while DB-backed reporting and control flow are adopted. Any errors or mismatches cause the CLI to exit non-zero.

### Offline replay usage

For local debugging and regression testing, the scraper can replay captured `dl_bfile` fixtures without talking to Playwright or the live judicial site.

1. Enable fixture capture for a real run:

   - Set `BAILIIKC_RECORD_REPLAY_FIXTURES=1`.
   - Run the scraper as normal (via CLI or UI).
   - This writes one JSONL file per run under `/app/data/replay_fixtures/run_<run_id>_dl_bfile.jsonl`.

2. Run the offline harness against those fixtures:

   ```bash
   python -m app.scraper.replay_harness /app/data/replay_fixtures/run_<run_id>_dl_bfile.jsonl --dry-run
   ```

   `--dry-run` forces `BAILIIKC_REPLAY_SKIP_NETWORK=1` and writes stub PDFs under `/app/data/replay_runs/replay_<timestamp>/dry_run_pdfs`.

   Omitting `--dry-run` replays the downloads against Box using `requests.Session` while keeping all output sandboxed under `/app/data/replay_runs/...`.

The harness never mutates the main `/app/data/pdfs` directory or JSON logs; it temporarily overrides `config.PDF_DIR` and `config.DOWNLOADS_LOG` and restores them at the end of the replay.

## Webhook API (ChangeDetection.io)

- **Endpoint**: `POST /webhook/changedetection`
- **Auth**: requires `BAILIIKC_WEBHOOK_SHARED_SECRET`; tokens are accepted via `X-Webhook-Token` header or `token` query parameter. When the secret is unset the route is disabled with `{ok: false, error: "webhook_disabled"}`.
- **Payload**: supports JSON, form, or query parameters. Requires `mode="new"` and `target_source="unreported_judgments"`. `new_limit` defaults to `min(SCRAPE_NEW_LIMIT, WEBHOOK_NEW_LIMIT_MAX)` and is clamped to `WEBHOOK_NEW_LIMIT_MAX` (default 50) with a `[SCRAPER][STATE] phase=webhook kind=limit_clamped` log when clamped.
- **Behaviour**: runs a synchronous `run_scrape` with `trigger="webhook"`, `resume_mode="none"`, and `limit_pages=[0]` to keep runs short. Responses include `{ok, entrypoint:"webhook", run_id, csv_version_id, summary:{processed, downloaded, skipped, failed}}`; invalid params yield 400 `invalid_params`, and config validation failures yield `config_invalid`.

## Scrape logs
- Structured scraper logs follow a `[SCRAPER][PHASE] key=value, ...` pattern so they can be grepped or machine-parsed.
- Phases include `NAV` (run setup), `PLAN` (CSV sync, case index selection, and worklist planning), `TABLE` (pagination, row counts, limits), `DECISION` (per-token skip/download choices), `BOX` (Box download results), `STATE` (checkpoint/resume decisions), and `ERROR` (unexpected run-level failures).
- For quick diagnosis, grep `[SCRAPER][ERROR]` for fatal issues and `[SCRAPER][BOX]` plus `[SCRAPER][DECISION]` to see why specific tokens were downloaded or skipped.

## Download state machine and DB-backed attempt logging

- **Downloads table semantics**
  - The `downloads` table tracks per-run, per-case attempt state. Each row is identified by `(run_id, case_id)` and carries:
    - `status` – one of `pending`, `in_progress`, `downloaded`, `skipped`, `failed`.
    - `attempt_count` – number of attempts made for this case in this run.
    - `last_attempt_at` – ISO8601 UTC timestamp of the most recent attempt.
    - `file_path` / `file_size_bytes` – last known file path and size when a download succeeds (or when earlier logic recorded these).
    - `box_url_last` – last Box URL used for this case.
    - `error_code` / `error_message` – a coarse error category and message for failures or permanent skips.
  - Rows are created lazily via `db.ensure_download_row(run_id, case_id)` whenever the scraper first needs to log activity for a case in a given run.

- **CaseDownloadState helper**
  - `app.scraper.download_state.CaseDownloadState` is a small state machine wrapper around the `downloads` row:
    - `CaseDownloadState.load(run_id, case_id)`:
      - Ensures a `downloads` row exists (if `case_id` is not `None`) and returns a `CaseDownloadState` with the current `status`, `attempt_count`, and `download_id`.
      - For unknown or malformed `status` values in the DB, it falls back to `pending` rather than raising.
    - `CaseDownloadState.start(run_id, case_id, box_url)`:
      - Ensures a row exists and, if allowed, transitions the case to `in_progress` for this run:
        - Increments `attempt_count`.
        - Sets `status="in_progress"`, updates `last_attempt_at`, and records `box_url_last`.
        - Emits `[SCRAPER][STATE]` with `from_status`, `to_status`, `attempt`, `run_id`, `case_id`, `download_id`, and `box_url`.
      - If the current status is already `downloaded`, the transition is rejected, a `[SCRAPER][ERROR] invalid_transition_after_download` event is logged, and the DB row is left unchanged.
    - `CaseDownloadState.mark_downloaded(file_path, file_size_bytes, box_url)`:
      - Moves the case to `downloaded` for this run, updating `status`, `last_attempt_at`, `file_path`, `file_size_bytes`, and `box_url_last`.
      - Emits a `[SCRAPER][STATE]` event describing the transition and file metadata.
    - `CaseDownloadState.mark_skipped(reason)`:
      - Marks the case as `skipped` for this run with a permanent reason, updating `status`, `last_attempt_at`, and `error_code=reason`.
      - Used both for pre-click skip decisions (e.g. worklist filters, CSV misses, already-downloaded cases) and for “pseudo-success” results where no new download occurs.
      - Emits a `[SCRAPER][STATE]` event including `reason` and current attempt metadata.
    - `CaseDownloadState.mark_failed(error_code, error_message)`:
      - Marks the case as `failed` for this run, updating `status`, `last_attempt_at`, `error_code`, and `error_message` (while leaving `file_path`/`file_size_bytes` untouched).
      - Emits a `[SCRAPER][STATE]` event with error details.

- **Mapping scraper results to download states**
  - The Playwright AJAX handler in `run.py` integrates `CaseDownloadState` as follows for each `dl_bfile` response when a `case_id` is known:
    - Before calling `handle_dl_bfile_from_ajax`, it calls `CaseDownloadState.start(...)` to mark the attempt as `in_progress`.
    - It then calls `handle_dl_bfile_from_ajax(...)`, which returns `(result, download_info)` where `result` is one of:
      - `"downloaded"` – a new file has been saved.
      - `"existing_file"` – a local PDF already exists or metadata/local file combination confirms a prior download.
      - `"checkpoint_skip"` – NEW-mode checkpoint indicates this token was processed in a prior run.
      - `"duplicate_in_run"` – in-run dedupe for repeated AJAX responses.
      - `"failed"` – the download attempt failed (e.g. network, HTTP, filesystem).
      - `"disk_full"` – disk-space guard triggered before or during download.
    - The `result` is mapped to the state machine:
      - `"downloaded"` → `state.mark_downloaded(file_path, file_size_bytes, box_url_last)`.
      - `"existing_file"`, `"checkpoint_skip"`, `"duplicate_in_run"` → `state.mark_skipped(reason=result)`.
      - `"disk_full"` → `state.mark_failed(error_code="disk_full", error_message=download_info.error_message)`.
      - `"failed"` → `state.mark_failed(error_code="download_other", error_message=download_info.error_message)`.
  - Skip decisions made earlier in the control flow (e.g. invalid token, worklist filtering, CSV miss, already downloaded in metadata, seen in checkpoint) call:
    - `_log_skip_status(case_id, reason)` → `CaseDownloadState.load(...).mark_skipped(reason)`; this ensures each skip is mirrored in the `downloads` table with `status="skipped"` and `error_code=reason`.

- **Invariants**
  - Each `(run_id, case_id)` has at most one logical state at any time, derived from the `downloads.status` value.
  - `downloaded` is terminal for that run:
    - Any later attempt to move the case to a non-`downloaded` status is rejected by `CaseDownloadState`, which logs a `[SCRAPER][ERROR] invalid_transition_after_download` and leaves the DB row unchanged.
  - Scraper code never updates `downloads.status` directly; all write paths go through `CaseDownloadState` or `db.ensure_download_row` (for row creation only).
  - All state transitions are echoed into structured logs via `[SCRAPER][STATE]` so that an operator can answer “what happened to case X in run Y?” by reading the DB and/or log stream.

## Known Limitations / Fragility
- The judgments CSV is still fetched from the remote URL on each run. While `csv_sync.sync_csv` now records `csv_versions` rows and local CSV files for versioning, there is no offline fallback when the source is unavailable, so network hiccups can still prevent a run from starting.
- Resume relies on JSON checkpoints and log parsing, leading to complexity when recovering mid-run.
- Disk space guardrails are basic (free-space checks via `utils.disk_has_room`), and filename length issues require fallbacks.
- The Playwright AJAX capture flow is delicate; nonce/session handling and Box URL extraction were tuned through trial and error and should remain untouched for now.

### Error codes, retries, and validation

- `downloads.error_code` values follow the internal taxonomy in `app.scraper.error_codes.ErrorCode` and are logged alongside failures for traceability.
- `CaseDownloadState.mark_failed` records `error_code`/`error_message` to keep DB rows and structured logs aligned.
- Box downloads validate `%PDF` headers and a minimum size before writing; replay stubs are padded to satisfy the same check.
- Retry behaviour is driven by `retry_policy.decide_retry`, using `error_code`/HTTP status to decide and `compute_backoff_seconds` for bounded exponential delays.
