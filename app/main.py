from __future__ import annotations

import csv
import io
import os
import threading
import time
from typing import Any, Dict, Generator

from flask import (
    Flask,
    Response,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    send_from_directory,
    url_for,
)

from app.scraper import config, db, db_reporting, sources
from app.scraper.download_rows import build_download_rows, load_download_records
from app.scraper.run import run_scrape
from app.scraper.config_validation import validate_runtime_config
from app.scraper.healthcheck import run_health_checks
from app.scraper.export_excel import export_latest_run_to_excel
from app.scraper.utils import (
    build_zip,
    ensure_dirs,
    get_current_log_path,
    load_base_url,
    load_json_file,
    load_json_lines,
    load_metadata,
    log_line,
    reset_state,
    save_base_url,
)
from app.scraper.logging_utils import _scraper_event

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")

# Initialise storage paths and SQLite schema on import so WSGI/ASGI entrypoints
# also have the expected environment ready. Idempotent by design.
ensure_dirs()
db.initialize_schema()

WEBHOOK_LIMIT_MAX = max(1, config.WEBHOOK_NEW_LIMIT_MAX)
WEBHOOK_DEFAULT_NEW_LIMIT = min(config.SCRAPE_NEW_LIMIT, WEBHOOK_LIMIT_MAX)


def use_db_reporting() -> bool:
    """Return True when DB-backed reporting endpoints should be used."""

    return config.use_db_reporting()


def _read_last_log_lines(limit: int = 150) -> list[str]:
    """Return the trailing ``limit`` log lines for initial display."""

    ensure_dirs()
    path = get_current_log_path()
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        lines = handle.readlines()[-limit:]
    return [line.rstrip("\n") for line in lines]


def _tail_log_generator() -> Generator[str, None, None]:
    """Yield Server-Sent Event messages for appended log lines."""

    ensure_dirs()
    current_path = get_current_log_path()
    current_path.parent.mkdir(parents=True, exist_ok=True)
    current_path.touch(exist_ok=True)

    handle = current_path.open("r", encoding="utf-8", errors="ignore")
    handle.seek(0, os.SEEK_END)

    try:
        while True:
            latest_path = get_current_log_path()
            if latest_path != current_path:
                handle.close()
                current_path = latest_path
                current_path.parent.mkdir(parents=True, exist_ok=True)
                current_path.touch(exist_ok=True)
                handle = current_path.open("r", encoding="utf-8", errors="ignore")
                handle.seek(0, os.SEEK_END)

            line = handle.readline()
            if line:
                yield f"data: {line.rstrip()}\n\n"
            else:
                time.sleep(1)
                yield ": heartbeat\n\n"
    finally:
        handle.close()


def _metadata_to_csv(meta: dict) -> str:
    """Serialise the metadata dictionary to a CSV string."""

    output = io.StringIO()
    fieldnames = [
        "fid",
        "slug",
        "title",
        "subject",
        "category",
        "court",
        "cause_number",
        "judgment_date",
        "downloaded_at",
        "local_filename",
        "source_url",
        "filesize",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for entry in meta.get("downloads", []):
        if not isinstance(entry, dict):
            continue
        writer.writerow({key: entry.get(key, "") for key in fieldnames})
    return output.getvalue()


def _load_download_records() -> list[dict[str, object]]:
    """Return download records sourced from ``downloads.jsonl``."""

    return load_download_records()


def _get_download_rows_for_ui() -> list[dict[str, object]]:
    """Return download rows for the UI (report + JSON API)."""

    if use_db_reporting():
        rows_from_db = db_reporting.get_download_rows_for_run(
            status_filter="downloaded"
        )
        return [dict(row) for row in rows_from_db]

    records = _load_download_records()
    return build_download_rows(records)


def _get_webhook_token() -> str | None:
    token = request.headers.get("X-Webhook-Token")
    if not token:
        token = request.args.get("token")
    return token


def _parse_webhook_payload() -> dict[str, object]:
    payload: dict[str, object] = {}
    payload.update(request.args or {})

    if request.is_json:
        payload.update(request.get_json(silent=True) or {})
    else:
        payload.update(request.form or {})

    return payload


@app.route("/debug/unreported_judgments.png")
def debug_unreported_png() -> Response:
    return send_from_directory(
        config.PDF_DIR,
        "unreported_judgments.png",
        mimetype="image/png",
    )


@app.context_processor
def inject_globals() -> dict[str, object]:
    """Inject global configuration into all template contexts."""

    return {"config": config}


@app.route("/")
def index() -> str:
    """Render the home page with scrape configuration controls."""

    ensure_dirs()
    last_summary = app.config.get("LAST_SUMMARY")
    last_params = app.config.get("LAST_PARAMS", {})
    context = {
        "default_base_url": load_base_url(),
        "default_wait": last_params.get("page_wait", config.PAGE_WAIT_SECONDS),
        "default_new_limit": last_params.get("new_limit", config.SCRAPE_NEW_LIMIT),
        "default_delay": last_params.get(
            "per_download_delay", config.PER_DOWNLOAD_DELAY
        ),
        "default_mode": last_params.get(
            "scrape_mode", config.SCRAPE_MODE_DEFAULT
        ),
        "default_resume_mode": last_params.get("resume_mode", "none"),
        "default_resume_page": last_params.get("resume_page"),
        "default_resume_index": last_params.get("resume_index"),
        "default_reset_before_run": last_params.get("reset_before_run", False),
        "default_reset_delete_pdfs": last_params.get("reset_delete_pdfs", False),
        "last_summary": last_summary,
    }
    return render_template("index.html", **context)


@app.post("/scrape")
def start_scrape() -> Response:
    """Handle the scrape form submission and trigger a scraping run."""

    base_url = request.form.get("base_url", config.DEFAULT_BASE_URL).strip()
    page_wait = int(request.form.get("page_wait", config.PAGE_WAIT_SECONDS))
    per_delay = float(request.form.get("per_download_delay", config.PER_DOWNLOAD_DELAY))
    scrape_mode = (
        request.form.get("scrape_mode", config.SCRAPE_MODE_DEFAULT).strip().lower()
        or config.SCRAPE_MODE_DEFAULT
    )
    resume_mode = request.form.get("resume_mode", "none").strip().lower()
    try:
        resume_page = int(request.form.get("resume_page"))
    except (TypeError, ValueError):
        resume_page = None
    try:
        resume_index = int(request.form.get("resume_index"))
    except (TypeError, ValueError):
        resume_index = None
    reset_before_run = request.form.get("reset_before_run") == "1"
    delete_pdfs_during_reset = request.form.get("reset_delete_pdfs") == "1"
    try:
        new_limit = int(request.form.get("new_limit", config.SCRAPE_NEW_LIMIT))
    except (TypeError, ValueError):
        new_limit = config.SCRAPE_NEW_LIMIT
    new_limit = max(0, new_limit)
    try:
        max_retries = int(request.form.get("max_retries", config.SCRAPER_MAX_RETRIES))
    except (TypeError, ValueError):
        max_retries = config.SCRAPER_MAX_RETRIES
    max_retries = max(1, max_retries)

    try:
        validate_runtime_config("ui", mode=scrape_mode)
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("index")), 400

    if reset_before_run:
        reset_state(delete_pdfs=delete_pdfs_during_reset, delete_logs=False)
        scrape_mode = "full"

    save_base_url(base_url)
    app.config["LAST_PARAMS"] = {
        "base_url": base_url,
        "page_wait": page_wait,
        "per_download_delay": per_delay,
        "scrape_mode": scrape_mode,
        "new_limit": new_limit,
        "max_retries": max_retries,
        "reset_before_run": reset_before_run,
        "reset_delete_pdfs": delete_pdfs_during_reset,
        "resume_mode": resume_mode,
        "resume_page": resume_page,
        "resume_index": resume_index,
    }

    def _run() -> None:
        with app.app_context():
            try:
                summary = run_scrape(
                    base_url=base_url,
                    page_wait=page_wait,
                    per_delay=per_delay,
                    start_message="Initiating scrape via web UI",
                    scrape_mode=scrape_mode,
                    new_limit=new_limit,
                    max_retries=max_retries,
                    resume=config.SCRAPE_RESUME_DEFAULT,
                    resume_mode=resume_mode,
                    resume_page=resume_page,
                    resume_index=resume_index,
                    target_source=config.DEFAULT_SOURCE,
                    trigger="ui",
                )
                app.config["LAST_SUMMARY"] = summary
                app.config["CURRENT_LOG_FILE"] = summary.get("log_file")
            except Exception as exc:  # noqa: BLE001
                log_line(f"Scrape thread failed: {exc}")

    threading.Thread(target=_run, daemon=True).start()
    flash("Scrape started! Check the Report page in a bit.", "info")
    return redirect(url_for("report"))


@app.post("/resume")
def resume_scrape() -> Response:
    """Trigger a resume-aware scrape using stored checkpoints or logs."""

    resume_mode = request.form.get("resume_mode", "auto").strip().lower() or "auto"
    try:
        resume_page = int(request.form.get("resume_page"))
    except (TypeError, ValueError):
        resume_page = None
    try:
        resume_index = int(request.form.get("resume_index"))
    except (TypeError, ValueError):
        resume_index = None

    base_url = request.form.get("base_url", config.DEFAULT_BASE_URL).strip()
    page_wait = int(request.form.get("page_wait", config.PAGE_WAIT_SECONDS))
    per_delay = float(request.form.get("per_download_delay", config.PER_DOWNLOAD_DELAY))

    try:
        validate_runtime_config("ui", mode="resume")
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("index")), 400

    def _run() -> None:
        with app.app_context():
            try:
                summary = run_scrape(
                    base_url=base_url,
                    page_wait=page_wait,
                    per_delay=per_delay,
                    start_message="Resume triggered via web UI",
                    scrape_mode=config.SCRAPE_MODE_DEFAULT,
                    new_limit=config.SCRAPE_NEW_LIMIT,
                    max_retries=config.SCRAPER_MAX_RETRIES,
                    resume=True,
                    resume_mode=resume_mode,
                    resume_page=resume_page,
                    resume_index=resume_index,
                    target_source=config.DEFAULT_SOURCE,
                    trigger="ui",
                )
                app.config["LAST_SUMMARY"] = summary
                app.config["CURRENT_LOG_FILE"] = summary.get("log_file")
            except Exception as exc:  # noqa: BLE001
                log_line(f"Resume thread failed: {exc}")

    threading.Thread(target=_run, daemon=True).start()
    flash("Resume run started!", "info")
    return redirect(url_for("report"))


@app.post("/reset")
def reset_view() -> Response:
    """Reset metadata and optionally clear downloads/logs."""

    delete_pdfs = request.form.get("delete_pdfs") == "1"
    delete_logs = request.form.get("delete_logs") == "1"
    reset_state(delete_pdfs=delete_pdfs, delete_logs=delete_logs)
    app.config["LAST_SUMMARY"] = None
    app.config["CURRENT_LOG_FILE"] = str(get_current_log_path())
    flash("Download state reset.", "info")
    return redirect(url_for("report"))


@app.get("/report")
def report() -> str:
    """Render the report page with current metadata and live logs."""

    ensure_dirs()
    downloads = _get_download_rows_for_ui()
    courts = sorted({row["court"] for row in downloads if row["court"]})
    categories = sorted({row["category"] for row in downloads if row["category"]})

    current_log_path = get_current_log_path()
    summary = load_json_file(config.SUMMARY_FILE)
    context = {
        "base_url": load_base_url(),
        "csv_url": config.CSV_URL,
        "log_lines": _read_last_log_lines(),
        "last_summary": summary or app.config.get("LAST_SUMMARY"),
        "last_params": app.config.get("LAST_PARAMS", {}),
        "downloads": downloads,
        "courts": courts,
        "categories": categories,
        "current_log_name": current_log_path.name,
    }
    return render_template("report.html", **context)


@app.get("/logs/stream")
def logs_stream() -> Response:
    """Stream log updates to the browser using SSE."""

    response = Response(_tail_log_generator(), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"
    return response


@app.get("/logs/<path:filename>")
def download_log(filename: str) -> Response:
    """Serve a log file from the logs directory."""

    target = (config.LOG_DIR / filename).resolve()
    root = config.LOG_DIR.resolve()
    if not str(target).startswith(str(root)):
        return Response("Invalid path", status=400)
    if not target.exists() or not target.is_file():
        return Response("File not found", status=404)
    return send_file(target, as_attachment=True, download_name=target.name)


@app.get("/files/<path:filename>")
def download_file(filename: str) -> Response:
    """Serve an individual PDF if it exists within the data directory."""

    target = (config.PDF_DIR / filename).resolve()
    pdf_root = config.PDF_DIR.resolve()
    if not str(target).startswith(str(pdf_root)):
        return Response("Invalid path", status=400)
    if not target.exists() or not target.is_file():
        return Response("File not found", status=404)
    return send_file(target, as_attachment=True, download_name=target.name)


@app.get("/download/all.zip")
def download_all_zip() -> Response:
    """Create (or refresh) an archive containing all downloaded PDFs."""

    archive = build_zip()
    return send_file(archive, as_attachment=True, download_name=config.ZIP_NAME)
@app.post("/webhook/changedetection")
def webhook_changedetection() -> Response:
    if not config.WEBHOOK_SHARED_SECRET:
        return jsonify({"ok": False, "error": "webhook_disabled"}), 404

    token = _get_webhook_token()
    if token != config.WEBHOOK_SHARED_SECRET:
        _scraper_event(
            "error",
            phase="webhook",
            context="changedetection",
            error="invalid_token",
            remote_addr=request.remote_addr,
        )
        return jsonify({"ok": False, "error": "invalid_token"}), 403

    payload = _parse_webhook_payload()
    raw_target_source = str(payload.get("target_source") or "").strip()
    target_source = sources.normalize_source(raw_target_source)
    mode = str(payload.get("mode") or "").strip().lower()
    new_limit_raw = payload.get("new_limit")

    errors = []
    if target_source != sources.UNREPORTED_JUDGMENTS:
        errors.append("target_source must be 'unreported_judgments'")
    if mode != "new":
        errors.append("mode must be 'new'")

    new_limit: int
    if new_limit_raw is None or new_limit_raw == "":
        new_limit = WEBHOOK_DEFAULT_NEW_LIMIT
    else:
        try:
            new_limit = int(new_limit_raw)
        except (TypeError, ValueError):
            errors.append("new_limit must be an integer")
            new_limit = WEBHOOK_DEFAULT_NEW_LIMIT

    if errors:
        _scraper_event(
            "error",
            phase="webhook",
            context="changedetection",
            error="invalid_params",
            details=errors,
            remote_addr=request.remote_addr,
        )
        return jsonify({"ok": False, "error": "invalid_params", "details": errors}), 400

    if new_limit < 1:
        new_limit = 1
    if new_limit > WEBHOOK_LIMIT_MAX:
        _scraper_event(
            "state",
            phase="webhook",
            context="changedetection",
            kind="limit_clamped",
            requested=new_limit_raw,
            max_limit=WEBHOOK_LIMIT_MAX,
        )
        new_limit = WEBHOOK_LIMIT_MAX

    try:
        validate_runtime_config("webhook", mode="new")
    except ValueError as exc:  # noqa: BLE001
        _scraper_event(
            "error",
            phase="webhook",
            context="changedetection",
            error="config_invalid",
            message=str(exc),
        )
        return jsonify({"ok": False, "error": "config_invalid", "details": str(exc)}), 500

    _scraper_event(
        "state",
        phase="webhook",
        context="changedetection",
        mode="new",
        new_limit=int(new_limit),
        remote_addr=request.remote_addr,
    )

    try:
        summary = run_scrape(
            base_url=config.DEFAULT_BASE_URL,
            page_wait=config.PAGE_WAIT_SECONDS,
            per_delay=config.PER_DOWNLOAD_DELAY,
            scrape_mode="new",
            new_limit=new_limit,
            max_retries=config.SCRAPER_MAX_RETRIES,
            resume=False,
            resume_mode="none",
            resume_page=None,
            resume_index=None,
            limit_pages=[0],
            row_limit=new_limit,
            start_message="[WEBHOOK] Triggered new-only scrape",
            target_source=target_source,
            trigger="webhook",
        )
    except Exception as exc:  # noqa: BLE001
        _scraper_event(
            "error",
            phase="webhook",
            context="changedetection",
            error="scrape_failed",
            message=str(exc),
        )
        return (
            jsonify({"ok": False, "error": "scrape_error", "error_summary": str(exc)}),
            500,
        )

    summary_counts = {
        "processed": summary.get("processed"),
        "downloaded": summary.get("downloaded"),
        "skipped": summary.get("skipped"),
        "failed": summary.get("failed"),
    }

    return jsonify(
        {
            "ok": True,
            "entrypoint": "webhook",
            "mode": "new",
            "target_source": target_source,
            "run_id": summary.get("run_id"),
            "csv_version_id": summary.get("csv_version_id"),
            "summary": summary_counts,
        }
    )


@app.get("/api/downloaded-cases")
def api_downloaded_cases() -> Response:
    """Return the downloaded cases in a JSON payload suitable for DataTables."""

    rows = _get_download_rows_for_ui()
    return jsonify({"data": rows})


@app.get("/api/db/runs")
def api_db_runs_list() -> Response:
    """Return recent runs from SQLite as a JSON array."""

    raw_limit = request.args.get("limit", type=int)
    if raw_limit is None:
        limit = 20
    else:
        limit = max(1, min(raw_limit, 200))

    runs = db_reporting.list_recent_runs(limit)

    return jsonify({"ok": True, "count": len(runs), "runs": runs})


@app.get("/api/db/runs/<int:run_id>/health")
def api_db_run_health(run_id: int) -> Response:
    """Return coverage and health classification for a run."""

    try:
        coverage = db_reporting.get_run_coverage(run_id)
    except db_reporting.RunNotFoundError:
        return jsonify({"ok": False, "error": "run_not_found", "run_id": run_id}), 404

    payload = {"ok": True, **coverage}
    return jsonify(payload)


@app.get("/api/db/runs/latest")
def api_db_runs_latest() -> Response:
    """Return the latest run summary backed by SQLite."""

    run_id = db_reporting.get_latest_run_id()
    if run_id is None:
        return jsonify({"ok": False, "error": "no runs"}), 404

    summary = db_reporting.get_run_summary(run_id)
    if not summary:
        return jsonify({"ok": False, "error": "no runs"}), 404

    return jsonify({"ok": True, "run": summary})


@app.get("/api/db/runs/<int:run_id>/download-summary")
def api_db_run_download_summary(run_id: int) -> Response:
    """Return aggregated download outcomes and error codes for a run."""

    if not use_db_reporting():
        return jsonify({"ok": False, "error": "db_reporting_disabled"}), 404

    try:
        summary = db_reporting.summarise_downloads_for_run(run_id)
    except db_reporting.RunNotFoundError:
        return jsonify({"ok": False, "error": "run_not_found", "run_id": run_id}), 404

    return jsonify(
        {
            "ok": True,
            "run_id": summary.run_id,
            "status_counts": summary.status_counts,
            "fail_reasons": summary.fail_reasons,
            "skip_reasons": summary.skip_reasons,
        }
    )


@app.get("/api/db/runs/latest/download-summary")
def api_db_latest_run_download_summary() -> Response:
    """Return download summary for the most recent run."""

    if not use_db_reporting():
        return jsonify({"ok": False, "error": "db_reporting_disabled"}), 404

    run_id = db_reporting.latest_run_id()
    if run_id is None:
        return jsonify({"ok": False, "error": "no_runs"}), 404

    try:
        summary = db_reporting.summarise_downloads_for_run(run_id)
    except db_reporting.RunNotFoundError:
        return jsonify({"ok": False, "error": "run_not_found", "run_id": run_id}), 404

    return jsonify(
        {
            "ok": True,
            "run_id": summary.run_id,
            "status_counts": summary.status_counts,
            "fail_reasons": summary.fail_reasons,
            "skip_reasons": summary.skip_reasons,
        }
    )


@app.get("/api/db/downloaded-cases")
def api_db_downloaded_cases() -> Response:
    """Return downloaded cases using DB-backed reporting helpers."""

    run_id_param = request.args.get("run_id")
    status = request.args.get("status", "downloaded")

    run_id: int | None
    if run_id_param is None or run_id_param == "":
        run_id = None
    else:
        try:
            run_id = int(run_id_param)
        except ValueError:
            return jsonify({"ok": False, "error": "invalid run_id"}), 400

    rows = db_reporting.get_download_rows_for_run(run_id=run_id, status_filter=status)
    return jsonify({"data": rows})


@app.get("/api/db/runs/<int:run_id>/downloaded-cases")
def api_db_downloaded_cases_for_run(run_id: int) -> Response:
    """Return downloaded cases for the given run_id from SQLite."""

    try:
        rows = db_reporting.get_downloaded_cases_for_run(run_id)
    except db_reporting.RunNotFoundError:
        return jsonify({"ok": False, "error": "run_not_found", "run_id": run_id}), 404

    return jsonify({"ok": True, "run_id": run_id, "count": len(rows), "downloads": rows})


@app.get("/api/db/csv_versions/<int:version_id>/case-diff")
def api_db_case_diff_for_csv_version(version_id: int) -> Response:
    """Return new and removed cases for a given CSV version from SQLite."""

    try:
        diff = db_reporting.get_case_diff_for_csv_version(version_id)
    except db_reporting.CsvVersionNotFoundError:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "csv_version_not_found",
                    "csv_version_id": version_id,
                }
            ),
            404,
        )

    return jsonify(
        {
            "ok": True,
            "csv_version_id": diff["csv_version_id"],
            "new_count": diff["new_count"],
            "removed_count": diff["removed_count"],
            "new_cases": diff["new_cases"],
            "removed_cases": diff["removed_cases"],
        }
    )


@app.get("/api/health")
def api_health() -> Response:
    """Return a JSON health summary for configuration, filesystem, and DB."""

    result = run_health_checks(entrypoint="ui")
    status = 200 if result.ok else 503
    return jsonify({"ok": result.ok, "checks": result.checks}), status


@app.get("/api/exports/latest.xlsx")
def api_export_latest_xlsx() -> Response:
    path = export_latest_run_to_excel()
    return send_file(path, as_attachment=True, download_name=os.path.basename(path))


@app.get("/api/runs/latest")
def api_runs_latest() -> Response:
    """Return the latest run summary backed by SQLite aggregates."""

    run_id = db_reporting.get_latest_run_id()
    if run_id is None:
        return jsonify({"ok": False, "error": "no runs"}), 404

    summary = db_reporting.get_run_summary(run_id)
    if not summary:
        return jsonify({"ok": False, "error": "no runs"}), 404

    stats = db_reporting.get_run_download_stats(run_id)

    payload: Dict[str, Any] = {
        "ok": True,
        "run": {
            "id": summary["id"],
            "trigger": summary["trigger"],
            "mode": summary["mode"],
            "csv_version_id": summary["csv_version_id"],
            "status": summary["status"],
            "started_at": summary["started_at"],
            "ended_at": summary["ended_at"],
            "error_summary": summary["error_summary"],
            "downloads": stats,
        },
    }

    return jsonify(payload)


@app.get("/api/metadata")
def api_metadata() -> Response:
    """Return the metadata JSON payload for programmatic consumption."""

    meta = load_metadata()
    return jsonify(meta)


@app.get("/export/csv")
def export_csv() -> Response:
    """Provide the metadata as a downloadable CSV file."""

    meta = load_metadata()
    csv_body = _metadata_to_csv(meta)
    return Response(
        csv_body,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=metadata.csv"},
    )


if __name__ == "__main__":
    # Direct invocation is primarily for local development; directories and
    # schema are initialised above during module import.
    app.run(host="0.0.0.0", port=8080)
