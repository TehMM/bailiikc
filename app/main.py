from __future__ import annotations

import csv
import io
import os
import re
import threading
import time
from datetime import datetime
from typing import Generator, Iterable

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

from app.scraper import config
from app.scraper.run import run_scrape
from app.scraper.utils import (
    build_zip,
    ensure_dirs,
    get_current_log_path,
    load_base_url,
    load_metadata,
    log_line,
    reset_state,
    save_base_url,
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")

ensure_dirs()


_DATE_FORMATS: Iterable[str] = ("%Y-%m-%d", "%Y-%b-%d", "%d/%m/%Y", "%d-%b-%Y")


def _sortable_date(value: str) -> str:
    """Return an ISO-like string suitable for sorting judgement dates."""

    candidate = (value or "").strip()
    if not candidate:
        return ""

    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(candidate, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    digits = re.sub(r"[^0-9]", "", candidate)
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return ""


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


def _build_download_rows(meta: dict) -> list[dict[str, str]]:
    """Build structured rows for the downloads table."""

    rows: list[dict[str, str]] = []
    for entry in meta.get("downloads", []):
        if not isinstance(entry, dict):
            continue
        filename = entry.get("local_filename") or entry.get("filename")
        if not filename:
            continue

        judgment_date = entry.get("judgment_date") or ""
        rows.append(
            {
                "slug": entry.get("slug") or entry.get("fid") or filename,
                "fid": entry.get("fid") or "",
                "title": entry.get("title") or entry.get("subject") or filename,
                "subject": entry.get("subject") or entry.get("title") or "",
                "court": entry.get("court") or "",
                "category": entry.get("category") or "",
                "judgment_date": judgment_date,
                "sort_judgment_date": _sortable_date(judgment_date),
                "cause_number": entry.get("cause_number") or "",
                "downloaded_at": entry.get("downloaded_at") or "",
                "local_filename": filename,
                "filesize": entry.get("filesize") or 0,
            }
        )
    return rows


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

    save_base_url(base_url)
    app.config["LAST_PARAMS"] = {
        "base_url": base_url,
        "page_wait": page_wait,
        "per_download_delay": per_delay,
        "scrape_mode": scrape_mode,
        "new_limit": new_limit,
        "max_retries": max_retries,
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
                )
                app.config["LAST_SUMMARY"] = summary
                app.config["CURRENT_LOG_FILE"] = summary.get("log_file")
            except Exception as exc:  # noqa: BLE001
                log_line(f"Scrape thread failed: {exc}")

    threading.Thread(target=_run, daemon=True).start()
    flash("Scrape started! Check the Report page in a bit.", "info")
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
    meta = load_metadata()
    downloads = _build_download_rows(meta)
    courts = sorted({row["court"] for row in downloads if row["court"]})
    categories = sorted({row["category"] for row in downloads if row["category"]})

    current_log_path = get_current_log_path()
    context = {
        "base_url": load_base_url(),
        "csv_url": config.CSV_URL,
        "log_lines": _read_last_log_lines(),
        "last_summary": app.config.get("LAST_SUMMARY"),
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
    ensure_dirs()
    app.run(host="0.0.0.0", port=8080)
