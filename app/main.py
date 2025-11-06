import os
import threading

from flask import (
    Flask,
    render_template,
    request,
    send_from_directory,
    redirect,
    url_for,
    flash,
    jsonify,
)

from app.scraper.utils import (
    ensure_dirs,
    list_pdfs,
    build_zip,
    load_base_url,
    save_base_url,
    load_metadata,
)
from app.scraper.run import run_scrape

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")

ensure_dirs()

# app/main.py (add near top)
import threading
from flask import redirect, url_for, flash
from app.scraper.run import run_scrape

# ...

@app.route("/scrape", methods=["POST", "GET"])
def scrape():
    def _job():
        try:
            run_scrape()  # base_url + others default inside run_scrape
        except Exception as e:
            print(f"[SCRAPE THREAD ERROR] {e}", flush=True)

    threading.Thread(target=_job, daemon=True).start()
    flash("Scrape started! Check the Report page in a bit.", "info")
    return redirect(url_for("report"))

@app.context_processor
def inject_globals() -> dict[str, object]:
    """Inject global configuration into all template contexts."""
    return {"config": config}


def _read_last_log_lines(limit: int = 150) -> list[str]:
    """Return the trailing ``limit`` log lines for initial display."""
    ensure_dirs()
    lines: list[str] = []
    if not config.LOG_FILE.exists():
        return lines
    with config.LOG_FILE.open("r", encoding="utf-8", errors="ignore") as handle:
        lines = handle.readlines()[-limit:]
    return [line.rstrip("\n") for line in lines]


def _metadata_to_csv(meta: dict) -> str:
    """Serialise the metadata dictionary to a CSV string."""
    output = io.StringIO()
    fieldnames = [
        "fid",
        "filename",
        "timestamp",
        "title",
        "category",
        "judgment_date",
        "source_url",
        "size_bytes",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for entry in meta.get("downloads", []):
        writer.writerow({key: entry.get(key, "") for key in fieldnames})
    return output.getvalue()


def _tail_log_generator() -> Generator[str, None, None]:
    """Yield Server-Sent Event messages for appended log lines."""
    ensure_dirs()
    with config.LOG_FILE.open("r", encoding="utf-8", errors="ignore") as handle:
        handle.seek(0, os.SEEK_END)
        while True:
            line = handle.readline()
            if line:
                yield f"data: {line.rstrip()}\n\n"
            else:
                time.sleep(1)
                yield ": heartbeat\n\n"


@app.route("/")
def index() -> str:
    """Render the home page with scrape configuration controls."""
    ensure_dirs()
    last_summary = app.config.get("LAST_SUMMARY")
    context = {
        "default_base_url": load_base_url(),
        "default_wait": app.config.get("LAST_PARAMS", {}).get("page_wait", config.PAGE_WAIT_SECONDS),
        "default_cap": app.config.get("LAST_PARAMS", {}).get("entry_cap", config.ENTRY_CAP),
        "default_delay": app.config.get("LAST_PARAMS", {}).get(
            "per_download_delay", config.PER_DOWNLOAD_DELAY
        ),
        "last_summary": last_summary,
    }
    return render_template("index.html", **context)


@app.post("/scrape")
def start_scrape() -> Response:
    """Handle the scrape form submission and trigger a scraping run."""
    base_url = request.form.get("base_url", config.DEFAULT_BASE_URL).strip()
    page_wait = int(request.form.get("page_wait", config.PAGE_WAIT_SECONDS))
    entry_cap = int(request.form.get("entry_cap", config.ENTRY_CAP))
    per_delay = float(request.form.get("per_download_delay", config.PER_DOWNLOAD_DELAY))

    save_base_url(base_url)
    app.config["LAST_PARAMS"] = {
        "base_url": base_url,
        "page_wait": page_wait,
        "entry_cap": entry_cap,
        "per_download_delay": per_delay,
    }
    log_line("Initiating scrape via web UI")
    summary = run_scrape(base_url=base_url, entry_cap=entry_cap, page_wait=page_wait, per_delay=per_delay)
    app.config["LAST_SUMMARY"] = summary
    return redirect(url_for("report"))


@app.get("/report")
def report() -> str:
    """Render the report page with current metadata and live logs."""
    ensure_dirs()
    meta = load_metadata()
    pdfs = list_pdfs()
    total_size = sum(pdf.stat().st_size for pdf in pdfs)
    context = {
        "base_url": load_base_url(),
        "csv_url": config.CSV_URL,
        "pdfs": [
            {
                "name": pdf.name,
                "size": pdf.stat().st_size,
                "timestamp": time.strftime(
                    "%Y-%m-%d %H:%M:%S",
                    time.gmtime(pdf.stat().st_mtime),
                ),
            }
            for pdf in pdfs
        ],
        "total_pdfs": len(pdfs),
        "total_size": total_size,
        "log_lines": _read_last_log_lines(),
        "last_summary": app.config.get("LAST_SUMMARY"),
        "last_params": app.config.get("LAST_PARAMS", {}),
    }
    return render_template("report.html", **context)


@app.get("/logs/stream")
def logs_stream() -> Response:
    """Stream log updates to the browser using SSE."""
    response = Response(_tail_log_generator(), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"
    return response


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
