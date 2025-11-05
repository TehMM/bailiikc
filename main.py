"""Cayman judicial PDF scraper and minimal Flask dashboard."""
from __future__ import annotations

import csv
import json
import os
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import parse_qs, urlparse
from zipfile import ZipFile

import requests
from bs4 import BeautifulSoup
from flask import (
    Flask,
    Response,
    jsonify,
    make_response,
    redirect,
    request,
    send_from_directory,
    url_for,
)

APP = Flask(__name__)

BASE_DATA_DIR = Path("data")
PDF_DIR = BASE_DATA_DIR / "pdfs"
BASE_DATA_DIR.mkdir(exist_ok=True)
PDF_DIR.mkdir(exist_ok=True)

DEFAULT_SOURCE_URL = "https://judicial.ky/judgments/unreported-judgments/"
CSV_URL = "https://judicial.ky/wp-content/uploads/box_files/judgments.csv"
AJAX_URL = "https://judicial.ky/wp-admin/admin-ajax.php"
ZIP_NAME = "judgments.zip"

SCRAPE_LOG = BASE_DATA_DIR / "scrape.log"
SCRAPED_IDS_FILE = BASE_DATA_DIR / "scraped_ids.txt"
CONFIG_FILE = BASE_DATA_DIR / "config.json"
METADATA_FILE = BASE_DATA_DIR / "metadata.json"

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "X-Requested-With": "XMLHttpRequest",
}


# ---------------------------------------------------------------------------
# Storage helpers
# ---------------------------------------------------------------------------


def log_message(message: str) -> None:
    """Write a log entry to stdout and the scrape log."""

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] {message}"
    print(entry)
    with SCRAPE_LOG.open("a", encoding="utf-8") as handle:
        handle.write(entry + "\n")


def read_json(path: Path, default: Iterable | Dict | None = None):
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except json.JSONDecodeError:
        log_message(f"Unable to parse JSON from {path}; using default")
        return default


def write_json(path: Path, payload) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def load_scraped_ids() -> set[str]:
    if not SCRAPED_IDS_FILE.exists():
        return set()
    with SCRAPED_IDS_FILE.open("r", encoding="utf-8") as handle:
        return {line.strip() for line in handle if line.strip()}


def remember_scraped_id(identifier: str) -> None:
    with SCRAPED_IDS_FILE.open("a", encoding="utf-8") as handle:
        handle.write(identifier + "\n")


def load_metadata() -> List[Dict[str, str]]:
    return read_json(METADATA_FILE, default=[]) or []


def save_metadata(rows: Iterable[Dict[str, str]]) -> None:
    write_json(METADATA_FILE, list(rows))


def load_base_url() -> str:
    config = read_json(CONFIG_FILE, default={}) or {}
    return config.get("base_url", DEFAULT_SOURCE_URL)


def save_base_url(url: str) -> None:
    write_json(CONFIG_FILE, {"base_url": url.strip() or DEFAULT_SOURCE_URL})


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------


@dataclass
class ActionInfo:
    file_id: str
    file_name: str
    fallback_url: Optional[str]


@dataclass
class CaseRecord:
    neutral_citation: str
    cause_number: str
    judgment_date: str
    title: str
    subject: str
    court: str
    category: str
    actions: str

    @classmethod
    def from_row(cls, row: Dict[str, str]) -> Optional["CaseRecord"]:
        actions = (row.get("Actions") or "").strip()
        if not actions:
            return None
        category = (row.get("Category") or "").lower()
        if "criminal" in category:
            return None
        return cls(
            neutral_citation=row.get("Neutral Citation", ""),
            cause_number=row.get("Cause Number", ""),
            judgment_date=row.get("Judgment Date", ""),
            title=row.get("Title", ""),
            subject=row.get("Subject", ""),
            court=row.get("Court", ""),
            category=row.get("Category", ""),
            actions=actions,
        )

    def to_metadata(self) -> Dict[str, str]:
        return asdict(self)


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._")
    return cleaned or "document"


def parse_actions(actions_html: str) -> Optional[ActionInfo]:
    if not actions_html:
        return None

    soup = BeautifulSoup(actions_html, "html.parser")
    link = soup.find("a") if soup else None

    file_id: Optional[str] = None
    file_name: Optional[str] = None
    fallback_url: Optional[str] = None

    if link:
        for key in ("data-fid", "data-file", "data-id", "data-item", "data-file-id"):
            value = link.get(key)
            if value:
                file_id = value.strip()
                break

        for key in ("data-fname", "data-name", "data-file-name", "data-title"):
            value = link.get(key)
            if value:
                file_name = value.strip()
                break

        href = link.get("href")
        if href:
            fallback_url = href.strip()
            parsed = urlparse(href)
            query = parse_qs(parsed.query)
            if not file_id:
                for candidate in ("fid", "file", "id"):
                    values = query.get(candidate)
                    if values:
                        file_id = values[0]
                        break
            if not file_name:
                for candidate in ("fname", "name", "file_name"):
                    values = query.get(candidate)
                    if values:
                        file_name = values[0]
                        break
            if not file_name and parsed.path:
                stem = Path(parsed.path).stem
                if stem:
                    file_name = stem

        if not file_name:
            label = link.get_text(strip=True)
            if label:
                file_name = label

    if not file_id and actions_html.strip():
        file_id = actions_html.strip()
        if not file_name:
            file_name = file_id

    if not file_id or not file_name:
        return None

    return ActionInfo(file_id=file_id, file_name=file_name, fallback_url=fallback_url)


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------


class Scraper:
    def __init__(self, base_url: Optional[str] = None):
        self.base_url = (base_url or load_base_url()).strip() or DEFAULT_SOURCE_URL
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)
        self.session.headers["Referer"] = self.base_url

    # ---- utility methods -------------------------------------------------
    def fetch_cases(self) -> List[CaseRecord]:
        log_message(f"Fetching CSV: {CSV_URL}")
        response = self.session.get(CSV_URL, timeout=30)
        response.raise_for_status()

        reader = csv.DictReader(StringIO(response.text))
        cases: List[CaseRecord] = []
        for row in reader:
            case = CaseRecord.from_row(row)
            if case:
                cases.append(case)
        log_message(f"Loaded {len(cases)} case rows from CSV")
        return cases

    def discover_nonce(self) -> Optional[str]:
        log_message(f"Fetching landing page for nonce: {self.base_url}")
        try:
            response = self.session.get(self.base_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            log_message(f"Failed to load base page: {exc}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        for script in soup.find_all("script"):
            content = script.string or ""
            match = re.search(r"security\s*[:=]\s*['\"]([a-f0-9]{10})['\"]", content, re.IGNORECASE)
            if match:
                nonce = match.group(1)
                log_message(f"Found security nonce: {nonce}")
                return nonce
        for script in soup.find_all("script"):
            content = (script.string or "").lower()
            match = re.search(r"\b([a-f0-9]{10})\b", content)
            if match:
                nonce = match.group(1)
                log_message(f"Using fallback nonce: {nonce}")
                return nonce
        log_message("Could not locate nonce on page")
        return None

    def ajax_download_url(self, nonce: str, action: ActionInfo) -> Optional[str]:
        payload = {
            "action": "dl_bfile",
            "fid": action.file_id,
            "fname": action.file_name,
            "security": nonce,
        }
        try:
            response = self.session.post(AJAX_URL, data=payload, timeout=30)
        except requests.RequestException as exc:
            log_message(f"AJAX request failed: {exc}")
            return None

        if response.status_code == 403:
            log_message("AJAX request returned 403")
            return None

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            log_message(f"AJAX error: {exc}")
            return None

        if response.content.startswith(b"%PDF"):
            return response.url or action.fallback_url

        try:
            payload_json = response.json()
        except ValueError:
            log_message("AJAX response was not JSON")
            return None

        if payload_json.get("success"):
            data = payload_json.get("data", {})
            return (
                data.get("url")
                or data.get("download_url")
                or data.get("downloadUrl")
                or data.get("fid")
            )
        log_message("AJAX payload indicated failure")
        return None

    def resolve_download_url(self, nonce: Optional[str], action: ActionInfo) -> Optional[str]:
        if nonce:
            ajax_url = self.ajax_download_url(nonce, action)
            if ajax_url:
                return ajax_url
        if action.fallback_url and action.fallback_url.startswith("http"):
            log_message("Using fallback URL from Actions column")
            return action.fallback_url
        constructed = f"https://judicial.ky/wp-content/uploads/box_files/{action.file_id}.pdf"
        log_message(f"Constructed fallback URL: {constructed}")
        return constructed

    def download_pdf(self, url: str, destination: Path) -> tuple[bool, Optional[str]]:
        try:
            with self.session.get(url, timeout=60, stream=True) as response:
                response.raise_for_status()
                content = response.content
        except requests.RequestException as exc:
            return False, f"Request failed: {exc}"

        if not content.startswith(b"%PDF"):
            return False, "Response did not contain a PDF"

        destination.write_bytes(content)
        return True, None

    # ---- main entry point ------------------------------------------------
    def run(self) -> List[Dict[str, str]]:
        log_message("=" * 70)
        log_message("Starting scrape session")
        log_message(f"Target page: {self.base_url}")

        scraped_ids = load_scraped_ids()
        log_message(f"Loaded {len(scraped_ids)} known identifiers")

        nonce = self.discover_nonce()
        if not nonce:
            log_message("Proceeding without nonce; will rely on fallbacks")

        try:
            cases = self.fetch_cases()
        except Exception as exc:  # pragma: no cover - defensive logging
            log_message(f"Failed to load CSV: {exc}")
            return []

        pending = [case for case in cases if case.actions not in scraped_ids]
        log_message(f"Preparing to download {len(pending)} new PDFs")

        results: List[Dict[str, str]] = []
        for index, case in enumerate(pending, start=1):
            action = parse_actions(case.actions)
            if not action:
                log_message("Unable to parse Actions field; skipping")
                continue

            filename = sanitize_filename(action.file_name) + ".pdf"
            destination = PDF_DIR / filename

            log_message(
                f"[{index}/{len(pending)}] {case.title or 'Untitled case'} "
                f"(fid={action.file_id}, file={filename})"
            )

            status = "SKIPPED"
            message = ""

            if destination.exists():
                status = "EXISTS"
                message = "Already downloaded"
                log_message(f"✓ {filename} already exists")
            else:
                download_url = self.resolve_download_url(nonce, action)
                if not download_url:
                    status = "FAILED"
                    message = "Could not resolve download URL"
                    log_message("✗ No download URL available")
                else:
                    success, error = self.download_pdf(download_url, destination)
                    if success:
                        status = "DOWNLOADED"
                        size_kib = destination.stat().st_size / 1024
                        message = f"Saved {filename} ({size_kib:.1f} KiB)"
                        log_message(f"✓ {message}")
                    else:
                        status = "FAILED"
                        message = error or "Download failed"
                        log_message(f"✗ Download failed: {message}")
                        if destination.exists():
                            destination.unlink()

            record = {
                **case.to_metadata(),
                "file": filename,
                "status": status,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "message": message,
            }
            results.append(record)

            if status in {"DOWNLOADED", "EXISTS"}:
                remember_scraped_id(case.actions)

            time.sleep(1)

        if results:
            existing = load_metadata()
            save_metadata(existing + results)
            log_message(f"Persisted metadata for {len(results)} rows")

        log_message("Scrape session complete")
        log_message("=" * 70)
        return results


# ---------------------------------------------------------------------------
# ZIP packaging
# ---------------------------------------------------------------------------


def build_zip_archive() -> Path:
    zip_path = BASE_DATA_DIR / ZIP_NAME
    with ZipFile(zip_path, "w") as archive:
        count = 0
        for pdf in PDF_DIR.glob("*.pdf"):
            archive.write(pdf, pdf.name)
            count += 1
    log_message(f"Created ZIP archive containing {count} PDFs")
    return zip_path


# ---------------------------------------------------------------------------
# Flask views
# ---------------------------------------------------------------------------


def cloak_url(url: str) -> str:
    return f"http://anon.to/?{url}" if url.startswith("http") else url


@APP.route("/")
def index() -> str:
    current_url = load_base_url()
    return f"""
    <html>
    <head>
        <title>Cayman Judicial PDF Scraper</title>
        <style>
            body {{ font-family: Arial, sans-serif; background: #f4f4f4; padding: 20px; }}
            h1 {{ color: #333; }}
            .form-group {{ margin-bottom: 14px; }}
            input[type=text] {{ width: 420px; padding: 8px; }}
            button {{ padding: 8px 16px; margin-top: 6px; }}
            a {{ color: #0052cc; text-decoration: none; }}
        </style>
    </head>
    <body>
        <h1>Cayman Judicial PDF Scraper</h1>
        <form method="POST" action="/update-config">
            <div class="form-group">
                <label>Source page URL:</label><br />
                <input type="text" name="base_url" value="{current_url}" />
            </div>
            <button type="submit">Save URL</button>
        </form>
        <form method="POST" action="/run-scraper">
            <button type="submit">Run Scraper</button>
        </form>
        <p>
            <a href="/report">View download report</a> |
            <a href="{cloak_url(CSV_URL)}" target="_blank">Open CSV source</a>
        </p>
    </body>
    </html>
    """


@APP.route("/update-config", methods=["POST"])
def update_config() -> Response:
    new_url = request.form.get("base_url", "").strip()
    if new_url:
        save_base_url(new_url)
        log_message(f"Updated base URL to {new_url}")
    return redirect(url_for("index"))


@APP.route("/run-scraper", methods=["POST"])
def run_scraper() -> Response:
    custom_url = request.form.get("base_url") or request.args.get("url")
    Scraper(custom_url).run()
    build_zip_archive()
    return redirect(url_for("report"))


@APP.route("/report")
def report() -> str:
    files = []
    for pdf in PDF_DIR.glob("*.pdf"):
        files.append(
            {
                "name": pdf.name,
                "size": f"{pdf.stat().st_size / 1024:.1f} KiB",
                "timestamp": datetime.fromtimestamp(pdf.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    files.sort(key=lambda item: item["timestamp"], reverse=True)

    log_tail = ""
    if SCRAPE_LOG.exists():
        log_tail = "\n".join(SCRAPE_LOG.read_text(encoding="utf-8").splitlines()[-200:])

    rows_html = "".join(
        f"<tr><td>{item['name']}</td><td>{item['size']}</td><td>{item['timestamp']}</td></tr>"
        for item in files
    )

    zip_exists = (BASE_DATA_DIR / ZIP_NAME).exists()
    zip_link = (
        f"<a href=\"/files/{ZIP_NAME}\">Download ZIP</a>" if zip_exists else "ZIP not created yet."
    )

    return f"""
    <html>
    <head>
        <title>Scraper report</title>
        <style>
            body {{ font-family: Arial, sans-serif; background: #f4f4f4; padding: 20px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; }}
            th {{ background-color: #222; color: #fff; }}
            tr:nth-child(even) {{ background-color: #fafafa; }}
            pre {{ background: #111; color: #0f0; padding: 12px; max-height: 320px; overflow-y: auto; }}
        </style>
    </head>
    <body>
        <h1>Download report</h1>
        <p><a href="/">Back to dashboard</a></p>
        <h2>PDF files</h2>
        <table>
            <tr><th>Filename</th><th>Size</th><th>Downloaded</th></tr>
            {rows_html}
        </table>
        <p>{zip_link}</p>
        <h2>Recent logs</h2>
        <pre>{log_tail}</pre>
    </body>
    </html>
    """


@APP.route("/files/<path:filename>")
def serve_file(filename: str):
    try:
        return send_from_directory(BASE_DATA_DIR, filename, as_attachment=True)
    except FileNotFoundError:
        return "File not found", 404


@APP.route("/api/metadata")
def api_metadata():
    return jsonify(load_metadata())


@APP.route("/export/csv")
def export_csv():
    metadata = load_metadata()
    if not metadata:
        return "No metadata available", 404

    buffer = StringIO()
    fieldnames = [
        "neutral_citation",
        "cause_number",
        "judgment_date",
        "title",
        "subject",
        "court",
        "category",
        "file",
        "status",
        "timestamp",
        "message",
    ]
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in metadata:
        writer.writerow({key: row.get(key, "") for key in fieldnames})

    response = make_response(buffer.getvalue())
    response.headers["Content-Disposition"] = "attachment; filename=judgments_metadata.csv"
    response.headers["Content-Type"] = "text/csv"
    return response


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    APP.run(host="0.0.0.0", port=port, debug=False)
