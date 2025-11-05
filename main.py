"""Comprehensive Cayman judicial PDF scraper and web dashboard."""
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
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse
from zipfile import ZipFile

import requests
from bs4 import BeautifulSoup
from flask import (
    Flask,
    jsonify,
    make_response,
    redirect,
    request,
    send_from_directory,
    url_for,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
APP = Flask(__name__)

DATA_DIR = Path("./data/pdfs")
DATA_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_BASE_URL = "https://judicial.ky/judgments/unreported-judgments/"
CSV_URL = "https://judicial.ky/wp-content/uploads/box_files/judgments.csv"
ZIP_NAME = "all_pdfs.zip"

SCRAPE_LOG_PATH = DATA_DIR / "scrape_log.txt"
SCRAPED_IDS_PATH = DATA_DIR / "scraped_urls.txt"
CONFIG_PATH = DATA_DIR / "config.txt"
METADATA_PATH = DATA_DIR / "metadata.json"

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
    "Origin": "https://judicial.ky",
}


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def log_message(message: str) -> None:
    """Write a timestamped log line to stdout and the scrape log."""

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] {message}"
    print(entry)
    with SCRAPE_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(entry + "\n")


def read_text(path: Path) -> str:
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    return ""


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def load_scraped_ids() -> set[str]:
    if not SCRAPED_IDS_PATH.exists():
        return set()
    with SCRAPED_IDS_PATH.open("r", encoding="utf-8") as handle:
        return {line.strip() for line in handle if line.strip()}


def append_scraped_id(identifier: str) -> None:
    with SCRAPED_IDS_PATH.open("a", encoding="utf-8") as handle:
        handle.write(identifier + "\n")


def load_metadata() -> List[Dict[str, str]]:
    if METADATA_PATH.exists():
        with METADATA_PATH.open("r", encoding="utf-8") as handle:
            try:
                return json.load(handle)
            except json.JSONDecodeError:
                log_message("Metadata file is corrupt; starting with an empty list")
    return []


def save_metadata(metadata: Iterable[Dict[str, str]]) -> None:
    with METADATA_PATH.open("w", encoding="utf-8") as handle:
        json.dump(list(metadata), handle, indent=2)


def load_base_url() -> str:
    stored = read_text(CONFIG_PATH)
    return stored or DEFAULT_BASE_URL


def save_base_url(url: str) -> None:
    write_text(CONFIG_PATH, url.strip())


def cloak_url(url: str) -> str:
    return f"http://anon.to/?{url}" if url.startswith("http") else url


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

@dataclass
class ActionInfo:
    file_id: str
    file_name: str
    fallback_url: Optional[str]


@dataclass
class CaseEntry:
    neutral_citation: str
    cause_number: str
    judgment_date: str
    title: str
    subject: str
    court: str
    category: str
    actions: str

    def to_metadata(self) -> Dict[str, str]:
        return asdict(self)


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._")
    return cleaned or "document"


def parse_actions_field(actions_html: str) -> Optional[ActionInfo]:
    if not actions_html:
        return None

    soup = BeautifulSoup(actions_html, "html.parser")
    link = soup.find("a") if soup else None

    file_id: Optional[str] = None
    file_name: Optional[str] = None
    fallback_url: Optional[str] = None

    if link:
        for attribute in ("data-fid", "data-file", "data-id", "data-item", "data-file-id"):
            value = link.get(attribute)
            if value:
                file_id = value.strip()
                break

        for attribute in ("data-fname", "data-name", "data-file-name", "data-title"):
            value = link.get(attribute)
            if value:
                file_name = value.strip()
                break

        href = link.get("href")
        if href:
            fallback_url = href.strip()
            parsed = urlparse(href)
            query = parse_qs(parsed.query)

            if not file_id:
                for key in ("fid", "file", "id"):
                    if query.get(key):
                        file_id = query[key][0]
                        break

            if not file_name:
                for key in ("fname", "name", "file_name"):
                    if query.get(key):
                        file_name = query[key][0]
                        break

            if not file_name and parsed.path:
                potential = Path(parsed.path).stem
                if potential:
                    file_name = potential

        if not file_name:
            label = link.get_text(strip=True)
            if label:
                file_name = label

    if not file_id and actions_html:
        stripped = actions_html.strip()
        if stripped:
            file_id = stripped
            if not file_name:
                file_name = stripped

    if not file_id or not file_name:
        return None

    return ActionInfo(file_id=file_id, file_name=file_name, fallback_url=fallback_url)


def row_is_relevant(row: Dict[str, str]) -> bool:
    actions = (row.get("Actions") or "").strip()
    category = (row.get("Category") or "").lower()
    return bool(actions) and "criminal" not in category


def load_cases(session: requests.Session) -> List[CaseEntry]:
    log_message(f"Fetching CSV data from {CSV_URL}")
    response = session.get(CSV_URL, timeout=30)
    response.raise_for_status()

    reader = csv.DictReader(StringIO(response.text))
    entries: List[CaseEntry] = []
    for row in reader:
        if not row_is_relevant(row):
            continue
        entries.append(
            CaseEntry(
                neutral_citation=row.get("Neutral Citation", ""),
                cause_number=row.get("Cause Number", ""),
                judgment_date=row.get("Judgment Date", ""),
                title=row.get("Title", ""),
                subject=row.get("Subject", ""),
                court=row.get("Court", ""),
                category=row.get("Category", ""),
                actions=row.get("Actions", ""),
            )
        )

    log_message(f"Loaded {len(entries)} relevant case rows")
    return entries


# ---------------------------------------------------------------------------
# Scraper internals
# ---------------------------------------------------------------------------

def build_session(base_url: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)
    session.headers["Referer"] = base_url
    return session


def discover_security_nonce(session: requests.Session, base_url: str) -> Optional[str]:
    log_message(f"Discovering security nonce from {base_url}")
    response = session.get(base_url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    for script in soup.find_all("script"):
        content = script.string or ""
        match = re.search(r"security\s*[:=]\s*['\"]([a-f0-9]{10})['\"]", content, re.IGNORECASE)
        if match:
            nonce = match.group(1)
            log_message(f"Found nonce: {nonce}")
            return nonce

    for script in soup.find_all("script"):
        content = (script.string or "").lower()
        match = re.search(r"\b([a-f0-9]{10})\b", content)
        if match:
            nonce = match.group(1)
            log_message(f"Possible fallback nonce: {nonce}")
            return nonce

    log_message("Failed to discover security nonce")
    return None


def ajax_download_url(session: requests.Session, nonce: str, action_info: ActionInfo) -> Optional[str]:
    payload = {
        "action": "dl_bfile",
        "fid": action_info.file_id,
        "fname": action_info.file_name,
        "security": nonce,
    }

    try:
        response = session.post(
            "https://judicial.ky/wp-admin/admin-ajax.php",
            data=payload,
            headers=session.headers,
            timeout=30,
        )
    except requests.RequestException as exc:
        log_message(f"AJAX request failed: {exc}")
        return None

    if response.status_code == 403:
        log_message("AJAX request returned 403 Forbidden")
        return None

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        log_message(f"AJAX error: {exc}")
        return None

    if response.content.startswith(b"%PDF"):
        return response.url or action_info.fallback_url

    try:
        payload_json = response.json()
    except ValueError:
        log_message("AJAX response was not JSON")
        return None

    if payload_json.get("success"):
        data = payload_json.get("data", {})
        return (
            data.get("fid")
            or data.get("url")
            or data.get("download_url")
            or data.get("downloadUrl")
        )

    log_message("AJAX payload indicated failure")
    return None


def resolve_download_url(
    session: requests.Session,
    nonce: Optional[str],
    action_info: ActionInfo,
) -> Optional[str]:
    if nonce:
        ajax_url = ajax_download_url(session, nonce, action_info)
        if ajax_url:
            return ajax_url

    if action_info.fallback_url and action_info.fallback_url.startswith("http"):
        log_message("Using fallback URL from Actions column")
        return action_info.fallback_url

    constructed = f"https://judicial.ky/wp-content/uploads/box_files/{action_info.file_id}.pdf"
    log_message(f"Constructed fallback URL: {constructed}")
    return constructed


def download_pdf(session: requests.Session, url: str, destination: Path) -> Tuple[bool, Optional[str]]:
    try:
        with session.get(url, timeout=60, stream=True) as response:
            response.raise_for_status()
            content = response.content
    except requests.RequestException as exc:
        return False, f"Request failed: {exc}"

    if not content.startswith(b"%PDF"):
        return False, "Downloaded content is not a PDF"

    destination.write_bytes(content)
    return True, None


def scrape_pdfs(base_url: Optional[str] = None) -> List[Dict[str, str]]:
    log_message("=" * 70)
    log_message("Starting new scrape session")

    base_url = base_url.strip() if base_url else load_base_url()
    log_message(f"Target page: {base_url}")

    session = build_session(base_url)
    scraped_ids = load_scraped_ids()
    log_message(f"Loaded {len(scraped_ids)} previously downloaded identifiers")

    nonce = discover_security_nonce(session, base_url)
    if not nonce:
        log_message("Proceeding without nonce; will rely on fallbacks")

    try:
        cases = load_cases(session)
    except Exception as exc:  # pragma: no cover - defensive logging
        log_message(f"Failed to load cases: {exc}")
        return []

    new_cases = [case for case in cases if case.actions not in scraped_ids]
    log_message(f"Preparing to download {len(new_cases)} new PDFs")

    results: List[Dict[str, str]] = []
    for index, case in enumerate(new_cases, start=1):
        action_info = parse_actions_field(case.actions)
        if not action_info:
            log_message("Unable to parse Actions column; skipping entry")
            continue

        sanitized_name = sanitize_filename(action_info.file_name)
        filename = f"{sanitized_name}.pdf"
        file_path = DATA_DIR / filename

        log_message(
            f"[{index}/{len(new_cases)}] {case.title or 'Untitled case'} "
            f"(fid={action_info.file_id}, file={filename})"
        )

        status = "SKIPPED"
        message = ""

        if file_path.exists():
            status = "EXISTS"
            message = "Already downloaded"
            log_message(f"✓ {filename} already exists")
        else:
            download_url = resolve_download_url(session, nonce, action_info)
            if not download_url:
                status = "FAILED"
                message = "No download URL resolved"
                log_message("✗ Could not resolve download URL")
            else:
                success, error = download_pdf(session, download_url, file_path)
                if success:
                    status = "DOWNLOADED"
                    message = f"Saved {filename}"
                    size_kib = file_path.stat().st_size / 1024
                    log_message(f"✓ Saved {filename} ({size_kib:.1f} KiB)")
                else:
                    status = "FAILED"
                    message = error or "Unknown error"
                    log_message(f"✗ Failed to download: {message}")
                    if file_path.exists():
                        file_path.unlink()

        entry_metadata = {
            **case.to_metadata(),
            "file": filename,
            "status": status,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message": message,
        }
        results.append(entry_metadata)

        if status in {"DOWNLOADED", "EXISTS"}:
            append_scraped_id(case.actions)

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

def create_zip_archive() -> Path:
    zip_path = DATA_DIR / ZIP_NAME
    with ZipFile(zip_path, "w") as archive:
        count = 0
        for file in DATA_DIR.glob("*.pdf"):
            archive.write(file, file.name)
            count += 1
    log_message(f"Created archive with {count} PDFs")
    return zip_path


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@APP.route("/")
def index() -> str:
    current_url = load_base_url()
    return f'''
    <html>
    <head>
        <title>Cayman Judicial PDF Scraper</title>
        <style>
            body {{ font-family: Arial, sans-serif; background: #f4f4f4; padding: 20px; }}
            h1 {{ color: #333; }}
            .form-group {{ margin-bottom: 15px; }}
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
        <form method="POST" action="/run-download">
            <button type="submit">Run Scraper</button>
        </form>
        <p>
            <a href="/report">View download report</a> |
            <a href="{cloak_url(CSV_URL)}" target="_blank">Open CSV source</a>
        </p>
    </body>
    </html>
    '''


@APP.route("/update-config", methods=["POST"])
def update_config():
    new_url = request.form.get("base_url", "").strip()
    if new_url:
        save_base_url(new_url)
        log_message(f"Updated base URL to {new_url}")
    return redirect(url_for("index"))


@APP.route("/run-download", methods=["POST"])
def run_download():
    custom_url = request.form.get("base_url") or request.args.get("url")
    scrape_pdfs(custom_url)
    create_zip_archive()
    return redirect(url_for("report"))


@APP.route("/report")
def report() -> str:
    files = []
    for pdf in DATA_DIR.glob("*.pdf"):
        files.append(
            {
                "name": pdf.name,
                "size": f"{pdf.stat().st_size / 1024:.1f} KiB",
                "timestamp": datetime.fromtimestamp(pdf.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    files.sort(key=lambda item: item["timestamp"], reverse=True)

    log_tail = ""
    if SCRAPE_LOG_PATH.exists():
        lines = SCRAPE_LOG_PATH.read_text(encoding="utf-8").splitlines()[-200:]
        log_tail = "\n".join(lines)

    zip_exists = (DATA_DIR / ZIP_NAME).exists()

    rows_html = "".join(
        f"<tr><td>{item['name']}</td><td>{item['size']}</td><td>{item['timestamp']}</td></tr>"
        for item in files
    )

    zip_link = (
        f"<a href=\"/files/{ZIP_NAME}\">Download ZIP</a>" if zip_exists else "ZIP not created yet."
    )

    return f'''
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
    '''


@APP.route("/files/<path:filename>")
def serve_file(filename: str):
    try:
        return send_from_directory(DATA_DIR, filename, as_attachment=True)
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
