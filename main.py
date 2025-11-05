"""Comprehensive Cayman judicial PDF scraper and web dashboard."""
from __future__ import annotations

import csv
import json
import os
import re
import time
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple
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
    render_template_string,
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
SCRAPE_LOG = str(DATA_DIR / "scrape_log.txt")
SCRAPED_URLS_FILE = str(DATA_DIR / "scraped_urls.txt")
CONFIG_FILE = str(DATA_DIR / "config.txt")
METADATA_FILE = str(DATA_DIR / "metadata.json")

# Headers (single, deduped set)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://judicial.ky",
    "Referer": DEFAULT_BASE_URL,
}

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def log_message(message: str) -> None:
    """Write a timestamped log line to stdout and the scrape log."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    print(log_entry.strip())
    with open(SCRAPE_LOG, "a", encoding="utf-8") as f:
        f.write(log_entry)

def load_scraped_urls() -> set:
    if os.path.exists(SCRAPED_URLS_FILE):
        with open(SCRAPED_URLS_FILE, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def save_scraped_url(identifier: str) -> None:
    with open(SCRAPED_URLS_FILE, "a", encoding="utf-8") as f:
        f.write(f"{identifier}\n")

def load_base_url() -> str:
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            url = f.read().strip()
            if url:
                return url
    return DEFAULT_BASE_URL

def save_base_url(url: str) -> None:
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        f.write(url.strip())

def save_metadata(metadata_list: List[Dict]) -> None:
    with open(METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(metadata_list, f, indent=2)

def load_metadata() -> List[Dict]:
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def cloak_url(url: str) -> str:
    if url and url.startswith("http"):
        return f"http://anon.to/?{url}"
    return url

def sanitize_filename(name: str) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", (name or "").strip())
    return name.strip("._") or "file"

# ---------------------------------------------------------------------------
# Page parsing helpers
# ---------------------------------------------------------------------------

def extract_security_nonce(soup: BeautifulSoup) -> Optional[str]:
    """Extract the correct WordPress security nonce for dl_bfile AJAX calls."""
    scripts = soup.find_all("script")

    for script in scripts:
        if not script.string:
            continue

        # Case 1: Find 'dl_bfile' or similar object with 'security' field
        if "dl_bfile" in script.string or "dlbfile" in script.string or "security" in script.string:
            m = re.search(r"['\"]security['\"]\s*:\s*['\"]([a-f0-9]{10})['\"]", script.string)
            if m:
                nonce = m.group(1)
                log_message(f"Found dl_bfile nonce: {nonce}")
                return nonce

        # Case 2: Generic pattern like var security="abc123"
        m = re.search(r"var\s+security\s*=\s*['\"]([a-f0-9]{10})['\"]", script.string)
        if m:
            nonce = m.group(1)
            log_message(f"Found generic nonce: {nonce}")
            return nonce

        # Case 3: Look for wp_localize_script or JSON blocks
        m = re.search(r'"_nonce"\s*:\s*"([a-f0-9]{10})"', script.string)
        if m:
            nonce = m.group(1)
            log_message(f"Found _nonce key: {nonce}")
            return nonce

    # Fallback search anywhere in page source
    html = soup.get_text(" ")
    m = re.search(r'["\']([a-f0-9]{10})["\']', html)
    if m:
        nonce = m.group(1)
        log_message(f"Fallback nonce found: {nonce}")
        return nonce

    log_message("✗ No valid security nonce found in scripts")
    return None

def fetch_csv_data(csv_url: str, session: requests.Session) -> List[Dict]:
    """Fetch and parse the judgments CSV file (skips criminal)."""
    try:
        log_message(f"Fetching CSV from: {csv_url}")
        resp = session.get(csv_url, timeout=30)
        resp.raise_for_status()

        reader = csv.DictReader(StringIO(resp.text))
        entries = []
        for row in reader:
            actions = row.get("Actions", "")
            if not actions:
                continue
            category = row.get("Category", "") or ""
            if "criminal" in category.lower() or "crim" in category.lower():
                continue
            entries.append(
                {
                    "neutral_citation": row.get("Neutral Citation", "") or "",
                    "cause_number": row.get("Cause Number", "") or "",
                    "judgment_date": row.get("Judgment Date", "") or "",
                    "title": row.get("Title", "") or "",
                    "subject": row.get("Subject", "") or "",
                    "court": row.get("Court", "") or "",
                    "category": category,
                    "actions": actions,
                }
            )
        log_message(f"Loaded {len(entries)} non-criminal cases from CSV")
        return entries
    except Exception as e:
        log_message(f"ERROR fetching CSV: {e}")
        return []

def parse_actions_field(actions_value: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract Box identifiers (fid, fname) from the CSV Actions column.
    Supports: data-fid/data-fname, URL query, inner text, or raw string fallback.
    """
    if not actions_value:
        return None, None

    fid: Optional[str] = None
    fname: Optional[str] = None

    try:
        action_soup = BeautifulSoup(actions_value, "html.parser")
    except Exception:
        action_soup = None

    if action_soup:
        link = action_soup.find("a")
        if link:
            # Data attributes
            for attr in ("data-fid", "data-file", "data-id", "data-item", "data-file-id"):
                if link.get(attr):
                    fid = (link.get(attr) or "").strip()
                    break
            for attr in ("data-fname", "data-name", "data-file-name", "data-title"):
                if link.get(attr):
                    fname = (link.get(attr) or "").strip()
                    break

            # Parse href query if necessary
            href = link.get("href")
            if href:
                parsed = urlparse(href)
                query_params = parse_qs(parsed.query)
                if not fid:
                    for key in ("fid", "file", "id"):
                        val = query_params.get(key)
                        if val:
                            fid = val[0]
                            break
                if not fname:
                    for key in ("fname", "name", "file_name"):
                        val = query_params.get(key)
                        if val:
                            fname = val[0]
                            break
                # Try path filename as last resort
                if not fname:
                    possible_name = os.path.basename(parsed.path)
                    if possible_name:
                        fname = os.path.splitext(possible_name)[0]

            # If still no fname, use link text
            if not fname:
                text_label = link.get_text(strip=True)
                if text_label:
                    fname = re.sub(r"\s+", "_", text_label)

    # Raw fallback
    if not fid or not fname:
        cleaned = (actions_value or "").strip()
        if not fid:
            fid = cleaned
        if not fname:
            fname = cleaned

    return fid, fname

# ---------------------------------------------------------------------------
# Networking / Download
# ---------------------------------------------------------------------------

def build_session(base_url: str) -> requests.Session:
    """Create a session and pre-populate cookies by visiting the base page."""
    session = requests.Session()
    session.headers.update(HEADERS)
    # Load main page to establish pdb-sess cookie
    try:
        r = session.get(base_url, timeout=15)
        r.raise_for_status()
        if "pdb-sess" in session.cookies:
            log_message(f"Session cookie set: pdb-sess={session.cookies.get('pdb-sess')[:8]}…")
        else:
            log_message("⚠️ pdb-sess cookie not found after initial request")
    except Exception as e:
        log_message(f"⚠️ Could not fetch main page for session init: {e}")
    return session


def get_box_url(fid: str, fname: str, security: str, session: requests.Session) -> Optional[str]:
    """Fully emulate browser AJAX to get Box.com download URL."""
    ajax_url = "https://judicial.ky/wp-admin/admin-ajax.php"
    payload = {
        "action": "dl_bfile",
        "fid": fid,
        "fname": fname,
        "security": security
    }

    try:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://judicial.ky",
            "Referer": "https://judicial.ky/judgments/unreported-judgments/",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        }

        # Ensure we’re sending pdb-sess
        if "pdb-sess" not in session.cookies:
            log_message("⚠️ pdb-sess cookie missing before AJAX, re-fetching base page")
            session.get("https://judicial.ky/judgments/unreported-judgments/", timeout=10)

        cookie_info = "; ".join(f"{c.name}={c.value}" for c in session.cookies)
        log_message(f"Using cookies: {cookie_info}")

        resp = session.post(ajax_url, data=payload, headers=headers, timeout=30)
        if resp.status_code == 403:
            log_message(f"✗ 403 Forbidden (nonce or cookie mismatch)")
            log_message(f"  Sent payload: {payload}")
            return None
        if resp.status_code != 200:
            log_message(f"✗ Unexpected status {resp.status_code}: {resp.text[:200]}")
            return None

        data = resp.json()
        if data.get("success"):
            box_url = (data.get("data") or {}).get("fid")
            if box_url and box_url.startswith("https://dl.boxcloud.com/"):
                log_message(f"✓ AJAX returned valid Box.com URL for {fid}")
                return box_url
            log_message(f"⚠️ AJAX success but unexpected structure: {json.dumps(data)[:400]}")
        else:
            log_message(f"✗ AJAX returned failure payload: {data}")
        return None

    except Exception as e:
        log_message(f"✗ AJAX call error: {e}")
        return None

def try_download_pdf(session: requests.Session, url: str, out_path: Path) -> Tuple[bool, Optional[str]]:
    try:
        r = session.get(url, timeout=60, stream=True)
        r.raise_for_status()
        content = r.content
        if content[:4] != b"%PDF":
            return False, "Not a PDF"
        with open(out_path, "wb") as f:
            f.write(content)
        return True, None
    except Exception as e:
        return False, str(e)

# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

def scrape_pdfs(base_url: Optional[str] = None) -> List[Dict]:
    results: List[Dict] = []
    log_message("=" * 60)
    log_message("Starting new scrape session")

    scraped_ids = load_scraped_urls()
    log_message(f"Loaded {len(scraped_ids)} previously scraped identifiers")

    if not base_url:
        base_url = load_base_url()
    log_message(f"Target URL: {base_url}")

    session = build_session(base_url)

    try:
        # 1) Load base page and discover nonce
        log_message("Fetching main page for security nonce...")
        r = session.get(base_url, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        security_nonce = extract_security_nonce(soup)
        if not security_nonce:
            log_message("ERROR: Could not find security nonce on page")
            # Save for debugging
            debug_file = DATA_DIR / "debug_page.html"
            with open(debug_file, "w", encoding="utf-8") as f:
                f.write(soup.prettify())
            return results

        log_message(f"✓ Found security nonce: {security_nonce}")

        # 2) Fetch CSV entries
        csv_entries = fetch_csv_data(CSV_URL, session)
        if not csv_entries:
            log_message("ERROR: Could not fetch CSV data")
            return results

        # Filter already processed by identifier (fid or fname)
        def id_from_entry(e: Dict) -> str:
            fid, fname = parse_actions_field(e["actions"])
            return (fid or fname or "").strip()

        new_entries = [e for e in csv_entries if id_from_entry(e) not in scraped_ids]
        skipped_count = len(csv_entries) - len(new_entries)
        if skipped_count:
            log_message(f"Skipping {skipped_count} previously scraped entries")
        log_message(f"Will download {len(new_entries)} new PDFs")

        # 3) Loop and download
        for idx, entry in enumerate(new_entries, start=1):
            try:
                fid, fname = parse_actions_field(entry["actions"])
                if not fid or not fname:
                    status = "PARSE_FAILED"
                    log_message("  ✗ Could not parse file identifiers from Actions")
                    results.append({
                        "file": "",
                        "status": status,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "identifier": entry["actions"],
                        "neutral_citation": entry["neutral_citation"],
                        "cause_number": entry["cause_number"],
                        "judgment_date": entry["judgment_date"],
                        "title": entry["title"],
                        "subject": entry["subject"],
                        "court": entry["court"],
                        "category": entry["category"],
                    })
                    continue

                safe_name = sanitize_filename(fname)
                pdf_filename = f"{safe_name}.pdf"
                pdf_path = DATA_DIR / pdf_filename

                log_message(f"[{idx}/{len(new_entries)}] {entry['title']} (fid={fid}, fname={safe_name})")

                if pdf_path.exists():
                    status = "EXISTING"
                    log_message(f"  ✓ Already have: {pdf_filename}")
                else:
                    # Resolve Box URL via AJAX
                    log_message("  → Calling AJAX to get Box URL...")
                    box_url = get_box_url(fid, fname, security_nonce, session)

                    # Fallback: try a plausible direct path (not guaranteed)
                    if not box_url:
                        fallback_url = f"https://judicial.ky/wp-content/uploads/box_files/{fid}.pdf"
                        log_message(f"  ⚠️ AJAX failed; trying fallback {fallback_url}")
                        box_url = fallback_url

                    if not box_url:
                        status = "API_FAILED"
                        log_message("  ✗ Could not resolve any download URL")
                    else:
                        ok, err = try_download_pdf(session, box_url, pdf_path)
                        if ok:
                            status = "NEW"
                            size_kb = pdf_path.stat().st_size / 1024
                            log_message(f"  ✓ Saved: {pdf_filename} ({size_kb:.1f} KB)")
                        else:
                            status = f"DOWNLOAD_ERROR: {err or 'unknown'}"
                            log_message(f"  ✗ Download failed: {err}")

                # Record
                results.append({
                    "file": pdf_filename,
                    "status": status,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "identifier": fid,
                    "neutral_citation": entry["neutral_citation"],
                    "cause_number": entry["cause_number"],
                    "judgment_date": entry["judgment_date"],
                    "title": entry["title"],
                    "subject": entry["subject"],
                    "court": entry["court"],
                    "category": entry["category"],
                })

                # Mark processed (both fid and fname, defensive)
                save_scraped_url(fid)
                save_scraped_url(fname)

                time.sleep(1)  # be polite
            except Exception as e:
                log_message(f"  ✗ Error processing entry: {e}")
                continue

        if results:
            save_metadata(results)
            log_message(f"Saved metadata for {len(results)} entries")

        log_message("Scraping complete")
        log_message("=" * 60)
    except Exception as e:
        log_message(f"SCRAPING ERROR: {e}")
        results.append({
            "file": "ERROR",
            "status": str(e),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "identifier": "",
        })
    return results

# ---------------------------------------------------------------------------
# ZIP packaging
# ---------------------------------------------------------------------------

def create_zip_archive() -> Path:
    zip_path = DATA_DIR / ZIP_NAME
    try:
        with ZipFile(zip_path, "w") as archive:
            count = 0
            for file in DATA_DIR.glob("*.pdf"):
                archive.write(file, file.name)
                count += 1
        log_message(f"Created archive with {count} PDFs")
    except Exception as e:
        log_message(f"ZIP creation error: {e}")
    return zip_path

# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@APP.route("/")
def index() -> str:
    current_url = load_base_url()
    html = f"""
    <html>
    <head>
        <title>Cayman Judicial PDF Scraper</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; max-width: 900px; }}
            .button {{ background:#4CAF50;color:#fff;padding:10px 16px;border:none;border-radius:4px;cursor:pointer; }}
            .button.secondary {{ background:#008CBA; }}
            input[type="url"], input[type="text"] {{ width:100%;padding:10px;border:1px solid #ddd;border-radius:4px; }}
            .info {{ background:#f0f8ff;border-left:4px solid #2196F3;padding:12px;margin:16px 0; }}
        </style>
    </head>
    <body>
        <h1>🏛️ Cayman Judicial PDF Scraper</h1>
        <div class="info">
            <div><strong>Current Target:</strong> <a href="{cloak_url(current_url)}" target="_blank" rel="noopener">{current_url}</a></div>
            <div><strong>CSV Source:</strong> <a href="{cloak_url(CSV_URL)}" target="_blank" rel="noopener">{CSV_URL}</a></div>
        </div>

        <form method="POST" action="{url_for('update_config')}">
            <label>Base URL (for nonce extraction)</label>
            <input type="url" name="base_url" value="{current_url}" required />
            <p><button class="button" type="submit">💾 Save URL</button></p>
        </form>

        <form method="POST" action="{url_for('run_download')}" style="margin-top:16px;">
            <p><button class="button secondary" type="submit">▶️ Run Scraper Now</button></p>
        </form>

        <p style="margin-top:16px;"><a class="button" href="{url_for('report')}">📊 View Report & Downloads</a></p>
    </body>
    </html>
    """
    return html

@APP.route("/update-config", methods=["POST"])
def update_config():
    new_url = (request.form.get("base_url") or "").strip()
    if new_url:
        save_base_url(new_url)
        log_message(f"Configuration updated: Base URL -> {new_url}")
    return redirect(url_for("index"))

@APP.route("/run-download", methods=["GET", "POST"])
def run_download():
    # use saved configuration / optional query param
    custom_url = (request.args.get("url") or request.form.get("url") or "").strip()
    if custom_url:
        scrape_pdfs(base_url=custom_url)
    else:
        scrape_pdfs()
    create_zip_archive()
    return redirect(url_for("report"))

@APP.route("/report")
def report():
    current_url = load_base_url()

    # Build file list from disk
    files = []
    for f in DATA_DIR.glob("*.pdf"):
        files.append({
            "file": f.name,
            "size": f"{f.stat().st_size/1024:.1f} KB",
            "timestamp": datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        })
    files.sort(key=lambda x: x["timestamp"], reverse=True)

    zip_exists = (DATA_DIR / ZIP_NAME).exists()

    # Recent log
    log_content = ""
    if os.path.exists(SCRAPE_LOG):
        with open(SCRAPE_LOG, "r", encoding="utf-8") as f:
            log_lines = f.readlines()
            log_content = "".join(log_lines[-200:])

    html = """
    <html>
    <head>
        <title>Cayman Judicial PDF Report</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            table { border-collapse: collapse; width: 100%; margin-top: 16px; font-size: 13px; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #4CAF50; color: white; position: sticky; top: 0; }
            tr:nth-child(even) { background-color: #f9f9f9; }
            .button { background:#4CAF50;color:#fff;padding:8px 14px;border:none;border-radius:4px;cursor:pointer; text-decoration:none; }
            pre { background:#111; color:#0f0; padding:12px; max-height: 400px; overflow: auto; }
        </style>
    </head>
    <body>
        <h1>📊 Cayman Judicial PDF Report</h1>

        <p>
            <a class="button" href="{{ url_for('index') }}">🏠 Home</a>
            {% if zip_exists %}
            <a class="button" href="{{ url_for('download_file', filename=zip_name) }}">📦 Download All as ZIP</a>
            {% endif %}
            <a class="button" style="background:#FF9800;" href="{{ url_for('export_csv') }}">📊 Export Metadata (CSV)</a>
            <a class="button" href="{{ url_for('run_download') }}" onclick="return confirm('Run scraper again?')">🔄 Run Scraper Again</a>
        </p>

        <div>
            <strong>Current Target:</strong>
            <a href="{{ cloak_url(current_url) }}" target="_blank" rel="noopener">{{ current_url }}</a><br>
            <strong>CSV Source:</strong>
            <a href="{{ cloak_url(csv_url) }}" target="_blank" rel="noopener">{{ csv_url }}</a><br>
            <strong>Total PDFs:</strong> {{ files|length }}
        </div>

        <h2>📁 Downloaded PDFs</h2>
        {% if files %}
        <table>
            <thead>
                <tr>
                    <th>Filename</th><th>Size</th><th>Downloaded</th><th>Actions</th>
                </tr>
            </thead>
            <tbody>
                {% for f in files %}
                <tr>
                    <td style="font-family:monospace;">{{ f.file }}</td>
                    <td>{{ f.size }}</td>
                    <td>{{ f.timestamp }}</td>
                    <td><a href="{{ url_for('download_file', filename=f.file) }}">Download</a></td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        {% else %}
        <p>No PDFs downloaded yet. Click “Run Scraper Again”.</p>
        {% endif %}

        <h2>📜 Recent Log (Last 200 lines)</h2>
        <pre>{{ log_content or "No log entries yet." }}</pre>
    </body>
    </html>
    """
    return render_template_string(
        html,
        files=files,
        zip_exists=zip_exists,
        zip_name=ZIP_NAME,
        log_content=log_content,
        current_url=current_url,
        csv_url=CSV_URL,
        cloak_url=cloak_url,
    )

@APP.route("/files/<path:filename>")
def download_file(filename: str):
    try:
        return send_from_directory(str(DATA_DIR), filename, as_attachment=True)
    except Exception as e:
        return f"File not found: {e}", 404

@APP.route("/api/metadata")
def api_metadata():
    return jsonify(load_metadata())

@APP.route("/export/csv")
def export_csv():
    metadata = load_metadata()
    if not metadata:
        return "No metadata available", 404

    # Fixed stable field order
    fieldnames = [
        "neutral_citation", "cause_number", "judgment_date", "title",
        "subject", "court", "category", "file", "status", "timestamp", "identifier"
    ]

    si = StringIO()
    writer = csv.DictWriter(si, fieldnames=fieldnames)
    writer.writeheader()
    for item in metadata:
        row = {k: item.get(k, "") for k in fieldnames}
        writer.writerow(row)

    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=judgments_metadata.csv"
    output.headers["Content-type"] = "text/csv"
    return output

# Expose both APP and app for different Procfile styles
app = APP

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    APP.run(host="0.0.0.0", port=port, debug=False)
