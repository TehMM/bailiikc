"""
Cayman Judicial PDF Scraper & Web Dashboard (Selenium-enabled)

- Renders the Cayman Judicial page in headless Chrome to get the *live* AJAX nonce.
- Copies Selenium cookies into requests.Session so admin-ajax.php accepts our calls.
- Scrapes the official CSV, resolves Box URLs via the AJAX endpoint, and downloads PDFs.
- Includes a web UI: Home (configure + triggers), Report (PDFs + FULL logs + exports).

Environment variables (optional):
  PORT                -> Flask port (default 5000 on Docker; Railway injects PORT)
  DATA_DIR            -> Where to store PDFs & logs (default: /app/data/pdfs)
  MAX_DOWNLOADS       -> Cap per run to avoid timeouts (default: 25)
  PAGE_WAIT_SECONDS   -> Wait time to let JS finish (default: 15)
  BASE_URL            -> Nonce page (default: https://judicial.ky/judgments/unreported-judgments/)
  CSV_URL             -> Judgments CSV (default: https://judicial.ky/wp-content/uploads/box_files/judgments.csv)
"""

from __future__ import annotations

import csv
import os
import re
import time
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlparse, parse_qs
from zipfile import ZipFile

import requests
from bs4 import BeautifulSoup
from flask import (
    Flask,
    jsonify,
    make_response,
    redirect,
    render_template_string,
    request,
    send_from_directory,
    url_for,
)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ----------------------------- Config --------------------------------

APP = Flask(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data/pdfs")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_BASE_URL = os.environ.get(
    "BASE_URL", "https://judicial.ky/judgments/unreported-judgments/"
)
CSV_URL = os.environ.get(
    "CSV_URL", "https://judicial.ky/wp-content/uploads/box_files/judgments.csv"
)
AJAX_URL = "https://judicial.ky/wp-admin/admin-ajax.php"
ZIP_NAME = "all_pdfs.zip"

SCRAPE_LOG = DATA_DIR / "scrape_log.txt"
SCRAPED_URLS_FILE = DATA_DIR / "scraped_urls.txt"
CONFIG_FILE = DATA_DIR / "config.txt"
METADATA_FILE = DATA_DIR / "metadata.json"

MAX_DOWNLOADS = int(os.environ.get("MAX_DOWNLOADS", "25"))
PAGE_WAIT_SECONDS = int(os.environ.get("PAGE_WAIT_SECONDS", "15"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://judicial.ky",
    "Referer": DEFAULT_BASE_URL,
    "Connection": "keep-alive",
}

# --------------------------- Utilities -------------------------------

def log_message(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)
    with open(SCRAPE_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def load_base_url() -> str:
    if CONFIG_FILE.exists():
        try:
            val = CONFIG_FILE.read_text(encoding="utf-8").strip()
            if val:
                return val
        except Exception:
            pass
    return DEFAULT_BASE_URL


def save_base_url(url: str) -> None:
    CONFIG_FILE.write_text(url.strip(), encoding="utf-8")
    log_message(f"Configuration updated: Base URL changed to {url}")


def load_scraped_ids() -> set[str]:
    if SCRAPED_URLS_FILE.exists():
        return set(
            ln.strip()
            for ln in SCRAPED_URLS_FILE.read_text(encoding="utf-8").splitlines()
            if ln.strip()
        )
    return set()


def save_scraped_id(identifier: str) -> None:
    with open(SCRAPED_URLS_FILE, "a", encoding="utf-8") as f:
        f.write(identifier + "\n")


def save_metadata(rows: list[dict]) -> None:
    try:
        if METADATA_FILE.exists():
            existing = []
            try:
                import json
                existing = json.loads(METADATA_FILE.read_text(encoding="utf-8"))
            except Exception:
                existing = []
            existing.extend(rows)
            METADATA_FILE.write_text(
                __import__("json").dumps(existing, indent=2), encoding="utf-8"
            )
        else:
            METADATA_FILE.write_text(
                __import__("json").dumps(rows, indent=2), encoding="utf-8"
            )
    except Exception as e:
        log_message(f"✗ save_metadata error: {e}")


def load_metadata() -> list[dict]:
    try:
        if METADATA_FILE.exists():
            return __import__("json").loads(METADATA_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return []


def sanitize_filename(name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    return safe.strip("._") or "file"


# ------------------------- Selenium bits -----------------------------

def fetch_live_nonce_and_cookies(base_url: str) -> tuple[Optional[str], list[dict]]:
    """
    Render the page in headless Chrome and extract the live 'security' token
    plus the cookies (so we can reuse the exact session in requests).
    """
    log_message("Launching headless Chrome to fetch live security nonce…")
    options = Options()
    # On Debian slim with chromium & chromedriver installed in Dockerfile:
    options.binary_location = "/usr/bin/chromium"
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    driver.get(base_url)

    # Let scripts (jQuery, localized data, etc.) fully initialize
    time.sleep(PAGE_WAIT_SECONDS)

    html = driver.page_source

    # Try multiple patterns: JSON blocks, var security, etc.
    nonce = None
    for pat in [
        r'"security"\s*:\s*"([a-f0-9]{10})"',
        r'"_nonce"\s*:\s*"([a-f0-9]{10})"',
        r"var\s+security\s*=\s*['\"]([a-f0-9]{10})['\"]",
    ]:
        m = re.search(pat, html)
        if m:
            nonce = m.group(1)
            break

    if nonce:
        log_message(f"✓ Extracted live security nonce: {nonce}")
    else:
        log_message("✗ Could not find live security nonce in rendered HTML")

    cookies = driver.get_cookies()
    if cookies:
        ck_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
        log_message(f"Selenium cookies: {ck_str}")

    driver.quit()
    return nonce, cookies or []


def session_with_cookies(cookies: list[dict]) -> requests.Session:
    """
    Create a requests.Session populated with cookies from Selenium.
    """
    sess = requests.Session()
    sess.headers.update(HEADERS)
    for c in cookies:
        # Only set name/value; domain scoping is handled by requests
        sess.cookies.set(c.get("name"), c.get("value"))
    return sess


# ---------------------- Scraper core functions -----------------------

def fetch_csv_entries(session: requests.Session) -> list[dict]:
    log_message(f"Fetching CSV from: {CSV_URL}")
    try:
        resp = session.get(CSV_URL, timeout=30)
        resp.raise_for_status()
        reader = csv.DictReader(StringIO(resp.text))
        entries = []
        for row in reader:
            if not row.get("Actions"):
                continue
            if "criminal" in row.get("Category", "").lower():
                continue
            entries.append(
                {
                    "neutral_citation": row.get("Neutral Citation", ""),
                    "cause_number": row.get("Cause Number", ""),
                    "judgment_date": row.get("Judgment Date", ""),
                    "title": row.get("Title", ""),
                    "subject": row.get("Subject", ""),
                    "court": row.get("Court", ""),
                    "category": row.get("Category", ""),
                    "actions": row.get("Actions", ""),
                }
            )
        log_message(f"Loaded {len(entries)} non-criminal cases from CSV")
        return entries
    except Exception as e:
        log_message(f"✗ CSV fetch error: {e}")
        return []


def parse_actions_field(actions_value: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract 'fid' and 'fname' from the 'Actions' HTML cell.
    """
    if not actions_value:
        return None, None

    soup = BeautifulSoup(actions_value, "html.parser")
    link = soup.find("a")
    if not link:
        return None, None

    fid = link.get("data-fid") or link.get("data-id") or None
    fname = link.get("data-fname") or link.get("data-name") or None

    # Try querystring if not present in data-attrs
    if (not fid or not fname) and link.get("href"):
        qs = parse_qs(urlparse(link["href"]).query)
        if not fid:
            fid = (qs.get("fid") or qs.get("file") or qs.get("id") or [None])[0]
        if not fname:
            fname = (qs.get("fname") or qs.get("name") or qs.get("file_name") or [None])[0]

    # Fallback: text label
    if not fname:
        label = link.get_text(strip=True)
        if label:
            fname = re.sub(r"\s+", "_", label)

    if fid:
        fid = str(fid).strip()
    if fname:
        fname = str(fname).strip()

    return fid or None, fname or None


def get_box_url(fid: str, fname: str, security: str, session: requests.Session) -> Optional[str]:
    """
    Call the site's AJAX endpoint to resolve a temporary Box.com download URL.
    """
    payload = {"action": "dl_bfile", "fid": fid, "fname": fname or fid, "security": security}
    try:
        # Ensure we have at least the site cookie too (sometimes needed)
        if "pdb-sess" not in session.cookies:
            session.get(load_base_url(), timeout=10)

        cookie_str = "; ".join(f"{c.name}={c.value}" for c in session.cookies)
        log_message(f"Using cookies: {cookie_str}")
        log_message(f"  Payload: {payload}")

        r = session.post(AJAX_URL, data=payload, headers=HEADERS, timeout=30)
        if r.status_code == 403:
            log_message("✗ 403 Forbidden (nonce/cookie mismatch)")
            return None

        # Some hosts gzip JSON; requests handles it. Try JSON parse:
        data = {}
        try:
            data = r.json()
        except Exception:
            # If not JSON, log a small snippet for debugging
            snippet = r.text[:300].replace("\n", " ")
            log_message(f"✗ Non-JSON AJAX response (first 300 chars): {snippet}")
            return None

        if data.get("success") and isinstance(data.get("data"), dict):
            url = data["data"].get("fid") or data["data"].get("url")
            if url and "boxcloud.com" in url:
                log_message(f"✓ Resolved Box URL for fid={fid}")
                return url

        log_message(f"✗ AJAX failed or no URL in response: {data}")
        # Save debug JSON
        debug_path = DATA_DIR / f"ajax_debug_{fid}.json"
        debug_path.write_text(__import__("json").dumps(data, indent=2), encoding="utf-8")
        return None

    except Exception as e:
        log_message(f"✗ AJAX exception: {e}")
        return None


def download_pdf(session: requests.Session, url: str, destination: Path) -> bool:
    try:
        r = session.get(url, stream=True, timeout=60)
        r.raise_for_status()
        # quick sanity: PDF header
        content = r.content
        if not content.startswith(b"%PDF"):
            log_message("✗ Downloaded file is not a PDF")
            return False
        destination.write_bytes(content)
        log_message(f"✓ Saved: {destination.name} ({len(content)/1024:.1f} KB)")
        return True
    except Exception as e:
        log_message(f"✗ Download error: {e}")
        return False


def create_zip() -> Path:
    zip_path = DATA_DIR / ZIP_NAME
    with ZipFile(zip_path, "w") as z:
        count = 0
        for f in DATA_DIR.glob("*.pdf"):
            z.write(f, f.name)
            count += 1
    log_message(f"Created ZIP with {count} PDFs")
    return zip_path


# ------------------------- Orchestrator ------------------------------

def scrape_all(limit: Optional[int] = None) -> None:
    log_message("=" * 60)
    log_message("Starting new scrape session")

    base_url = load_base_url()
    log_message(f"Target URL: {base_url}")

    # 1) Selenium: get live nonce + cookies
    security, se_cookies = fetch_live_nonce_and_cookies(base_url)
    if not security:
        log_message("✗ No valid security nonce, aborting scrape.")
        return

    # 2) Transfer cookies into requests session
    session = session_with_cookies(se_cookies)

    # 3) Load entries from CSV
    entries = fetch_csv_entries(session)
    if not entries:
        log_message("No cases found in CSV.")
        return

    scraped_ids = load_scraped_ids()
    new_entries = [e for e in entries if e["actions"] not in scraped_ids]
    total = len(new_entries)
    if total == 0:
        log_message("Everything up to date; nothing to download.")
        return

    cap = limit or MAX_DOWNLOADS
    if cap and total > cap:
        log_message(f"Will download {cap} of {total} entries (cap active).")
        new_entries = new_entries[:cap]
    else:
        log_message(f"Will download {total} new PDFs")

    results_batch: list[dict] = []

    for idx, entry in enumerate(new_entries, 1):
        fid, fname = parse_actions_field(entry["actions"])
        if not fid:
            log_message(f"[{idx}/{len(new_entries)}] ✗ Could not parse FID; skipping")
            continue

        safe_name = sanitize_filename(fname or fid)
        pdf_path = DATA_DIR / f"{safe_name}.pdf"

        log_message(
            f"[{idx}/{len(new_entries)}] {entry['title'] or 'Untitled'} "
            f"(fid={fid}, fname={safe_name})"
        )

        if pdf_path.exists():
            log_message(f"  ✓ Already have: {pdf_path.name}")
            status = "EXISTS"
        else:
            # Resolve Box URL via AJAX
            box_url = get_box_url(fid, fname or fid, security, session)
            if not box_url:
                log_message("  ✗ Could not get download URL; skipping")
                status = "FAILED_RESOLVE"
            else:
                ok = download_pdf(session, box_url, pdf_path)
                status = "DOWNLOADED" if ok else "FAILED_DOWNLOAD"

        # record metadata row
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = [
            {
                "neutral_citation": entry["neutral_citation"],
                "cause_number": entry["cause_number"],
                "judgment_date": entry["judgment_date"],
                "title": entry["title"],
                "subject": entry["subject"],
                "court": entry["court"],
                "category": entry["category"],
                "file": pdf_path.name,
                "status": status,
                "timestamp": now,
                "identifier": fid,
            }
        ]
        results_batch.extend(rows)
        save_scraped_id(entry["actions"])

        # politeness delay
        time.sleep(1)

    if results_batch:
        save_metadata(results_batch)
    create_zip()
    log_message("Scrape complete")
    log_message("=" * 60)


# ---------------------------- Web UI --------------------------------

HOME_HTML = """
<!doctype html>
<html>
<head>
  <title>Cayman Judicial PDF Scraper</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 32px; max-width: 980px; }
    .box { background: #f7f9fc; padding: 16px; border-left: 4px solid #0d6efd; margin-bottom: 16px; }
    .btn { display: inline-block; padding: 10px 16px; background: #2e7d32; color: #fff; text-decoration: none; border-radius: 6px; }
    .btn:hover { background: #1b5e20; }
    .btn-secondary { background: #0d6efd; }
    label { display:block; margin:8px 0 4px; font-weight: bold; }
    input[type="url"], input[type="number"] { width: 100%; padding: 8px; box-sizing: border-box; }
    .row { display:flex; gap: 12px; }
    .row > div { flex:1; }
    code { background: #eef; padding:2px 4px; border-radius: 3px; }
  </style>
</head>
<body>
  <h1>🏛️ Cayman Judicial PDF Scraper</h1>

  <div class="box">
    <p><strong>How it works</strong> — This app renders the Cayman site in headless Chrome to obtain the
    live WordPress AJAX <code>security</code> key, reuses the same cookies for requests, and downloads
    PDFs listed in the official CSV.</p>
  </div>

  <form action="{{ url_for('update_config') }}" method="post" class="box">
    <label>Base URL (for nonce):</label>
    <input type="url" name="base_url" value="{{ base_url }}" required>
    <div class="row">
      <div>
        <label>Max downloads per run (cap):</label>
        <input type="number" name="limit" value="{{ max_downloads }}" min="1" step="1">
      </div>
      <div>
        <label>JS wait seconds (Selenium):</label>
        <input type="number" name="wait" value="{{ wait_seconds }}" min="5" step="1">
      </div>
    </div>
    <p style="margin-top:12px;">
      <button class="btn btn-secondary" type="submit">💾 Save Config</button>
      <a class="btn" href="{{ url_for('run_scrape') }}">▶️ Run Scraper Now</a>
      <a class="btn btn-secondary" href="{{ url_for('report') }}">📊 View Report</a>
    </p>
  </form>

  <div class="box">
    <p><strong>Data directory:</strong> <code>{{ data_dir }}</code></p>
    <p><strong>CSV source:</strong> <a target="_blank" href="{{ csv_url }}">{{ csv_url }}</a></p>
    <p><strong>AJAX endpoint:</strong> <code>{{ ajax_url }}</code></p>
    <p><strong>ZIP file:</strong> <a href="{{ url_for('download_file', filename=zip_name) }}">{{ zip_name }}</a></p>
  </div>
</body>
</html>
"""

REPORT_HTML = """
<!doctype html>
<html>
<head>
  <title>Cayman Judicial PDF Report</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 32px; max-width: 1200px; }
    .btn { display: inline-block; padding: 10px 16px; background: #0d6efd; color: #fff; text-decoration: none; border-radius: 6px; }
    .btn:hover { background: #0b5ed7; }
    .btn-green { background: #2e7d32; }
    .box { background: #f7f9fc; padding: 16px; border-left: 4px solid #0d6efd; margin-bottom: 16px; }
    table { width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 13px; }
    th, td { padding: 8px; border: 1px solid #ddd; text-align: left; }
    th { background: #e3f2fd; position: sticky; top: 0; }
    .log { white-space: pre-wrap; font-family: monospace; background: #111; color: #ddd; padding: 12px; border-radius: 6px; max-height: 600px; overflow-y: auto; }
    .row { display:flex; gap:12px; flex-wrap:wrap; align-items:center;}
    .filename { font-family: monospace; font-size: 12px; }
  </style>
</head>
<body>
  <h1>📊 PDF Report & Logs</h1>

  <div class="row">
    <a class="btn btn-green" href="{{ url_for('run_scrape') }}">🔄 Run Scraper Again</a>
    <a class="btn" href="{{ url_for('download_file', filename=zip_name) }}">📦 Download All as ZIP</a>
    <a class="btn" href="{{ url_for('export_csv') }}">📑 Export Metadata (CSV)</a>
    <a class="btn" href="{{ url_for('home') }}">🏠 Home</a>
  </div>

  <div class="box">
    <p><strong>Stats</strong></p>
    <p>Total PDFs: <strong>{{ files|length }}</strong></p>
    <p>Data directory: <code>{{ data_dir }}</code></p>
    <p>CSV source: <a target="_blank" href="{{ csv_url }}">{{ csv_url }}</a></p>
  </div>

  <h2>📁 Downloaded PDFs</h2>
  {% if files %}
  <table>
    <thead>
      <tr>
        <th>Filename</th><th>Size</th><th>Modified</th><th>Action</th>
      </tr>
    </thead>
    <tbody>
      {% for f in files %}
      <tr>
        <td class="filename">{{ f.name }}</td>
        <td>{{ f.size }}</td>
        <td>{{ f.timestamp }}</td>
        <td><a href="{{ url_for('download_file', filename=f.name) }}">Download</a></td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  {% else %}
  <p>No PDFs yet.</p>
  {% endif %}

  <h2>🗒️ Full Log</h2>
  <div class="box">
    <form method="get">
      <label>Show last N lines (0 = full):</label>
      <input type="number" name="tail" min="0" value="{{ tail }}">
      <button class="btn" type="submit">Update</button>
    </form>
  </div>
  <div class="log">{{ log_content }}</div>
</body>
</html>
"""


# ---------------------------- Flask routes ---------------------------

@APP.route("/")
def home():
    return render_template_string(
        HOME_HTML,
        base_url=load_base_url(),
        max_downloads=MAX_DOWNLOADS,
        wait_seconds=PAGE_WAIT_SECONDS,
        data_dir=str(DATA_DIR),
        csv_url=CSV_URL,
        ajax_url=AJAX_URL,
        zip_name=ZIP_NAME,
    )


@APP.route("/config", methods=["POST"])
def update_config():
    new_url = request.form.get("base_url", "").strip()
    limit = request.form.get("limit", "").strip()
    wait = request.form.get("wait", "").strip()

    if new_url:
        save_base_url(new_url)

    global MAX_DOWNLOADS, PAGE_WAIT_SECONDS
    if limit.isdigit() and int(limit) > 0:
        MAX_DOWNLOADS = int(limit)
        log_message(f"Max downloads per run set to {MAX_DOWNLOADS}")
    if wait.isdigit() and int(wait) >= 5:
        PAGE_WAIT_SECONDS = int(wait)
        log_message(f"Page wait seconds set to {PAGE_WAIT_SECONDS}")

    return redirect(url_for("home"))


@APP.route("/scrape")
def run_scrape():
    # Allow optional ?limit=NN per-call override
    lim = request.args.get("limit")
    limit_val = None
    if lim and lim.isdigit():
        limit_val = int(lim)
    scrape_all(limit_val)
    return redirect(url_for("report"))


@APP.route("/report")
def report():
    files = []
    for f in DATA_DIR.glob("*.pdf"):
        files.append(
            {
                "name": f.name,
                "size": f"{f.stat().st_size / 1024:.1f} KB",
                "timestamp": datetime.fromtimestamp(f.stat().st_mtime).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            }
        )
    files.sort(key=lambda x: x["timestamp"], reverse=True)

    # FULL logs; tail=N (0 or blank = entire file)
    tail = request.args.get("tail", "0")
    log_content = ""
    if SCRAPE_LOG.exists():
        lines = SCRAPE_LOG.read_text(encoding="utf-8", errors="ignore").splitlines()
        try:
            tail_n = int(tail)
        except Exception:
            tail_n = 0
        if tail_n and tail_n > 0:
            lines = lines[-tail_n:]
        log_content = "\n".join(lines)
    else:
        log_content = "No logs yet."

    return render_template_string(
        REPORT_HTML,
        files=files,
        data_dir=str(DATA_DIR),
        csv_url=CSV_URL,
        zip_name=ZIP_NAME,
        tail=tail,
        log_content=log_content,
    )


@APP.route("/download/<path:filename>")
def download_file(filename):
    try:
        return send_from_directory(DATA_DIR, filename, as_attachment=True)
    except Exception as e:
        return f"File not found: {e}", 404


@APP.route("/export/csv")
def export_csv():
    meta = load_metadata()
    if not meta:
        return "No metadata available", 404

    # stream CSV from metadata
    out = StringIO()
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
        "identifier",
    ]
    w = csv.DictWriter(out, fieldnames=fieldnames)
    w.writeheader()
    for row in meta:
        w.writerow({k: row.get(k, "") for k in fieldnames})

    resp = make_response(out.getvalue())
    resp.headers["Content-Disposition"] = "attachment; filename=judgments_metadata.csv"
    resp.headers["Content-type"] = "text/csv"
    return resp


# ---------------------------- Entrypoint -----------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    APP.run(host="0.0.0.0", port=port)
