"""
Cayman Judicial PDF Scraper with dynamic nonce discovery (Selenium)
Fully Railway-compatible Flask app
"""
import os
import re
import time
import json
import csv
from datetime import datetime
from pathlib import Path
from io import StringIO
from zipfile import ZipFile
from urllib.parse import urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, send_from_directory
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
APP = Flask(__name__)
DATA_DIR = Path("./data/pdfs")
DATA_DIR.mkdir(parents=True, exist_ok=True)

CSV_URL = "https://judicial.ky/wp-content/uploads/box_files/judgments.csv"
BASE_URL = "https://judicial.ky/judgments/unreported-judgments/"
AJAX_URL = "https://judicial.ky/wp-admin/admin-ajax.php"
ZIP_NAME = "all_pdfs.zip"
SCRAPE_LOG = DATA_DIR / "scrape_log.txt"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://judicial.ky",
    "Referer": BASE_URL,
    "Connection": "keep-alive",
}


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def log_message(msg: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(SCRAPE_LOG, "a") as f:
        f.write(line + "\n")


def fetch_real_nonce() -> str | None:
    """Render the page with headless Chrome and extract live 'security' nonce."""
    log_message("Launching headless Chrome to fetch live security nonce…")
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    driver.get(BASE_URL)
    time.sleep(5)  # Allow JS to execute

    html = driver.page_source
    driver.quit()

    m = re.search(r'"security"\s*:\s*"([a-f0-9]{10})"', html)
    if not m:
        m = re.search(r"var\s+security\s*=\s*['\"]([a-f0-9]{10})['\"]", html)
    if m:
        nonce = m.group(1)
        log_message(f"✓ Extracted live security nonce: {nonce}")
        return nonce
    log_message("✗ Could not find live security nonce in rendered HTML")
    return None


def fetch_csv_data(session: requests.Session) -> list[dict]:
    log_message(f"Fetching CSV from: {CSV_URL}")
    try:
        resp = session.get(CSV_URL, timeout=30)
        resp.raise_for_status()
        reader = csv.DictReader(StringIO(resp.text))
        entries = [
            row for row in reader
            if row.get("Actions") and "criminal" not in row.get("Category", "").lower()
        ]
        log_message(f"Loaded {len(entries)} non-criminal cases from CSV")
        return entries
    except Exception as e:
        log_message(f"✗ CSV fetch error: {e}")
        return []


def parse_actions_field(actions_value: str):
    """Extract fid and fname from Actions HTML."""
    if not actions_value:
        return None, None
    soup = BeautifulSoup(actions_value, "html.parser")
    link = soup.find("a")
    if not link:
        return None, None
    fid = link.get("data-fid") or link.get("data-id") or None
    fname = link.get("data-fname") or link.get("data-name") or None
    if not fid and link.get("href"):
        q = parse_qs(urlparse(link["href"]).query)
        fid = q.get("fid", [None])[0]
    if not fname and link.get_text(strip=True):
        fname = re.sub(r"\s+", "_", link.get_text(strip=True))
    return fid, fname


def get_box_url(fid: str, fname: str, security: str, session: requests.Session) -> str | None:
    """Emulate browser AJAX call to get real Box.com link."""
    payload = {"action": "dl_bfile", "fid": fid, "fname": fname, "security": security}
    headers = HEADERS.copy()
    try:
        if "pdb-sess" not in session.cookies:
            session.get(BASE_URL, timeout=10)
        cookie_str = "; ".join(f"{c.name}={c.value}" for c in session.cookies)
        log_message(f"Using cookies: {cookie_str}")
        r = session.post(AJAX_URL, data=payload, headers=headers, timeout=20)
        if r.status_code == 403:
            log_message(f"✗ 403 Forbidden (nonce/cookie mismatch)")
            log_message(f"  Payload: {payload}")
            return None
        data = r.json()
        if data.get("success"):
            box_url = data["data"].get("fid")
            if box_url and "boxcloud.com" in box_url:
                log_message(f"✓ Got valid Box URL for {fid}")
                return box_url
        log_message(f"✗ AJAX failed: {data}")
        return None
    except Exception as e:
        log_message(f"✗ AJAX exception: {e}")
        return None


def download_pdf(session: requests.Session, url: str, path: Path) -> bool:
    try:
        r = session.get(url, stream=True, timeout=60)
        r.raise_for_status()
        if not r.content.startswith(b"%PDF"):
            log_message("✗ File is not a PDF")
            return False
        with open(path, "wb") as f:
            f.write(r.content)
        log_message(f"✓ Saved PDF: {path.name} ({len(r.content)/1024:.1f} KB)")
        return True
    except Exception as e:
        log_message(f"✗ Download error: {e}")
        return False


def create_zip_archive() -> Path:
    zip_path = DATA_DIR / ZIP_NAME
    with ZipFile(zip_path, "w") as z:
        for f in DATA_DIR.glob("*.pdf"):
            z.write(f, f.name)
    log_message(f"Created ZIP with {len(list(DATA_DIR.glob('*.pdf')))} PDFs")
    return zip_path


# ---------------------------------------------------------------------
# Main Scrape
# ---------------------------------------------------------------------
def scrape_all():
    log_message("=" * 60)
    log_message("Starting new scrape session")

    session = requests.Session()
    session.headers.update(HEADERS)

    # Dynamic nonce
    security = fetch_real_nonce()
    if not security:
        log_message("✗ No valid security nonce, aborting scrape.")
        return

    csv_rows = fetch_csv_data(session)
    if not csv_rows:
        log_message("No cases found.")
        return

    for idx, entry in enumerate(csv_rows[:10], start=1):  # limit to 10 for testing
        fid, fname = parse_actions_field(entry["Actions"])
        if not fid:
            continue
        log_message(f"[{idx}/{len(csv_rows)}] {entry['Title']} (fid={fid})")
        box_url = get_box_url(fid, fname, security, session)
        if not box_url:
            continue
        pdf_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", fname or fid) + ".pdf"
        pdf_path = DATA_DIR / pdf_name
        download_pdf(session, box_url, pdf_path)
        time.sleep(1)

    create_zip_archive()
    log_message("Scrape complete")


# ---------------------------------------------------------------------
# Flask Routes
# ---------------------------------------------------------------------
@APP.route("/")
def home():
    return jsonify({"status": "running", "message": "Visit /scrape to start scraping"})


@APP.route("/scrape")
def run_scrape():
    scrape_all()
    return jsonify({"status": "done"})


@APP.route("/download/<path:filename>")
def download(filename):
    return send_from_directory(DATA_DIR, filename, as_attachment=True)


@APP.route("/report")
def report():
    pdfs = [f.name for f in DATA_DIR.glob("*.pdf")]
    return jsonify({"pdfs": pdfs})


if __name__ == "__main__":
    APP.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
