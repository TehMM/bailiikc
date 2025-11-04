import os
import re
import time
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from flask import Flask, render_template_string, send_from_directory, redirect, url_for
from zipfile import ZipFile
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__)

# Config
DATA_DIR = "./data/pdfs"
ZIP_NAME = "judicialky_pdfs.zip"
SCRAPE_LOG = os.path.join(DATA_DIR, "scrape_log.txt")
SCRAPED_IDS_FILE = os.path.join(DATA_DIR, "scraped_ids.txt")

os.makedirs(DATA_DIR, exist_ok=True)

# Headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0'
}

# Retry strategy for requests
def get_with_retry(session, url, retries=3, delay=2, timeout=20, **kwargs):
    """Perform GET request with retry and exponential backoff."""
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, timeout=timeout, **kwargs)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == retries:
                raise
            print(f"[Retry {attempt}/{retries}] Error fetching {url}: {e}. Retrying in {delay}s...")
            time.sleep(delay)
            delay *= 2  # exponential backoff

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] {message}\n"
    print(entry.strip())
    with open(SCRAPE_LOG, "a") as f:
        f.write(entry)

def load_scraped_ids():
    if os.path.exists(SCRAPED_IDS_FILE):
        with open(SCRAPED_IDS_FILE, "r") as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def save_scraped_id(identifier):
    with open(SCRAPED_IDS_FILE, "a") as f:
        f.write(f"{identifier}\n")

def extract_security_nonce(soup):
    """Extract WordPress security nonce from inline JS or data attributes."""
    scripts = soup.find_all("script")

    for script in scripts:
        if script.string and "security" in script.string.lower():
            # Pattern 1: security: "nonce"
            matches = re.findall(r'security["\s:=]+(["\']?)([a-f0-9]{10})(["\']?)', script.string, re.IGNORECASE)
            if matches:
                log_message(f"  Found nonce (pattern 1): {matches[0][1]}")
                return matches[0][1]
            # Pattern 2: "security":"nonce"
            matches = re.findall(r'"security"\s*:\s*"([a-f0-9]{10})"', script.string)
            if matches:
                log_message(f"  Found nonce (pattern 2): {matches[0]}")
                return matches[0]
            # Pattern 3: var security = "nonce"
            matches = re.findall(r'var\s+security\s*=\s*["\']([a-f0-9]{10})["\']', script.string)
            if matches:
                log_message(f"  Found nonce (pattern 3): {matches[0]}")
                return matches[0]

    # Look for data-security attribute
    buttons = soup.find_all("button", {"data-security": True})
    if buttons:
        nonce = buttons[0].get("data-security")
        log_message(f"  Found nonce in data attribute: {nonce}")
        return nonce

    # Fallback: any 10-char hex string in inline JS
    for script in scripts:
        if script.string:
            hex_matches = re.findall(r'\b([a-f0-9]{10})\b', script.string.lower())
            if hex_matches:
                log_message(f"  Found possible nonce (hex pattern): {hex_matches[0]}")
                return hex_matches[0]

    return None

def fetch_datatable_data(ajax_url, session, length=2740):
    """Fetch all data from DataTables AJAX endpoint."""
    params = {
        'draw': 1,
        'start': 0,
        'length': length,
        'search[value]': '',
        'search[regex]': 'false',
        'order[0][column]': 2,
        'order[0][dir]': 'desc'
    }

    try:
        response = get_with_retry(session, ajax_url, params=params)
        data = response.json()
        if 'data' in data:
            log_message(f"  Retrieved {len(data['data'])} records from DataTables API")
            return data['data']
        log_message(f"  No 'data' field in response: {list(data.keys())}")
        return []
    except Exception as e:
        log_message(f"  Error fetching DataTable data: {e}")
        return []

def scrape_pdfs():
    """Main scraper function for judicial.ky"""
    session = requests.Session()
    session.headers.update(HEADERS)
    base_url = "https://judicial.ky/judgments/unreported-judgments/"
    ajax_url = "https://judicial.ky/wp-admin/admin-ajax.php"

    log_message("=" * 60)
    log_message("Starting scrape session for judicial.ky")

    scraped_ids = load_scraped_ids()
    log_message(f"Loaded {len(scraped_ids)} previously scraped IDs")

    try:
        main_page = get_with_retry(session, base_url)
        soup = BeautifulSoup(main_page.text, "html.parser")

        # Find nonce
        nonce = extract_security_nonce(soup)
        if not nonce:
            log_message("✗ Could not find security nonce.")
            return []

        data = fetch_datatable_data(ajax_url, session)
        if not data:
            log_message("✗ No data returned from AJAX endpoint.")
            return []

        new_pdfs = []
        for entry in data:
            # Each entry is likely a dict: {"id":..., "title":..., "pdf":...}
            case_id = str(entry.get("id", "")).strip()
            title = entry.get("title", "Untitled Case")
            pdf_url = entry.get("pdf", "")

            if not pdf_url or not pdf_url.lower().endswith(".pdf"):
                continue

            if case_id in scraped_ids:
                continue

            filename = f"{case_id}.pdf"
            filepath = os.path.join(DATA_DIR, filename)

            try:
                log_message(f"↓ Downloading PDF for case {case_id}: {pdf_url}")
                pdf_r = get_with_retry(session, pdf_url, timeout=40)
                if pdf_r.content[:4] != b"%PDF":
                    log_message(f"✗ Not a valid PDF for {case_id}")
                    continue

                with open(filepath, "wb") as f:
                    f.write(pdf_r.content)

                log_message(f"✓ Saved {filename} ({len(pdf_r.content)} bytes)")
                save_scraped_id(case_id)
                new_pdfs.append(filename)
            except Exception as e:
                log_message(f"✗ Error downloading {pdf_url}: {e}")
                continue

        log_message(f"Scraping complete. New PDFs: {len(new_pdfs)}")
        return new_pdfs

    except Exception as e:
        log_message(f"SCRAPING ERROR: {e}")
        return []

def create_zip():
    """Bundle all PDFs into a ZIP archive."""
    zip_path = os.path.join(DATA_DIR, ZIP_NAME)
    try:
        with ZipFile(zip_path, "w") as zf:
            count = 0
            for file in os.listdir(DATA_DIR):
                if file.endswith(".pdf"):
                    zf.write(os.path.join(DATA_DIR, file), file)
                    count += 1
        log_message(f"Created ZIP with {count} PDFs")
        return zip_path
    except Exception as e:
        log_message(f"ZIP creation error: {e}")
        return None

@app.route("/")
def index():
    return redirect(url_for("report"))

@app.route("/run", methods=["GET", "POST"])
def run_scraper():
    scrape_pdfs()
    create_zip()
    return redirect(url_for("report"))

@app.route("/report")
def report():
    files = []
    for f in os.listdir(DATA_DIR):
        if f.endswith(".pdf"):
            files.append({
                "name": f,
                "timestamp": datetime.fromtimestamp(os.path.getmtime(os.path.join(DATA_DIR, f))).strftime("%Y-%m-%d %H:%M:%S"),
                "size": f"{os.path.getsize(os.path.join(DATA_DIR, f))/1024:.1f} KB"
            })

    files.sort(key=lambda x: x["timestamp"], reverse=True)
    zip_exists = os.path.exists(os.path.join(DATA_DIR, ZIP_NAME))

    log_content = ""
    if os.path.exists(SCRAPE_LOG):
        with open(SCRAPE_LOG, "r") as f:
            log_content = "".join(f.readlines()[-60:])

    html = """
    <html><head><title>Judicial.ky Scraper Report</title></head>
    <body style="font-family:Arial;margin:40px;">
        <h1>Judicial.ky Scraper Report</h1>
        <p><a href="{{ url_for('run_scraper') }}">▶️ Run Scraper Again</a></p>
        {% if zip_exists %}
            <p><a href="{{ url_for('download_file', filename=zip_name) }}">Download All as ZIP</a></p>
        {% endif %}
        <h2>Files</h2>
        {% if files %}
            <table border="1" cellspacing="0" cellpadding="6">
                <tr><th>File</th><th>Size</th><th>Timestamp</th></tr>
                {% for f in files %}
                    <tr>
                        <td><a href="{{ url_for('download_file', filename=f.name) }}">{{ f.name }}</a></td>
                        <td>{{ f.size }}</td>
                        <td>{{ f.timestamp }}</td>
                    </tr>
                {% endfor %}
            </table>
        {% else %}
            <p>No PDFs downloaded yet.</p>
        {% endif %}
        <h2>Recent Logs</h2>
        <pre style="background:#f4f4f4;padding:10px;border:1px solid #ccc;max-height:300px;overflow:auto;">{{ log_content }}</pre>
    </body></html>
    """
    return render_template_string(html, files=files, zip_exists=zip_exists, zip_name=ZIP_NAME, log_content=log_content)

@app.route("/files/<path:filename>")
def download_file(filename):
    return send_from_directory(DATA_DIR, filename, as_attachment=True)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
