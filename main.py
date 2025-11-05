"""
Cayman Judicial PDF Scraper & Web Dashboard (Live Logs Edition)

Adds:
- Real-time log streaming via Server-Sent Events (SSE)
- HTML-decoding fix for Actions column
"""

from __future__ import annotations
import csv, os, re, time
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlparse, parse_qs
from zipfile import ZipFile
from html import unescape
import requests
from bs4 import BeautifulSoup
from flask import (
    Flask, jsonify, make_response, redirect, render_template_string,
    request, send_from_directory, url_for, Response
)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ----------------------------- Config --------------------------------
APP = Flask(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data/pdfs")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_BASE_URL = os.environ.get("BASE_URL", "https://judicial.ky/judgments/unreported-judgments/")
CSV_URL = os.environ.get("CSV_URL", "https://judicial.ky/wp-content/uploads/box_files/judgments.csv")
AJAX_URL = "https://judicial.ky/wp-admin/admin-ajax.php"
ZIP_NAME = "all_pdfs.zip"
SCRAPE_LOG = DATA_DIR / "scrape_log.txt"
SCRAPED_URLS_FILE = DATA_DIR / "scraped_urls.txt"
CONFIG_FILE = DATA_DIR / "config.txt"
METADATA_FILE = DATA_DIR / "metadata.json"
MAX_DOWNLOADS = int(os.environ.get("MAX_DOWNLOADS", "25"))
PAGE_WAIT_SECONDS = int(os.environ.get("PAGE_WAIT_SECONDS", "15"))
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/142.0.0.0 Safari/537.36",
    "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest", "Origin": "https://judicial.ky",
    "Referer": DEFAULT_BASE_URL, "Connection": "keep-alive",
}

# --------------------------- Utilities -------------------------------
def log_message(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(SCRAPE_LOG, "a", encoding="utf-8") as f: f.write(line + "\n")

def load_base_url(): return CONFIG_FILE.read_text().strip() if CONFIG_FILE.exists() else DEFAULT_BASE_URL
def save_base_url(u): CONFIG_FILE.write_text(u.strip()); log_message(f"Base URL updated to {u}")
def load_scraped_ids(): return set(SCRAPED_URLS_FILE.read_text().splitlines()) if SCRAPED_URLS_FILE.exists() else set()
def save_scraped_id(x): open(SCRAPED_URLS_FILE, "a").write(x + "\n")
def sanitize_filename(name): return re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._") or "file"

# ------------------------- Selenium bits -----------------------------
def fetch_live_nonce_and_cookies(base_url):
    log_message("Launching headless Chrome to fetch live nonce…")
    options = Options()
    options.binary_location = "/usr/bin/chromium"
    options.add_argument("--headless=new"); options.add_argument("--no-sandbox"); options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    driver.get(base_url); time.sleep(PAGE_WAIT_SECONDS)
    html = driver.page_source
    nonce = None
    for pat in [r'"security"\s*:\s*"([a-f0-9]{10})"', r'"_nonce"\s*:\s*"([a-f0-9]{10})"', r"var\s+security\s*=\s*['\"]([a-f0-9]{10})['\"]"]:
        m = re.search(pat, html); 
        if m: nonce = m.group(1); break
    log_message(f"✓ Extracted nonce: {nonce}" if nonce else "✗ No nonce found")
    cookies = driver.get_cookies()
    if cookies: log_message("Selenium cookies: " + "; ".join(f"{c['name']}={c['value']}" for c in cookies))
    driver.quit()
    return nonce, cookies

def session_with_cookies(cookies):
    s = requests.Session(); s.headers.update(HEADERS)
    for c in cookies: s.cookies.set(c["name"], c["value"])
    return s

# ---------------------- Scraper core functions -----------------------
def fetch_csv_entries(session):
    log_message(f"Fetching CSV: {CSV_URL}")
    try:
        r = session.get(CSV_URL, timeout=30); r.raise_for_status()
        rows = []
        for row in csv.DictReader(StringIO(r.text)):
            if not row.get("Actions"): continue
            if "criminal" in row.get("Category","").lower(): continue
            rows.append(row)
        log_message(f"Loaded {len(rows)} non-criminal cases")
        return rows
    except Exception as e:
        log_message(f"✗ CSV error: {e}"); return []

def parse_actions_field(actions_value):
    if not actions_value: return None, None
    decoded = unescape(actions_value)
    soup = BeautifulSoup(decoded, "html.parser")
    link = soup.find("a")
    if not link: return None, None
    fid = link.get("data-fid") or link.get("data-id")
    fname = link.get("data-fname") or link.get("data-name")
    if not fid and link.get("href"):
        qs = parse_qs(urlparse(link["href"]).query)
        fid = (qs.get("fid") or qs.get("file") or [None])[0]
        fname = (qs.get("fname") or qs.get("file_name") or [None])[0]
    if not fname: fname = re.sub(r"\s+","_",link.get_text(strip=True))
    return fid, fname

def get_box_url(fid, fname, security, session):
    payload = {"action":"dl_bfile","fid":fid,"fname":fname,"security":security}
    try:
        session.get(load_base_url(), timeout=10)
        r = session.post(AJAX_URL, data=payload, headers=HEADERS, timeout=30)
        if r.status_code == 403: log_message("✗ 403 Forbidden"); return None
        data = r.json()
        if data.get("success") and isinstance(data.get("data"), dict):
            url = data["data"].get("fid") or data["data"].get("url")
            if url and "boxcloud.com" in url:
                log_message(f"✓ Got Box URL for {fid}")
                return url
        log_message(f"✗ AJAX no URL: {data}")
        return None
    except Exception as e:
        log_message(f"✗ AJAX exception: {e}"); return None

def download_pdf(session, url, dest):
    try:
        r = session.get(url, stream=True, timeout=60); r.raise_for_status()
        content = r.content
        if not content.startswith(b"%PDF"): log_message("✗ Not a PDF"); return False
        dest.write_bytes(content); log_message(f"✓ Saved {dest.name}"); return True
    except Exception as e: log_message(f"✗ Download error: {e}"); return False

def create_zip():
    zp = DATA_DIR / ZIP_NAME
    with ZipFile(zp, "w") as z:
        for f in DATA_DIR.glob("*.pdf"): z.write(f, f.name)
    return zp

# ------------------------- Orchestrator ------------------------------
def scrape_all(limit=None):
    log_message("="*60); log_message("Starting new scrape session")
    base = load_base_url(); log_message(f"Target URL: {base}")
    nonce, ck = fetch_live_nonce_and_cookies(base)
    if not nonce: return log_message("No nonce, abort.")
    sess = session_with_cookies(ck)
    entries = fetch_csv_entries(sess)
    if not entries: return log_message("No entries.")
    scraped = load_scraped_ids()
    new = [e for e in entries if e["Actions"] not in scraped]
    total = len(new)
    if not total: return log_message("Up to date.")
    cap = limit or MAX_DOWNLOADS
    if total > cap: new = new[:cap]; log_message(f"Will download {cap} of {total}")
    for i, e in enumerate(new,1):
        fid,fname = parse_actions_field(e["Actions"])
        if not fid: log_message(f"[{i}/{len(new)}] ✗ Could not parse FID"); continue
        name = sanitize_filename(fname or fid); pdfp = DATA_DIR / f"{name}.pdf"
        log_message(f"[{i}/{len(new)}] {e.get('Title','Untitled')} (fid={fid})")
        if pdfp.exists(): log_message("  ✓ Exists"); continue
        url = get_box_url(fid,fname,nonce,sess)
        if not url: log_message("  ✗ Could not resolve URL"); continue
        if download_pdf(sess,url,pdfp): save_scraped_id(e["Actions"])
        time.sleep(1)
    create_zip(); log_message("Scrape complete"); log_message("="*60)

# ---------------------------- Web UI --------------------------------
HOME_HTML = """<!doctype html><html><head><title>Cayman Judicial PDF Scraper</title>
<style>body{font-family:Arial;margin:32px;max-width:980px}.btn{background:#2e7d32;color:#fff;
padding:10px 16px;border-radius:6px;text-decoration:none}.btn:hover{background:#1b5e20}
.box{background:#f7f9fc;padding:16px;border-left:4px solid #0d6efd;margin-bottom:16px}</style>
</head><body><h1>🏛️ Cayman Judicial PDF Scraper</h1>
<div class=box><p>Downloads Cayman judicial PDFs using live nonce & cookies.</p></div>
<form action='{{url_for("update_config")}}' method=post class=box>
<label>Base URL:</label><input type=url name=base_url value='{{base_url}}' required>
<p><button class=btn type=submit>💾 Save</button>
<a class=btn href='{{url_for("run_scrape")}}'>▶️ Run</a>
<a class=btn href='{{url_for("report")}}'>📊 Report</a></p></form></body></html>"""

REPORT_HTML = """<!doctype html><html><head><title>Live Logs</title>
<style>body{font-family:Arial;margin:32px;max-width:1200px}
.btn{background:#0d6efd;color:#fff;padding:10px 16px;border-radius:6px;text-decoration:none}
.log{white-space:pre-wrap;background:#111;color:#ddd;padding:12px;border-radius:6px;height:600px;overflow-y:auto}
</style></head><body><h1>📊 PDF Report & Live Logs</h1>
<a class=btn href='{{url_for("home")}}'>🏠 Home</a>
<a class=btn href='{{url_for("run_scrape")}}'>🔄 Run Scraper</a>
<div class=log id=live-log>Connecting...</div>
<script>
const logDiv=document.getElementById("live-log");
const evtSrc=new EventSource("/stream_logs");
evtSrc.onmessage=e=>{logDiv.textContent=e.data;logDiv.scrollTop=logDiv.scrollHeight;}
</script></body></html>"""

@APP.route("/")
def home():
    return render_template_string(HOME_HTML, base_url=load_base_url())

@APP.route("/config", methods=["POST"])
def update_config():
    u=request.form.get("base_url"); 
    if u: save_base_url(u)
    return redirect(url_for("home"))

@APP.route("/scrape")
def run_scrape(): scrape_all(); return redirect(url_for("report"))

@APP.route("/report")
def report(): return render_template_string(REPORT_HTML)

@APP.route("/stream_logs")
def stream_logs():
    def generate():
        last_size=0
        while True:
            if SCRAPE_LOG.exists():
                lines=SCRAPE_LOG.read_text(encoding="utf-8",errors="ignore").splitlines()
                if len(lines)>last_size:
                    yield "data: "+ "\n".join(lines[-500:]) +"\n\n"
                    last_size=len(lines)
            time.sleep(2)
    return Response(generate(), mimetype="text/event-stream")

@APP.route("/download/<path:filename>")
def download_file(filename):
    return send_from_directory(DATA_DIR, filename, as_attachment=True)

if __name__=="__main__":
    APP.run(host="0.0.0.0", port=int(os.environ.get("PORT",5000)))
