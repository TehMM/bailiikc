import os
import re
import time
import csv
import json
import requests
from io import StringIO
from datetime import datetime
from zipfile import ZipFile
from bs4 import BeautifulSoup
from flask import Flask, render_template_string, redirect, url_for, send_from_directory, jsonify, make_response, request

app = Flask(__name__)

# Config
DATA_DIR = "./data/pdfs"
DEFAULT_BASE_URL = "https://judicial.ky/judgments/unreported-judgments/"
CSV_URL = "https://judicial.ky/wp-content/uploads/box_files/judgments.csv"
ZIP_NAME = "all_pdfs.zip"
SCRAPE_LOG = os.path.join(DATA_DIR, "scrape_log.txt")
SCRAPED_URLS_FILE = os.path.join(DATA_DIR, "scraped_urls.txt")
CONFIG_FILE = os.path.join(DATA_DIR, "config.txt")
METADATA_FILE = os.path.join(DATA_DIR, "metadata.json")

os.makedirs(DATA_DIR, exist_ok=True)

# Headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': 'https://judicial.ky',
    'Referer': DEFAULT_BASE_URL,
}

# --- Utility Functions ---
def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    print(log_entry.strip())
    with open(SCRAPE_LOG, "a") as f:
        f.write(log_entry)

def load_scraped_urls():
    if os.path.exists(SCRAPED_URLS_FILE):
        with open(SCRAPED_URLS_FILE, "r") as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def save_scraped_url(identifier):
    with open(SCRAPED_URLS_FILE, "a") as f:
        f.write(f"{identifier}\n")

def load_base_url():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            url = f.read().strip()
            if url:
                return url
    return DEFAULT_BASE_URL

def save_base_url(url):
    with open(CONFIG_FILE, "w") as f:
        f.write(url.strip())

def save_metadata(metadata_list):
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata_list, f, indent=2)

def load_metadata():
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            return json.load(f)
    return []

def cloak_url(url):
    if url and url.startswith("http"):
        return f"http://anon.to/?{url}"
    return url

def extract_security_nonce(soup):
    scripts = soup.find_all("script")
    for script in scripts:
        if script.string and "dl_bfile" in script.string:
            matches = re.findall(r'security["\s:=]+["\']?([a-f0-9]{10})["\']?', script.string, re.IGNORECASE)
            if matches:
                log_message(f"Found nonce: {matches[0]}")
                return matches[0]
    # Fallback
    for script in scripts:
        if script.string:
            matches = re.findall(r'\b([a-f0-9]{10})\b', script.string.lower())
            if matches:
                log_message(f"Possible nonce fallback: {matches[0]}")
                return matches[0]
    return None

def fetch_csv_data(csv_url, session):
    try:
        log_message(f"Fetching CSV from: {csv_url}")
        resp = session.get(csv_url, timeout=30)
        resp.raise_for_status()
        reader = csv.DictReader(StringIO(resp.text))
        entries = []
        for row in reader:
            if not row.get('Actions'):
                continue
            if 'criminal' in row.get('Category', '').lower():
                continue
            entries.append({
                'neutral_citation': row.get('Neutral Citation', ''),
                'cause_number': row.get('Cause Number', ''),
                'judgment_date': row.get('Judgment Date', ''),
                'title': row.get('Title', ''),
                'subject': row.get('Subject', ''),
                'court': row.get('Court', ''),
                'category': row.get('Category', ''),
                'actions': row.get('Actions', '')
            })
        log_message(f"Loaded {len(entries)} non-criminal cases from CSV")
        return entries
    except Exception as e:
        log_message(f"ERROR fetching CSV: {e}")
        return []

def get_box_url(fid, fname, security, session):
    ajax_url = "https://judicial.ky/wp-admin/admin-ajax.php"
    payload = {'action':'dl_bfile','fid':fid,'fname':fname,'security':security}
    try:
        headers = HEADERS.copy()
        headers.update({'Referer': DEFAULT_BASE_URL, 'Origin': 'https://judicial.ky'})
        resp = session.post(ajax_url, data=payload, headers=headers, timeout=30)
        if resp.status_code == 403:
            log_message("403 Forbidden: could not get PDF via AJAX")
            return None
        resp.raise_for_status()
        try:
            data = resp.json()
            if data.get('success'):
                return data.get('data', {}).get('fid') or data.get('data', {}).get('url')
        except:
            if resp.content[:4] == b'%PDF':
                return resp.url
        return None
    except Exception as e:
        log_message(f"Error calling API: {e}")
        return None

def scrape_pdfs(base_url=None):
    results = []
    log_message("="*60)
    log_message("Starting new scrape session")

    scraped_ids = load_scraped_urls()
    log_message(f"Loaded {len(scraped_ids)} previously scraped identifiers")

    if not base_url:
        base_url = load_base_url()
    log_message(f"Target URL: {base_url}")

    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        r = session.get(base_url, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        security_nonce = extract_security_nonce(soup)
        if not security_nonce:
            log_message("ERROR: could not find security nonce")
            return results
        log_message(f"Found security nonce: {security_nonce}")

        csv_entries = fetch_csv_data(CSV_URL, session)
        if not csv_entries:
            return results

        new_entries = [e for e in csv_entries if e['actions'] not in scraped_ids]
        log_message(f"Will download {len(new_entries)} new PDFs")

        for idx, entry in enumerate(new_entries, 1):
            try:
                fid = entry['actions']
                fname = entry['actions']
                pdf_filename = f"{fname}.pdf"
                pdf_path = os.path.join(DATA_DIR, pdf_filename)

                log_message(f"[{idx}/{len(new_entries)}] {entry['title']} ({fid})")

                if os.path.exists(pdf_path):
                    status = "EXISTING"
                    log_message(f"✓ Already exists: {pdf_filename}")
                else:
                    box_url = get_box_url(fid, fname, security_nonce, session)
                    if not box_url:
                        status = "API_FAILED"
                        log_message(f"✗ Could not get download URL, skipping")
                    else:
                        pdf_resp = session.get(box_url, timeout=60, stream=True)
                        pdf_resp.raise_for_status()
                        content = pdf_resp.content
                        if content[:4] != b'%PDF':
                            status = "NOT_PDF"
                            log_message(f"✗ Downloaded file is not a PDF")
                        else:
                            with open(pdf_path, "wb") as f:
                                f.write(content)
                            status = "NEW"
                            log_message(f"✓ Saved: {pdf_filename} ({len(content)/1024:.1f} KB)")

                results.append({
                    "file": pdf_filename,
                    "status": status,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "identifier": fname,
                    "neutral_citation": entry["neutral_citation"],
                    "cause_number": entry["cause_number"],
                    "judgment_date": entry["judgment_date"],
                    "title": entry["title"],
                    "subject": entry["subject"],
                    "court": entry["court"],
                    "category": entry["category"]
                })
                save_scraped_url(fname)
                time.sleep(1)
            except Exception as e:
                log_message(f"✗ Error processing entry: {e}")
                continue

        if results:
            save_metadata(results)
            log_message(f"Saved metadata for {len(results)} entries")

        log_message("Scraping complete")
        log_message("="*60)
    except Exception as e:
        log_message(f"SCRAPING ERROR: {e}")
        results.append({"file":"ERROR","status":str(e),"timestamp":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"identifier":""})
    return results

def create_zip():
    zip_path = os.path.join(DATA_DIR, ZIP_NAME)
    try:
        with ZipFile(zip_path, "w") as zipf:
            count = 0
            for f in os.listdir(DATA_DIR):
                if f.endswith(".pdf"):
                    zipf.write(os.path.join(DATA_DIR, f), f)
                    count += 1
        log_message(f"Created ZIP with {count} PDFs")
    except Exception as e:
        log_message(f"ZIP creation error: {e}")
    return zip_path

# --- Flask Routes ---
@app.route("/")
def index():
    current_url = load_base_url()
    html = f"""
    <html>
    <head>
        <title>Cayman Judgments PDF Scraper</title>
        <style>
            body {{ font-family: Arial, sans-serif; background: #f4f4f4; padding: 20px; }}
            h1 {{ color: #333; }}
            .form-group {{ margin-bottom: 15px; }}
            input[type=text] {{ width: 400px; padding: 8px; }}
            button {{ padding: 8px 16px; margin-top: 5px; }}
            a {{ color: #0066cc; text-decoration: none; }}
        </style>
    </head>
    <body>
        <h1>Cayman Judicial PDF Scraper</h1>
        <form method="POST" action="/update-config">
            <div class="form-group">
                <label>Base URL:</label>
                <input type="text" name="base_url" value="{current_url}" />
            </div>
            <button type="submit">Update URL</button>
        </form>
        <form method="POST" action="/run-download">
            <button type="submit">Run Scraper Now</button>
        </form>
        <p><a href="/report">View Report</a> | <a href="{cloak_url(CSV_URL)}" target="_blank">CSV Source</a></p>
    </body>
    </html>
    """
    return html

@app.route("/update-config", methods=["POST"])
def update_config():
    new_url = request.form.get("base_url", "").strip()
    if new_url:
        save_base_url(new_url)
        log_message(f"Configuration updated: {new_url}")
    return redirect(url_for("index"))

@app.route("/run-download", methods=["POST"])
def run_download():
    custom_url = request.form.get("url") or request.args.get("url")
    if custom_url:
        scrape_pdfs(base_url=custom_url.strip())
    else:
        scrape_pdfs()
    create_zip()
    return redirect(url_for("report"))

@app.route("/report")
def report():
    current_url = load_base_url()
    files = []
    for f in os.listdir(DATA_DIR):
        if f.endswith(".pdf"):
            files.append({
                "name": f,
                "status": "DOWNLOADED",
                "timestamp": datetime.fromtimestamp(os.path.getmtime(os.path.join(DATA_DIR,f))).strftime("%Y-%m-%d %H:%M:%S"),
                "size": f"{os.path.getsize(os.path.join(DATA_DIR,f))/1024:.1f} KB"
            })
    files.sort(key=lambda x: x["timestamp"], reverse=True)
    zip_exists = os.path.exists(os.path.join(DATA_DIR, ZIP_NAME))
    log_content = ""
    if os.path.exists(SCRAPE_LOG):
        with open(SCRAPE_LOG) as f:
            log_content = "".join(f.readlines()[-150:])
    html = f"""
    <html>
    <head>
        <title>Scraper Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; background: #f4f4f4; padding: 20px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; }}
            th {{ background-color: #333; color: white; }}
            tr:nth-child(even) {{ background-color: #f2f2f2; }}
            pre {{ background: #000; color: #0f0; padding: 10px; max-height: 300px; overflow-y: scroll; }}
        </style>
    </head>
    <body>
        <h1>Scraper Report</h1>
        <p><a href="/">Back to Home</a></p>
        <h2>Downloaded PDFs</h2>
        <table>
            <tr><th>Filename</th><th>Status</th><th>Timestamp</th><th>Size</th></tr>
            {''.join(f'<tr><td>{f["name"]}</td><td>{f["status"]}</td><td>{f["timestamp"]}</td><td>{f["size"]}</td></tr>' for f in files)}
        </table>
        <p>{'<a href="/files/'+ZIP_NAME+'">Download ZIP</a>' if zip_exists else ''}</p>
        <h2>Recent Logs</h2>
        <pre>{log_content}</pre>
    </body>
    </html>
    """
    return html

@app.route("/files/<path:filename>")
def download_file(filename):
    try:
        return send_from_directory(DATA_DIR, filename, as_attachment=True)
    except Exception as e:
        return f"File not found: {e}", 404

@app.route("/export/csv")
def export_csv():
    metadata = load_metadata()
    if not metadata:
        return "No metadata available", 404
    si = StringIO()
    fieldnames = ['neutral_citation','cause_number','judgment_date','title','subject','court','category','file','status','timestamp']
    writer = csv.DictWriter(si, fieldnames=fieldnames)
    writer.writeheader()
    for item in metadata:
        writer.writerow({k:item.get(k,'') for k in fieldnames})
    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=judgments_metadata.csv"
    output.headers["Content-type"] = "text/csv"
    return output

@app.route("/api/metadata")
def api_metadata():
    return jsonify(load_metadata())

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
