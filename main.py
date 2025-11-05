import os
import re
import time
import csv
import json
import requests
from urllib.parse import urlparse, parse_qs
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://judicial.ky",
    "Referer": DEFAULT_BASE_URL,
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
@@ -107,161 +106,250 @@ def fetch_csv_data(csv_url, session):
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
    payload = {"action": "dl_bfile", "fid": fid, "fname": fname, "security": security}
    try:
        headers = HEADERS.copy()
        headers.update({'Referer': DEFAULT_BASE_URL, 'Origin': 'https://judicial.ky'})
        referer = session.headers.get("Referer", DEFAULT_BASE_URL)
        headers.update({"Referer": referer, "Origin": "https://judicial.ky"})
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


def parse_actions_field(actions_value):
    """Extract the Box.com identifiers from the CSV actions column."""
    if not actions_value:
        return None, None

    try:
        action_soup = BeautifulSoup(actions_value, "html.parser")
    except Exception:
        action_soup = None

    fid = None
    fname = None

    if action_soup:
        link = action_soup.find("a")
        if link:
            for attr in ("data-fid", "data-file", "data-id", "data-item", "data-file-id"):
                if link.get(attr):
                    fid = link.get(attr).strip()
                    break
            for attr in ("data-fname", "data-name", "data-file-name", "data-title"):
                if link.get(attr):
                    fname = link.get(attr).strip()
                    break

            href = link.get("href")
            if href:
                parsed = urlparse(href)
                query_params = parse_qs(parsed.query)
                if not fid:
                    for key in ("fid", "file", "id"):
                        if query_params.get(key):
                            fid = query_params[key][0]
                            break
                if not fname:
                    for key in ("fname", "name", "file_name"):
                        if query_params.get(key):
                            fname = query_params[key][0]
                            break
                if not fname:
                    possible_name = os.path.basename(parsed.path)
                    if possible_name:
                        fname = os.path.splitext(possible_name)[0]

            if not fname:
                text_label = link.get_text(strip=True)
                if text_label:
                    fname = re.sub(r"\s+", "_", text_label)

    if not fid or not fname:
        cleaned = actions_value.strip()
        if not fid:
            fid = cleaned
        if not fname:
            fname = cleaned

    return fid, fname

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
    session.headers["Referer"] = base_url

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
                fid, fname = parse_actions_field(entry['actions'])
                if not fid or not fname:
                    log_message("✗ Could not parse file identifiers from CSV entry, skipping")
                    status = "PARSE_FAILED"
                    results.append({
                        "file": "",
                        "status": status,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "identifier": entry['actions'],
                        "neutral_citation": entry["neutral_citation"],
                        "cause_number": entry["cause_number"],
                        "judgment_date": entry["judgment_date"],
                        "title": entry["title"],
                        "subject": entry["subject"],
                        "court": entry["court"],
                        "category": entry["category"]
                    })
                    continue

                safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", fname).strip("._") or fid
                pdf_filename = f"{safe_name}.pdf"
                pdf_path = os.path.join(DATA_DIR, pdf_filename)

                log_message(f"[{idx}/{len(new_entries)}] {entry['title']} ({fid})")
                log_message(f"[{idx}/{len(new_entries)}] {entry['title']} (fid={fid}, fname={safe_name})")

                if os.path.exists(pdf_path):
                    status = "EXISTING"
                    log_message(f"✓ Already exists: {pdf_filename}")
                else:
                    box_url = get_box_url(fid, fname, security_nonce, session)
                    if not box_url:
                        status = "API_FAILED"
                        log_message(f"✗ Could not get download URL, skipping")
                    else:
                        fallback_url = f"https://judicial.ky/wp-content/uploads/box_files/{fid}.pdf"
                        log_message(f"⚠️ AJAX download failed; attempting direct link {fallback_url}")
                        box_url = fallback_url

                    if box_url:
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
                    else:
                        status = "API_FAILED"
                        log_message(f"✗ Could not get download URL, skipping")

                results.append({
                    "file": pdf_filename,
                    "status": status,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "identifier": fname,
                    "identifier": fid,
                    "neutral_citation": entry["neutral_citation"],
                    "cause_number": entry["cause_number"],
                    "judgment_date": entry["judgment_date"],
                    "title": entry["title"],
                    "subject": entry["subject"],
                    "court": entry["court"],
                    "category": entry["category"]
                })
                save_scraped_url(fname)
                save_scraped_url(fid)
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
        results.append({
            "file": "ERROR",
            "status": str(e),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "identifier": "",
        })
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
