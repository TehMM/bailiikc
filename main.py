"""Comprehensive Cayman judicial PDF scraper and web dashboard."""
from __future__ import annotations

import csv
import json
import os
import re
import time
import csv
import json
import requests
from urllib.parse import urlparse, parse_qs
from io import StringIO
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


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def log_message(message: str) -> None:
    """Write a timestamped log line to stdout and the scrape log."""

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

    if response.content.startswith(b"%PDF"):
        return response.url or action_info.fallback_url

    try:
        payload_json = response.json()
    except ValueError:
        log_message("AJAX response was not JSON")
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

    log_message("AJAX payload indicated failure")
    return None


    session = requests.Session()
    session.headers.update(HEADERS)
    session.headers["Referer"] = base_url

def download_pdf(session: requests.Session, url: str, destination: Path) -> Tuple[bool, Optional[str]]:
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
                    success, error = self.download_pdf(download_url, destination)
                    if success:
                        status = "DOWNLOADED"
                        size_kib = destination.stat().st_size / 1024
                        message = f"Saved {filename} ({size_kib:.1f} KiB)"
                        log_message(f"✓ {message}")
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
