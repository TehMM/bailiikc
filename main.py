import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from flask import Flask, render_template_string, send_from_directory, redirect, url_for, jsonify, make_response
from zipfile import ZipFile
from urllib.parse import urljoin, urlparse
import time
import re
import json
import csv
from io import StringIO

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

# Headers to mimic a real browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://judicial.ky/judgments/unreported-judgments/',
    'Origin': 'https://judicial.ky',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
}

def log_message(message):
    """Log scraping messages to file and console."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    print(log_entry.strip())
    with open(SCRAPE_LOG, "a") as f:
        f.write(log_entry)

def load_scraped_urls():
    """Load the set of previously scraped identifiers."""
    if os.path.exists(SCRAPED_URLS_FILE):
        with open(SCRAPED_URLS_FILE, "r") as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def save_scraped_url(identifier):
    """Save a successfully scraped identifier to the tracking file."""
    with open(SCRAPED_URLS_FILE, "a") as f:
        f.write(f"{identifier}\n")

def load_base_url():
    """Load the configured base URL or return default."""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            url = f.read().strip()
            if url:
                return url
    return DEFAULT_BASE_URL

def save_base_url(url):
    """Save the base URL to config file."""
    with open(CONFIG_FILE, "w") as f:
        f.write(url.strip())

def save_metadata(metadata_list):
    """Save metadata for all downloaded PDFs."""
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata_list, f, indent=2)

def load_metadata():
    """Load metadata for all downloaded PDFs."""
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            return json.load(f)
    return []

def cloak_url(url):
    """Cloak URL with anon.to to strip referrer information."""
    if url and url.startswith("http"):
        return f"http://anon.to/?{url}"
    return url

def extract_ajax_endpoints(soup):
    print("=== Checking for AJAX endpoints ===")
    scripts = soup.find_all("script")
    for s in scripts:
        if s.string and "ajax" in s.string.lower():
            print(s.string[:500])
            
def extract_security_nonce(soup):
    """Extract the WordPress security nonce from the page."""
    scripts = soup.find_all("script")
    
    # First, look for the specific nonce variable used for file downloads
    for script in scripts:
        if script.string and "dl_bfile" in script.string:
            log_message(f"  Found script containing 'dl_bfile'")
            # Look for the nonce in this specific context
            matches = re.findall(r'security["\s:=]+(["\']?)([a-f0-9]{10})(["\']?)', script.string, re.IGNORECASE)
            if matches:
                log_message(f"  Found nonce in dl_bfile script: {matches[0][1]}")
                return matches[0][1]
    
    # Look for specific variable names that might contain the nonce
    for script in scripts:
        if script.string:
            # Look for common WordPress nonce variable patterns
            patterns = [
                r'var\s+box_ajax\s*=\s*\{[^}]*security["\s:]+["\']([a-f0-9]{10})["\']',
                r'boxAjax\s*=\s*\{[^}]*security["\s:]+["\']([a-f0-9]{10})["\']',
                r'wp_ajax\s*=\s*\{[^}]*security["\s:]+["\']([a-f0-9]{10})["\']',
                r'"security"\s*:\s*"([a-f0-9]{10})"',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, script.string, re.IGNORECASE | re.DOTALL)
                if matches:
                    log_message(f"  Found nonce with pattern: {pattern[:50]}...")
                    log_message(f"  Nonce value: {matches[0]}")
                    return matches[0]
    
    # Original patterns as fallback
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
    
    # Look for data attributes on buttons
    buttons = soup.find_all("button", {"data-security": True})
    if buttons:
        nonce = buttons[0].get("data-security")
        log_message(f"  Found nonce in data attribute: {nonce}")
        return nonce
    
    # Try to find any 10-character hex string as last resort
    for script in scripts:
        if script.string:
            hex_matches = re.findall(r'\b([a-f0-9]{10})\b', script.string.lower())
            if hex_matches:
                log_message(f"  Found possible nonce (hex pattern): {hex_matches[0]}")
                # Save a snippet of surrounding context for debugging
                for match in hex_matches[:3]:  # Check first 3 matches
                    idx = script.string.lower().find(match)
                    if idx > 0:
                        context = script.string[max(0, idx-50):min(len(script.string), idx+60)]
                        log_message(f"  Context for {match}: ...{context}...")
                return hex_matches[0]
    
    return None

def get_box_url(fid, fname, security, session):
    """Get the Box.com download URL from WordPress AJAX endpoint."""
    ajax_url = "https://judicial.ky/wp-admin/admin-ajax.php"
    
    payload = {
        'action': 'dl_bfile',
        'fid': fid,
        'fname': fname,
        'security': security
    }
    
    try:
        response = session.post(ajax_url, data=payload, headers=HEADERS, timeout=30)
        
        # Log the full response for debugging
        if response.status_code == 403:
            log_message(f"  ✗ 403 Forbidden. Response: {response.text[:200]}")
            log_message(f"  Security nonce used: {security}")
            log_message(f"  Payload: {payload}")
            return None
        
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('success'):
            box_url = data.get('data', {}).get('fid')
            if box_url:
                return box_url
        
        log_message(f"  ✗ API returned no URL: {data}")
        return None
        
    except requests.exceptions.HTTPError as e:
        log_message(f"  ✗ HTTP Error calling API: {e}")
        log_message(f"  Response status: {response.status_code}")
        log_message(f"  Response text: {response.text[:500]}")
        return None
    except Exception as e:
        log_message(f"  ✗ Error calling API: {e}")
        return None

def fetch_csv_data(csv_url, session):
    """Fetch and parse the judgments CSV file."""
    try:
        log_message(f"Fetching CSV from: {csv_url}")
        response = session.get(csv_url, timeout=30)
        response.raise_for_status()
        
        # Parse CSV
        csv_content = response.text
        reader = csv.DictReader(StringIO(csv_content))
        
        entries = []
        for row in reader:
            # Skip empty rows
            if not row.get('Actions'):
                continue
            
            # Skip criminal cases
            category = row.get('Category', '')
            if 'criminal' in category.lower() or 'crim' in category.lower():
                continue
            
            entries.append({
                'neutral_citation': row.get('Neutral Citation', ''),
                'cause_number': row.get('Cause Number', ''),
                'judgment_date': row.get('Judgment Date', ''),
                'title': row.get('Title', ''),
                'subject': row.get('Subject', ''),
                'court': row.get('Court', ''),
                'category': row.get('Category', ''),
                'actions': row.get('Actions', '')  # This is the identifier/fid
            })
        
        log_message(f"Loaded {len(entries)} non-criminal cases from CSV")
        return entries
        
    except Exception as e:
        log_message(f"ERROR fetching CSV: {e}")
        return []

def extract_ajax_endpoints(soup):
    print("=== Checking for AJAX endpoints ===")
    scripts = soup.find_all("script")
    for s in scripts:
        if s.string and "ajax" in s.string.lower():
            print(s.string[:500])

def debug_list_scripts(soup):
    """List all script tags to help find AJAX and nonce sources."""
    print("=== Listing all <script> tags ===")
    for s in soup.find_all("script"):
        src = s.get("src")
        if src:
            print("External JS:", src)
        elif s.string:
            snippet = s.string.strip().replace("\n", " ")[:250]
            print("Inline JS snippet:", snippet)
            
def scrape_pdfs(base_url=None):
    """Scrape PDFs from Cayman Judicial website using CSV data."""
    results = []
    log_message("=" * 60)
    log_message("Starting new scrape session")
    
    # Load previously scraped identifiers
    scraped_ids = load_scraped_urls()
    log_message(f"Loaded {len(scraped_ids)} previously scraped identifiers")
    
    # Determine which base URL to use
    if not base_url:
        base_url = load_base_url()
    log_message(f"Target URL: {base_url}")
    
    # Create a session to maintain cookies
    session = requests.Session()
    session.headers.update(HEADERS)
    
    try:
        # Get the main page to extract security nonce and establish session
        log_message(f"Fetching main page for security nonce...")
        r = session.get(base_url, timeout=15)
        r.raise_for_status()
        
        # Log cookies received
        log_message(f"Session cookies: {list(session.cookies.keys())}")
        
        soup = BeautifulSoup(r.text, "html.parser")

        # Debug: list all scripts to find AJAX handlers or nonces
        debug_list_scripts(soup)

        # Debug new AJAX endpoint scanner
        extract_ajax_endpoints(soup)

        # Extract security nonce
        security_nonce = extract_security_nonce(soup)
        if not security_nonce:
            log_message("ERROR: Could not find security nonce on page!")
            log_message("Saving page HTML for debugging...")
            debug_file = os.path.join(DATA_DIR, "debug_page.html")
            with open(debug_file, "w", encoding="utf-8") as f:
                f.write(soup.prettify())
            log_message(f"Saved page HTML to: {debug_file}")
            return results
        
        log_message(f"✓ Found security nonce: {security_nonce}")
        
        # Test the nonce with a sample request before processing all entries
        log_message("Testing security nonce with sample API call...")
        test_fid = "59LLDG6R2OTW1DE6089H76C60314E467CE573E05DE3E0A9E4167"
        test_fname = test_fid
        test_url = get_box_url(test_fid, test_fname, security_nonce, session)
        
        if test_url:
            log_message(f"✓ Security nonce is valid! Test succeeded.")
        else:
            log_message(f"✗ Security nonce test FAILED!")
            log_message(f"The nonce may be invalid or the API endpoint may have changed.")
            log_message(f"Check the debug output above for details.")
            # Continue anyway to log all failures
        
        # Add a small delay after test
        time.sleep(2)
        
        # Fetch CSV data
        csv_entries = fetch_csv_data(CSV_URL, session)
        
        if not csv_entries:
            log_message("ERROR: Could not fetch CSV data")
            return results
        
        # Filter out already scraped entries
        new_entries = [e for e in csv_entries if e['actions'] not in scraped_ids]
        skipped_count = len(csv_entries) - len(new_entries)
        
        if skipped_count > 0:
            log_message(f"Skipping {skipped_count} previously scraped entries")
        log_message(f"Will download {len(new_entries)} new PDFs")
        
        # Download each PDF
        for idx, entry in enumerate(new_entries, 1):
            try:
                fid = entry['actions']
                fname = entry['actions']  # Use actions field as both fid and fname
                
                log_message(f"[{idx}/{len(new_entries)}] Processing: {entry['title']}")
                log_message(f"  Citation: {entry['neutral_citation']}")
                log_message(f"  Category: {entry['category']}")
                log_message(f"  FID: {fid}")
                
                # Generate filename
                pdf_filename = f"{fname}.pdf"
                pdf_path = os.path.join(DATA_DIR, pdf_filename)
                
                # Check if already downloaded
                if os.path.exists(pdf_path):
                    status = "EXISTING"
                    log_message(f"  ✓ Already have: {pdf_filename}")
                else:
                    # Get Box.com URL from API
                    log_message(f"  → Calling API to get download URL...")
                    box_url = get_box_url(fid, fname, security_nonce, session)
                    
                    if not box_url:
                        status = "API_FAILED"
                        log_message(f"  ✗ Could not get download URL from API")
                    else:
                        # Download the PDF from Box.com
                        try:
                            log_message(f"  ↓ Downloading from Box.com...")
                            pdf_response = session.get(box_url, timeout=60, stream=True)
                            pdf_response.raise_for_status()
                            
                            # Verify it's a PDF
                            content = pdf_response.content
                            if content[:4] != b'%PDF':
                                status = "NOT_PDF"
                                log_message(f"  ✗ Downloaded file is not a valid PDF")
                            else:
                                with open(pdf_path, "wb") as f:
                                    f.write(content)
                                
                                status = "NEW"
                                file_size = len(content)
                                log_message(f"  ✓ Saved: {pdf_filename} ({file_size / 1024:.1f} KB)")
                        
                        except Exception as e:
                            status = f"DOWNLOAD_ERROR: {e}"
                            log_message(f"  ✗ Error downloading PDF: {e}")
                
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
                
                # Mark as processed
                save_scraped_url(fname)
                
                # Be polite to the server
                time.sleep(1)
                
            except Exception as e:
                log_message(f"  ✗ Error processing entry: {e}")
                continue
        
        # Save metadata
        if results:
            save_metadata(results)
            log_message(f"Saved metadata for {len(results)} entries")
        
        log_message(f"Scraping complete. Processed {len(results)} entries")
        log_message("=" * 60)
        
    except Exception as e:
        error_msg = f"SCRAPING ERROR: {e}"
        log_message(error_msg)
        results.append({
            "file": "ERROR",
            "status": error_msg,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "identifier": ""
        })
    
    return results

def create_zip():
    """Create a ZIP of all PDFs safely."""
    zip_path = os.path.join(DATA_DIR, ZIP_NAME)
    try:
        with ZipFile(zip_path, "w") as zipf:
            pdf_count = 0
            for filename in os.listdir(DATA_DIR):
                if filename.endswith(".pdf"):
                    zipf.write(os.path.join(DATA_DIR, filename), filename)
                    pdf_count += 1
        log_message(f"Created ZIP with {pdf_count} PDFs")
    except Exception as e:
        log_message(f"ZIP creation error: {e}")
    return zip_path

@app.route("/")
def index():
    """Home page with scrape button and URL configuration."""
    current_url = load_base_url()
    
    html = """
    <html>
    <head>
        <title>Cayman Judicial PDF Scraper</title>
        <style>
            body { 
                font-family: Arial, sans-serif; 
                margin: 40px;
                max-width: 900px;
            }
            .form-group {
                margin-bottom: 20px;
            }
            label {
                display: block;
                font-weight: bold;
                margin-bottom: 5px;
            }
            input[type="url"] {
                width: 100%;
                padding: 10px;
                font-size: 14px;
                border: 1px solid #ddd;
                border-radius: 4px;
                box-sizing: border-box;
            }
            .button {
                background-color: #4CAF50;
                color: white;
                padding: 12px 24px;
                font-size: 16px;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                margin-right: 10px;
                text-decoration: none;
                display: inline-block;
            }
            .button:hover {
                background-color: #45a049;
            }
            .button-secondary {
                background-color: #008CBA;
            }
            .button-secondary:hover {
                background-color: #007399;
            }
            .info-box {
                background-color: #f0f8ff;
                border-left: 4px solid #2196F3;
                padding: 15px;
                margin: 20px 0;
            }
            .success-box {
                background-color: #d4edda;
                border-left: 4px solid #28a745;
                padding: 15px;
                margin: 20px 0;
            }
        </style>
    </head>
    <body>
        <h1>🏛️ Cayman Judicial PDF Scraper</h1>
        
        <div class="success-box">
            <strong>✅ CSV-Based Automated Scraping</strong><br>
            This scraper uses the official CSV file from judicial.ky and automatically downloads PDFs via the WordPress AJAX API.
            <ul style="margin: 10px 0;">
                <li>✓ Reads judgments from official CSV file</li>
                <li>✓ Automatically skips Criminal cases</li>
                <li>✓ Captures full metadata (8 fields)</li>
                <li>✓ Downloads PDFs via Box.com API</li>
                <li>✓ Tracks progress to avoid re-downloading</li>
            </ul>
        </div>
        
        <form action="{{ url_for('update_config') }}" method="post">
            <div class="form-group">
                <label for="base_url">Base URL (for nonce extraction):</label>
                <input 
                    type="url" 
                    id="base_url" 
                    name="base_url" 
                    value="{{ current_url }}"
                    placeholder="https://judicial.ky/judgments/unreported-judgments/"
                    required
                >
            </div>
            
            <button type="submit" class="button">
                💾 Save URL
            </button>
            <button type="button" class="button button-secondary" 
                    onclick="document.getElementById('base_url').value='{{ default_url }}'">
                🔄 Reset to Default
            </button>
        </form>
        
        <div class="info-box">
            <strong>Current Target:</strong><br>
            <a href="{{ cloak_url(current_url) }}" target="_blank" rel="noopener noreferrer">{{ current_url }}</a><br><br>
            <strong>CSV Source:</strong><br>
            <a href="{{ cloak_url(csv_url) }}" target="_blank" rel="noopener noreferrer">{{ csv_url }}</a>
        </div>
        
        <form action="{{ url_for('run_download') }}" method="post" style="margin-top: 20px;">
            <button type="submit" class="button" style="font-size: 18px; padding: 15px 30px;">
                ▶️ Run Scraper Now
            </button>
        </form>
        
        <p style="margin-top: 20px;">
            <a href="{{ url_for('report') }}" class="button">📊 View Report & Downloads</a>
        </p>
        
        <div class="info-box" style="margin-top: 30px; border-left-color: #FF9800;">
            <strong>🔗 For changedetection.io integration:</strong><br>
            Use this endpoint: <code>{{ url_for('run_download', _external=True) }}</code><br>
            <small>Set up a POST request to this URL to trigger scraping automatically</small>
        </div>
    </body>
    </html>
    """
    return render_template_string(
        html, 
        current_url=current_url,
        default_url=DEFAULT_BASE_URL,
        csv_url=CSV_URL,
        cloak_url=cloak_url
    )

@app.route("/update-config", methods=["POST"])
def update_config():
    """Update the base URL configuration."""
    from flask import request
    new_url = request.form.get("base_url", "").strip()
    
    if new_url:
        save_base_url(new_url)
        log_message(f"Configuration updated: Base URL changed to {new_url}")
    
    return redirect(url_for("index"))

@app.route("/run-download", methods=["GET", "POST"])
def run_download():
    """Run the scraper and redirect to report."""
    from flask import request
    
    # Check if a custom URL was provided
    custom_url = request.args.get("url") or request.form.get("url")
    
    if custom_url:
        custom_url = custom_url.strip()
        results = scrape_pdfs(base_url=custom_url)
    else:
        # Use saved configuration
        results = scrape_pdfs()
    
    create_zip()
    return redirect(url_for("report"))

@app.route("/report")
def report():
    """Display report of all PDFs with download links."""
    current_url = load_base_url()
    files = []
    
    # Get all PDFs in directory
    for f in os.listdir(DATA_DIR):
        if f.endswith(".pdf"):
            file_path = os.path.join(DATA_DIR, f)
            files.append({
                "name": f,
                "status": "DOWNLOADED",
                "timestamp": datetime.fromtimestamp(
                    os.path.getmtime(file_path)
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "size": f"{os.path.getsize(file_path) / 1024:.1f} KB"
            })
    
    # Sort by timestamp (newest first)
    files.sort(key=lambda x: x["timestamp"], reverse=True)
    
    # Check if ZIP exists
    zip_exists = os.path.exists(os.path.join(DATA_DIR, ZIP_NAME))
    
    # Read log file
    log_content = ""
    if os.path.exists(SCRAPE_LOG):
        with open(SCRAPE_LOG, "r") as f:
            log_lines = f.readlines()
            # Get last 150 lines
            log_content = "".join(log_lines[-150:])
    
    html = """
    <html>
    <head>
        <title>Cayman Judicial PDF Report</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            table { border-collapse: collapse; width: 100%; margin-top: 20px; font-size: 13px; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #4CAF50; color: white; position: sticky; top: 0; }
            tr:nth-child(even) { background-color: #f2f2f2; }
            .button { 
                background-color: #4CAF50; 
                color: white; 
                padding: 10px 20px; 
                text-decoration: none; 
                display: inline-block;
                margin: 10px 5px;
                border-radius: 4px;
            }
            .button:hover {
                background-color: #45a049;
            }
            .log { 
                background-color: #f5f5f5; 
                padding: 10px; 
                border: 1px solid #ddd;
                max-height: 400px;
                overflow-y: scroll;
                font-family: monospace;
                font-size: 12px;
                white-space: pre-wrap;
            }
            .stats {
                background-color: #e8f5e9;
                padding: 15px;
                border-radius: 4px;
                margin: 20px 0;
            }
            .table-container {
                max-height: 600px;
                overflow-y: auto;
                margin-top: 20px;
            }
            .filename-cell {
                font-family: monospace;
                font-size: 11px;
            }
        </style>
    </head>
    <body>
        <h1>📊 Cayman Judicial PDF Report</h1>
        
        <div class="stats">
            <strong>Statistics</strong><br>
            Current Target: <a href="{{ cloak_url(current_url) }}" target="_blank" rel="noopener noreferrer">{{ current_url }}</a><br>
            CSV Source: <a href="{{ cloak_url(csv_url) }}" target="_blank" rel="noopener noreferrer">{{ csv_url }}</a><br>
            Total PDFs Downloaded: <strong>{{ files|length }}</strong><br>
            <em>Note: Criminal cases are automatically excluded from downloads</em>
        </div>
        
        <div>
            <a href="{{ url_for('index') }}" class="button">🏠 Home</a>
            {% if zip_exists %}
            <a href="{{ url_for('download_file', filename=zip_name) }}" class="button">
                📦 Download All as ZIP
            </a>
            {% endif %}
            <a href="{{ url_for('export_csv') }}" class="button" style="background-color: #FF9800;">
                📊 Export Metadata (CSV)
            </a>
            <a href="{{ url_for('run_download') }}" class="button" 
               onclick="return confirm('Run scraper to check for new judgments?')">
                🔄 Run Scraper Again
            </a>
        </div>
        
        <h2>📁 Downloaded PDFs</h2>
        {% if files %}
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th style="width: 250px;">Filename</th>
                        <th style="width: 80px;">Size</th>
                        <th style="width: 140px;">Downloaded</th>
                        <th style="width: 100px;">Actions</th>
                    </tr>
                </thead>
                <tbody>
                    {% for f in files %}
                    <tr>
                        <td class="filename-cell">{{ f.name }}</td>
                        <td>{{ f.size }}</td>
                        <td>{{ f.timestamp }}</td>
                        <td>
                            <a href="{{ url_for('download_file', filename=f.name) }}">Download</a>
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        {% else %}
        <p>No PDFs downloaded yet. Click "Run Scraper Again" to start downloading.</p>
        {% endif %}
        
        <h2>📜 Recent Log (Last 150 lines)</h2>
        <div class="log">{{ log_content or "No log entries yet." }}</div>
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
        cloak_url=cloak_url
    )

@app.route("/files/<path:filename>")
def download_file(filename):
    """Serve individual files for download."""
    try:
        return send_from_directory(DATA_DIR, filename, as_attachment=True)
    except Exception as e:
        return f"File not found: {e}", 404

@app.route("/api/metadata")
def api_metadata():
    """API endpoint to get metadata as JSON."""
    metadata = load_metadata()
    return jsonify(metadata)

@app.route("/export/csv")
def export_csv():
    """Export metadata as CSV."""
    metadata = load_metadata()
    
    if not metadata:
        return "No metadata available", 404
    
    # Create CSV in memory
    si = StringIO()
    fieldnames = ['neutral_citation', 'cause_number', 'judgment_date', 'title', 
                  'subject', 'court', 'category', 'file', 'status', 'timestamp']
    writer = csv.DictWriter(si, fieldnames=fieldnames)
    
    writer.writeheader()
    for item in metadata:
        # Only write fields that exist
        row = {k: item.get(k, '') for k in fieldnames}
        writer.writerow(row)
    
    # Create response
    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=judgments_metadata.csv"
    output.headers["Content-type"] = "text/csv"
    
    return output

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
