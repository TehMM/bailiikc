import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from flask import Flask, render_template_string, send_from_directory, redirect, url_for
from zipfile import ZipFile
from urllib.parse import urljoin, urlparse
import time

app = Flask(__name__)

# Config
DATA_DIR = "./data/pdfs"
DEFAULT_BASE_URL = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"
ZIP_NAME = "all_pdfs.zip"
SCRAPE_LOG = os.path.join(DATA_DIR, "scrape_log.txt")
SCRAPED_URLS_FILE = os.path.join(DATA_DIR, "scraped_urls.txt")
CONFIG_FILE = os.path.join(DATA_DIR, "config.txt")

os.makedirs(DATA_DIR, exist_ok=True)

# Headers to mimic a real browser and avoid 403 errors
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Cache-Control': 'max-age=0'
}

def log_message(message):
    """Log scraping messages to file and console."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    print(log_entry.strip())
    with open(SCRAPE_LOG, "a") as f:
        f.write(log_entry)

def load_scraped_urls():
    """Load the set of previously scraped URLs."""
    if os.path.exists(SCRAPED_URLS_FILE):
        with open(SCRAPED_URLS_FILE, "r") as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def save_scraped_url(url):
    """Save a successfully scraped URL to the tracking file."""
    with open(SCRAPED_URLS_FILE, "a") as f:
        f.write(f"{url}\n")

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

def scrape_pdfs(base_url=None):
    """Scrape PDFs from Bailii and return a list of dicts with metadata."""
    results = []
    log_message("=" * 60)
    log_message("Starting new scrape session")
    
    # Load previously scraped URLs
    scraped_urls = load_scraped_urls()
    log_message(f"Loaded {len(scraped_urls)} previously scraped URLs")
    
    # Determine which base URL to use
    if not base_url:
        base_url = load_base_url()
    log_message(f"Fetching main page: {base_url}")
    
    try:
        # Get the main page
        r = requests.get(base_url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Find all case links (typically in a list or table)
        links = soup.find_all("a", href=True)
        case_links = []
        
        for link in links:
            href = link.get("href", "")
            # Filter for case links (usually end with .html or are relative paths)
            if href and not href.startswith("http") and not href.startswith("#"):
                # Skip parent directory links
                if href == "../" or href == "./":
                    continue
                # Construct full URL
                full_url = urljoin(base_url, href)
                if full_url not in case_links:
                    case_links.append(full_url)
        
        log_message(f"Found {len(case_links)} potential case pages")
        
        # Filter out already scraped URLs
        new_case_links = [url for url in case_links if url not in scraped_urls]
        skipped_count = len(case_links) - len(new_case_links)
        
        if skipped_count > 0:
            log_message(f"Skipping {skipped_count} previously scraped pages")
        log_message(f"Will check {len(new_case_links)} new/unscraped pages")
        
        # Visit each case page to find PDF links
        for idx, case_url in enumerate(new_case_links, 1):
            try:
                log_message(f"[{idx}/{len(new_case_links)}] Checking: {case_url}")
                time.sleep(0.5)  # Be polite to the server
                
                case_r = requests.get(case_url, headers=HEADERS, timeout=10)
                case_r.raise_for_status()
                case_soup = BeautifulSoup(case_r.text, "html.parser")
                
                # Track if we found any PDFs on this page
                pdfs_found_on_page = 0
                
                # Look for PDF links on the case page (both <a> tags and <object> tags)
                pdf_sources = []
                
                # Method 1: Find all <a> tags with PDF hrefs
                pdf_links = case_soup.find_all("a", href=True)
                for pdf_link in pdf_links:
                    pdf_href = pdf_link.get("href", "")
                    if pdf_href.lower().endswith(".pdf"):
                        pdf_sources.append(pdf_href)
                
                # Method 2: Find all <object> tags with PDF data attributes
                pdf_objects = case_soup.find_all("object", {"type": "application/pdf"})
                for pdf_obj in pdf_objects:
                    pdf_data = pdf_obj.get("data", "")
                    if pdf_data:
                        pdf_sources.append(pdf_data)
                
                # Remove duplicates while preserving order
                seen = set()
                unique_sources = []
                for src in pdf_sources:
                    if src not in seen:
                        seen.add(src)
                        unique_sources.append(src)
                
                # Download each unique PDF
                for pdf_href in unique_sources:
                    pdf_url = urljoin(case_url, pdf_href)
                    pdf_filename = os.path.basename(urlparse(pdf_url).path)
                        
                    # Sanitize filename
                    pdf_filename = pdf_filename.replace("/", "_").replace("\\", "_")
                    
                    # Skip if filename is empty or invalid
                    if not pdf_filename or pdf_filename == ".pdf":
                        continue
                    
                    pdf_path = os.path.join(DATA_DIR, pdf_filename)
                    
                    # Check if already downloaded
                    if os.path.exists(pdf_path):
                        status = "EXISTING"
                        log_message(f"  ✓ Already have: {pdf_filename}")
                    else:
                        status = "NEW"
                        try:
                            log_message(f"  ↓ Downloading: {pdf_filename}")
                            pdf_r = requests.get(pdf_url, headers=HEADERS, timeout=30)
                            pdf_r.raise_for_status()
                            
                            # Verify it's actually a PDF
                            if pdf_r.content[:4] != b'%PDF':
                                log_message(f"  ✗ Not a valid PDF: {pdf_filename}")
                                continue
                            
                            with open(pdf_path, "wb") as f:
                                f.write(pdf_r.content)
                            
                            log_message(f"  ✓ Saved: {pdf_filename} ({len(pdf_r.content)} bytes)")
                        except Exception as e:
                            status = f"ERROR: {e}"
                            log_message(f"  ✗ Error downloading {pdf_filename}: {e}")
                    
                    results.append({
                        "file": pdf_filename,
                        "status": status,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "url": pdf_url
                    })
                    pdfs_found_on_page += 1
                
                # Mark this URL as successfully scraped (even if no PDFs found)
                save_scraped_url(case_url)
                if pdfs_found_on_page > 0:
                    log_message(f"  Found {pdfs_found_on_page} PDF(s) on this page")
                else:
                    log_message(f"  No PDFs found on this page")
                
            except Exception as e:
                log_message(f"  ✗ Error accessing case page: {e}")
                continue
        
        log_message(f"Scraping complete. Found {len(results)} PDFs")
        
    except Exception as e:
        error_msg = f"SCRAPING ERROR: {e}"
        log_message(error_msg)
        results.append({
            "file": "ERROR",
            "status": error_msg,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "url": ""
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
        <title>Bailii PDF Scraper</title>
        <style>
            body { 
                font-family: Arial, sans-serif; 
                margin: 40px;
                max-width: 800px;
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
            .examples {
                background-color: #f9f9f9;
                padding: 15px;
                border-radius: 4px;
                margin-top: 10px;
            }
            .examples ul {
                margin: 10px 0;
                padding-left: 20px;
            }
        </style>
    </head>
    <body>
        <h1>🔍 Bailii PDF Scraper</h1>
        
        <form action="{{ url_for('update_config') }}" method="post">
            <div class="form-group">
                <label for="base_url">Base URL to Scrape:</label>
                <input 
                    type="url" 
                    id="base_url" 
                    name="base_url" 
                    value="{{ current_url }}"
                    placeholder="https://www.bailii.org/ky/cases/GCCI/FSD/2025/"
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
            <a href="{{ current_url }}" target="_blank">{{ current_url }}</a>
        </div>
        
        <form action="{{ url_for('run_download') }}" method="post" style="margin-top: 20px;">
            <button type="submit" class="button" style="font-size: 18px; padding: 15px 30px;">
                ▶️ Run Scraper Now
            </button>
        </form>
        
        <p style="margin-top: 20px;">
            <a href="{{ url_for('report') }}">📊 View Report & Downloads</a>
        </p>
        
        <div class="examples">
            <strong>💡 Example URLs:</strong>
            <ul>
                <li><code>https://www.bailii.org/ky/cases/GCCI/FSD/2025/</code> - Cayman Islands 2025</li>
                <li><code>https://www.bailii.org/ky/cases/GCCI/FSD/2024/</code> - Cayman Islands 2024</li>
                <li><code>https://www.bailii.org/ew/cases/EWHC/</code> - England & Wales High Court</li>
                <li><code>https://www.bailii.org/uk/cases/UKSC/</code> - UK Supreme Court</li>
            </ul>
            <p><small>⚠️ Make sure URL ends with a trailing slash (/) for proper scraping</small></p>
        </div>
        
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
        default_url=DEFAULT_BASE_URL
    )

@app.route("/update-config", methods=["POST"])
def update_config():
    """Update the base URL configuration."""
    from flask import request
    new_url = request.form.get("base_url", "").strip()
    
    if new_url:
        # Ensure URL ends with /
        if not new_url.endswith("/"):
            new_url += "/"
        
        save_base_url(new_url)
        log_message(f"Configuration updated: Base URL changed to {new_url}")
    
    return redirect(url_for("index"))

@app.route("/run-download", methods=["GET", "POST"])
def run_download():
    """Run the scraper and redirect to report."""
    from flask import request
    
    # Check if a custom URL was provided (for API/changedetection.io use)
    custom_url = request.args.get("url") or request.form.get("url")
    
    if custom_url:
        custom_url = custom_url.strip()
        if not custom_url.endswith("/"):
            custom_url += "/"
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
                "status": "EXISTING",
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
            # Get last 50 lines
            log_content = "".join(log_lines[-50:])
    
    html = """
    <html>
    <head>
        <title>Bailii PDF Report</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            table { border-collapse: collapse; width: 100%; margin-top: 20px; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #4CAF50; color: white; }
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
            .log { 
                background-color: #f5f5f5; 
                padding: 10px; 
                border: 1px solid #ddd;
                max-height: 300px;
                overflow-y: scroll;
                font-family: monospace;
                font-size: 12px;
                white-space: pre-wrap;
            }
        </style>
    </head>
    <body>
        <h1>Bailii PDF Report</h1>
        <p><strong>Current Target:</strong> <a href="{{ current_url }}" target="_blank">{{ current_url }}</a></p>
        <p><strong>Total PDFs:</strong> {{ files|length }}</p>
        
        <div>
            <a href="{{ url_for('index') }}" class="button">Home</a>
            {% if zip_exists %}
            <a href="{{ url_for('download_file', filename=zip_name) }}" class="button">
                Download All as ZIP
            </a>
            {% endif %}
            <a href="{{ url_for('run_download') }}" class="button" 
               onclick="return confirm('Start new scrape?')">
                Run Scraper Again
            </a>
        </div>
        
        <h2>Downloaded PDFs</h2>
        {% if files %}
        <table>
            <tr>
                <th>Filename</th>
                <th>Size</th>
                <th>Downloaded</th>
                <th>Actions</th>
            </tr>
            {% for f in files %}
            <tr>
                <td>{{ f.name }}</td>
                <td>{{ f.size }}</td>
                <td>{{ f.timestamp }}</td>
                <td>
                    <a href="{{ url_for('download_file', filename=f.name) }}">Download</a>
                </td>
            </tr>
            {% endfor %}
        </table>
        {% else %}
        <p>No PDFs found. Run the scraper to download files.</p>
        {% endif %}
        
        <h2>Recent Log</h2>
        <div class="log">{{ log_content or "No log entries yet." }}</div>
    </body>
    </html>
    """
    return render_template_string(
        html, 
        files=files, 
        zip_exists=zip_exists,
        zip_name=ZIP_NAME,
        log_content=log_content
    )

@app.route("/files/<path:filename>")
def download_file(filename):
    """Serve individual files for download."""
    try:
        return send_from_directory(DATA_DIR, filename, as_attachment=True)
    except Exception as e:
        return f"File not found: {e}", 404

if __name__ == "__main__":
    # Railway expects 0.0.0.0 and correct port
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
