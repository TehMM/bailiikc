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
BASE_URL = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"
ZIP_NAME = "all_pdfs.zip"
SCRAPE_LOG = os.path.join(DATA_DIR, "scrape_log.txt")

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

def scrape_pdfs():
    """Scrape PDFs from Bailii and return a list of dicts with metadata."""
    results = []
    log_message("=" * 60)
    log_message("Starting new scrape session")
    
    try:
        # Get the main page
        log_message(f"Fetching main page: {BASE_URL}")
        r = requests.get(BASE_URL, headers=HEADERS, timeout=10)
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
                full_url = urljoin(BASE_URL, href)
                if full_url not in case_links:
                    case_links.append(full_url)
        
        log_message(f"Found {len(case_links)} potential case pages")
        
        # Visit each case page to find PDF links
        for idx, case_url in enumerate(case_links, 1):
            try:
                log_message(f"[{idx}/{len(case_links)}] Checking: {case_url}")
                time.sleep(0.5)  # Be polite to the server
                
                case_r = requests.get(case_url, headers=HEADERS, timeout=10)
                case_r.raise_for_status()
                case_soup = BeautifulSoup(case_r.text, "html.parser")
                
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
    """Home page with scrape button."""
    html = """
    <html>
    <head><title>Bailii PDF Scraper</title></head>
    <body style="font-family: Arial, sans-serif; margin: 40px;">
    <h1>Bailii PDF Scraper</h1>
    <p>Target: <a href="{{ base_url }}" target="_blank">{{ base_url }}</a></p>
    <form action="{{ url_for('run_download') }}" method="post">
        <button type="submit" style="padding: 10px 20px; font-size: 16px;">
            Run Scraper
        </button>
    </form>
    <br>
    <a href="{{ url_for('report') }}">View Report</a>
    </body>
    </html>
    """
    return render_template_string(html, base_url=BASE_URL)

@app.route("/run-download", methods=["GET", "POST"])
def run_download():
    """Run the scraper and redirect to report."""
    results = scrape_pdfs()
    create_zip()
    return redirect(url_for("report"))

@app.route("/report")
def report():
    """Display report of all PDFs with download links."""
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
