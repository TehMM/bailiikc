import os
import csv
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from flask import Flask, render_template_string, send_from_directory, redirect, url_for
from zipfile import ZipFile
from urllib.parse import urlparse
from io import StringIO
import time

app = Flask(__name__)

# ------------------------------
# Configuration
# ------------------------------
DATA_DIR = "./data/pdfs"
os.makedirs(DATA_DIR, exist_ok=True)

CSV_URL = "https://judicial.ky/wp-content/uploads/box_files/judgments.csv"
ZIP_NAME = "all_pdfs.zip"
SCRAPE_LOG = os.path.join(DATA_DIR, "scrape_log.txt")
SCRAPED_URLS_FILE = os.path.join(DATA_DIR, "scraped_urls.txt")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

# ------------------------------
# Utility functions
# ------------------------------
def log_message(message: str):
    """Log messages to both console and file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    print(log_entry.strip())
    with open(SCRAPE_LOG, "a", encoding="utf-8") as f:
        f.write(log_entry)


def load_scraped_urls():
    """Load URLs that have been previously scraped."""
    if os.path.exists(SCRAPED_URLS_FILE):
        with open(SCRAPED_URLS_FILE, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def save_scraped_url(url: str):
    """Append a scraped URL to the log file."""
    with open(SCRAPED_URLS_FILE, "a", encoding="utf-8") as f:
        f.write(f"{url}\n")


def create_zip():
    """Bundle all PDFs into one ZIP."""
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


# ------------------------------
# Main scraping logic
# ------------------------------
def scrape_pdfs():
    """Scrape judicial.ky PDFs using the official CSV feed."""
    results = []
    log_message("=" * 60)
    log_message("Starting scrape session for judicial.ky (CSV-based)")

    scraped_urls = load_scraped_urls()
    log_message(f"Loaded {len(scraped_urls)} previously scraped URLs")

    try:
        # Download the CSV
        r = requests.get(CSV_URL, headers=HEADERS, timeout=30)
        r.raise_for_status()
        csv_text = r.text

        reader = csv.DictReader(StringIO(csv_text))
        rows = list(reader)
        log_message(f"Fetched {len(rows)} rows from CSV")

        for idx, row in enumerate(rows, 1):
            pdf_url = row.get("Url") or row.get("File") or ""
            if not pdf_url or not pdf_url.lower().endswith(".pdf"):
                continue

            display_url = f"http://anon.to/?{pdf_url}"

            if pdf_url in scraped_urls:
                log_message(f"[{idx}/{len(rows)}] Skipping previously scraped: {pdf_url}")
                continue

            pdf_filename = os.path.basename(urlparse(pdf_url).path).replace("/", "_")
            pdf_path = os.path.join(DATA_DIR, pdf_filename)
            status = "EXISTING"

            if not os.path.exists(pdf_path):
                try:
                    log_message(f"[{idx}/{len(rows)}] ↓ Downloading {pdf_filename}")
                    pdf_r = requests.get(pdf_url, headers=HEADERS, timeout=30)
                    pdf_r.raise_for_status()

                    if not pdf_r.content.startswith(b"%PDF"):
                        log_message(f"  ✗ Not a valid PDF: {pdf_filename}")
                        continue

                    with open(pdf_path, "wb") as f:
                        f.write(pdf_r.content)
                    log_message(f"  ✓ Saved {pdf_filename} ({len(pdf_r.content)} bytes)")
                    status = "NEW"
                except Exception as e:
                    log_message(f"  ✗ Error downloading {pdf_filename}: {e}")
                    status = f"ERROR: {e}"

            save_scraped_url(pdf_url)
            results.append({
                "file": pdf_filename,
                "status": status,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "url": display_url,
            })

        log_message(f"Scraping complete. Found {len(results)} PDFs.")

    except Exception as e:
        error_msg = f"SCRAPING ERROR: {e}"
        log_message(error_msg)
        results.append({
            "file": "ERROR",
            "status": error_msg,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "url": "",
        })

    return results


# ------------------------------
# Flask Routes
# ------------------------------
@app.route("/")
def index():
    html = """
    <html>
    <head><title>Judicial.ky PDF Scraper</title></head>
    <body style="font-family: Arial, sans-serif; margin: 40px;">
        <h1>Judicial.ky PDF Scraper</h1>
        <p>Source CSV: <a href="{{ csv_url }}" target="_blank">{{ csv_url }}</a></p>
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
    return render_template_string(html, csv_url=CSV_URL)


@app.route("/run-download", methods=["GET", "POST"])
def run_download():
    results = scrape_pdfs()
    create_zip()
    return redirect(url_for("report"))


@app.route("/report")
def report():
    files = []
    for f in os.listdir(DATA_DIR):
        if f.endswith(".pdf"):
            file_path = os.path.join(DATA_DIR, f)
            files.append({
                "name": f,
                "status": "EXISTING",
                "timestamp": datetime.fromtimestamp(os.path.getmtime(file_path)).strftime("%Y-%m-%d %H:%M:%S"),
                "size": f"{os.path.getsize(file_path) / 1024:.1f} KB",
            })

    files.sort(key=lambda x: x["timestamp"], reverse=True)
    zip_exists = os.path.exists(os.path.join(DATA_DIR, ZIP_NAME))

    log_content = ""
    if os.path.exists(SCRAPE_LOG):
        with open(SCRAPE_LOG, "r", encoding="utf-8") as f:
            lines = f.readlines()
            log_content = "".join(lines[-50:])

    html = """
    <html>
    <head>
        <title>Judicial.ky PDF Report</title>
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
        <h1>Judicial.ky PDF Report</h1>
        <p><strong>Total PDFs:</strong> {{ files|length }}</p>

        <div>
            <a href="{{ url_for('index') }}" class="button">Home</a>
            {% if zip_exists %}
            <a href="{{ url_for('download_file', filename=zip_name) }}" class="button">Download All as ZIP</a>
            {% endif %}
            <a href="{{ url_for('run_download') }}" class="button" onclick="return confirm('Start new scrape?')">Run Scraper Again</a>
        </div>

        <h2>Downloaded PDFs</h2>
        {% if files %}
        <table>
            <tr><th>Filename</th><th>Size</th><th>Downloaded</th><th>Actions</th></tr>
            {% for f in files %}
            <tr>
                <td>{{ f.name }}</td>
                <td>{{ f.size }}</td>
                <td>{{ f.timestamp }}</td>
                <td><a href="{{ url_for('download_file', filename=f.name) }}">Download</a></td>
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
    return render_template_string(html, files=files, zip_exists=zip_exists, zip_name=ZIP_NAME, log_content=log_content)


@app.route("/files/<path:filename>")
def download_file(filename):
    try:
        return send_from_directory(DATA_DIR, filename, as_attachment=True)
    except Exception as e:
        return f"File not found: {e}", 404


# ------------------------------
# Flask Entry Point
# ------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
