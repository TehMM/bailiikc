import os
import threading
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from zipfile import ZipFile
from flask import Flask, jsonify, send_from_directory, render_template_string

# --- CONFIG ---
BASE_URL = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"
DATA_DIR = "/app/data/bailii_ky"
ZIP_FILE = "/app/data/bailii_ky/all_cases.zip"

os.makedirs(DATA_DIR, exist_ok=True)

app = Flask(__name__)

# --- GLOBALS ---
scrape_results = []  # list of dicts: {file, status, timestamp}

# --- UTILITY FUNCTIONS ---
def scrape_cases():
    """Download new cases and update report."""
    global scrape_results
    existing_files = set(os.listdir(DATA_DIR))
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/117.0.0.0 Safari/537.36"
    }

    try:
        r = requests.get(BASE_URL, headers=headers)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        links = soup.select("a[href$='.html']")
    except Exception as e:
        return [{"file": "BASE_URL", "status": "ERROR", "error": str(e)}], []

    new_files = []
    results = []
    for a in links:
        href = a["href"]
        filename = os.path.basename(href)
        file_path = os.path.join(DATA_DIR, filename)
        status = "EXISTING"
        if filename not in existing_files:
            status = "NEW"
            try:
                case_url = BASE_URL + href
                case_resp = requests.get(case_url, headers=headers)
                case_resp.raise_for_status()

                # Handle embedded PDFs
                case_soup = BeautifulSoup(case_resp.text, "html.parser")
                pdf_link = case_soup.find("a", href=lambda x: x and x.endswith(".pdf"))
                if pdf_link:
                    pdf_url = BASE_URL + pdf_link["href"]
                    pdf_resp = requests.get(pdf_url, headers=headers)
                    pdf_file_path = os.path.join(DATA_DIR, os.path.basename(pdf_link["href"]))
                    with open(pdf_file_path, "wb") as f:
                        f.write(pdf_resp.content)

                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(case_resp.text)
                new_files.append(filename)
            except Exception as e:
                results.append({"file": filename, "status": "ERROR", "error": str(e)})
                continue
        results.append({"file": filename, "status": status, "timestamp": datetime.now().isoformat()})
    
    # Update global results
    scrape_results = results

    # Create ZIP file
    with ZipFile(ZIP_FILE, 'w') as zipf:
        for f in os.listdir(DATA_DIR):
            zipf.write(os.path.join(DATA_DIR, f), f)

    return results, new_files

def run_scraper_background():
    """Run scraper in a separate thread."""
    thread = threading.Thread(target=scrape_cases)
    thread.start()

# --- ROUTES ---
@app.route("/run-download", methods=["GET"])
def run_download():
    """Trigger scraping via browser."""
    run_scraper_background()
    return "Scraper triggered! Refresh /report after a few seconds to see results."

@app.route("/webhook", methods=["POST"])
def webhook():
    """Trigger scraping via webhook."""
    run_scraper_background()
    return jsonify({"status": "ok", "message": "Scraper triggered via webhook"}), 200

@app.route("/report", methods=["GET"])
def report():
    """Display HTML report of downloaded cases."""
    global scrape_results
    html_template = """
    <html>
    <head>
        <title>Bailii Cases Report</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            table { border-collapse: collapse; width: 100%; }
            th, td { padding: 8px 12px; border: 1px solid #ddd; text-align: left; }
            th { background-color: #f4f4f4; }
            tr.new { background-color: #e0ffe0; }
            tr.existing { background-color: #f9f9f9; }
            .btn { display:inline-block; padding:10px 20px; margin:10px 0; background:#007BFF; color:#fff; text-decoration:none; border-radius:5px; }
            .btn:hover { background:#0056b3; }
        </style>
    </head>
    <body>
        <h1>Bailii Cases Report</h1>
        <a class="btn" href="/download-zip">Download All as ZIP</a>
        <table>
            <tr><th>Case File</th><th>Status</th><th>Timestamp</th><th>Link</th></tr>
            {% for r in results %}
            <tr class="{{ r.status.lower() }}">
                <td>{{ r.file }}</td>
                <td>{{ r.status }}</td>
                <td>{{ r.timestamp }}</td>
                <td><a href="/files/{{ r.file }}" target="_blank">Open</a></td>
            </tr>
            {% endfor %}
        </table>
    </body>
    </html>
    """
    return render_template_string(html_template, results=scrape_results)

@app.route("/files/<path:filename>")
def serve_file(filename):
    """Serve individual downloaded files."""
    file_path = os.path.join(DATA_DIR, filename)
    if os.path.exists(file_path):
        return send_from_directory(DATA_DIR, filename)
    return "You selected a file which is not on our system.", 404

@app.route("/download-zip")
def download_zip():
    """Serve the ZIP archive of all cases."""
    if os.path.exists(ZIP_FILE):
        return send_from_directory(os.path.dirname(ZIP_FILE), os.path.basename(ZIP_FILE), as_attachment=True)
    return "ZIP file not found.", 404

if __name__ == "__main__":
    # Railway uses PORT env variable
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
