import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from flask import Flask, render_template_string, send_from_directory, redirect, url_for
from zipfile import ZipFile

app = Flask(__name__)

# Config
DATA_DIR = "./data/pdfs"
BASE_URL = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"
ZIP_NAME = "all_pdfs.zip"

os.makedirs(DATA_DIR, exist_ok=True)

def scrape_pdfs():
    """Scrape PDFs from Bailii and return a list of dicts with metadata."""
    results = []
    try:
        r = requests.get(BASE_URL)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        links = soup.find_all("a")
        for link in links:
            href = link.get("href", "")
            if href.lower().endswith(".pdf"):
                pdf_filename = os.path.basename(href)
                pdf_path = os.path.join(DATA_DIR, pdf_filename)
                status = "EXISTING" if os.path.exists(pdf_path) else "NEW"
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if status == "NEW":
                    try:
                        pdf_url = BASE_URL + href
                        pdf_r = requests.get(pdf_url)
                        pdf_r.raise_for_status()
                        with open(pdf_path, "wb") as f:
                            f.write(pdf_r.content)
                    except Exception as e:
                        status = f"ERROR: {e}"
                results.append({
                    "file": pdf_filename,
                    "status": status,
                    "timestamp": timestamp
                })
    except Exception as e:
        results.append({"file": None, "status": f"SCRAPING ERROR: {e}", "timestamp": ""})
    return results

def create_zip():
    """Create a ZIP of all PDFs safely."""
    zip_path = os.path.join(DATA_DIR, ZIP_NAME)
    try:
        with ZipFile(zip_path, "w") as zipf:
            seen = set()
            for filename in os.listdir(DATA_DIR):
                if filename.endswith(".pdf") and filename not in seen:
                    zipf.write(os.path.join(DATA_DIR, filename), filename)
                    seen.add(filename)
    except Exception as e:
        print(f"ZIP creation error: {e}")
    return zip_path

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
            files.append({
                "name": f,
                "status": "EXISTING",
                "timestamp": datetime.fromtimestamp(os.path.getmtime(os.path.join(DATA_DIR, f))).strftime("%Y-%m-%d %H:%M:%S")
            })

    # Sort so NEW appear first
    files.sort(key=lambda x: x["timestamp"], reverse=True)

    zip_path = ZIP_NAME
    html = """
    <html>
    <head><title>Bailii PDF Report</title></head>
    <body>
    <h1>Bailii PDF Report</h1>
    <a href="{{ zip_path }}">Download All as ZIP</a>
    <table border="1" cellpadding="5" cellspacing="0">
    <tr><th>Filename</th><th>Status</th><th>Timestamp</th><th>Open</th></tr>
    {% for f in files %}
    <tr>
        <td>{{ f.name }}</td>
        <td>{{ f.status }}</td>
        <td>{{ f.timestamp }}</td>
        <td><a href="{{ url_for('download_file', filename=f.name) }}">Open</a></td>
    </tr>
    {% endfor %}
    </table>
    </body>
    </html>
    """
    return render_template_string(html, files=files, zip_path=zip_path)

@app.route("/files/<path:filename>")
def download_file(filename):
    try:
        return send_from_directory(DATA_DIR, filename, as_attachment=True)
    except Exception as e:
        return f"File not found: {e}", 404

if __name__ == "__main__":
    # Railway expects 0.0.0.0 and correct port
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
