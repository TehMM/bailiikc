import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from zipfile import ZipFile
from flask import Flask, request, jsonify
import threading

# === Configuration ===
DATA_DIR = "/app/data/bailii_ky"
REPORT_FILE = os.path.join(DATA_DIR, "report.html")
ZIP_FILE = os.path.join(DATA_DIR, "all_cases.zip")
BASE_URL = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"

# Ensure directories exist
os.makedirs(DATA_DIR, exist_ok=True)

# Flask app (for webhook trigger)
app = Flask(__name__)

def scrape_cases():
    """Scrape new cases, generate report and ZIP safely."""
    existing_files = set(os.listdir(DATA_DIR))
    new_cases = []
    all_cases = []

    # Example: scrape index page (adjust CSS selector as needed)
    index_url = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"
    resp = requests.get(index_url)
    soup = BeautifulSoup(resp.content, "html.parser")

    # Collect links to cases (adjust selector)
    links = soup.select("pre a")  # or whatever matches the links

    for a in links:
        href = a.get("href")
        if not href.endswith(".html") and not href.endswith(".pdf"):
            continue
        filename = os.path.basename(href)
        filepath = os.path.join(DATA_DIR, filename)
        status = "EXISTING" if filename in existing_files else "NEW"

        # Only download if new
        if status == "NEW":
            try:
                file_url = href if href.startswith("http") else f"https://www.bailii.org{href}"
                r = requests.get(file_url)
                with open(filepath, "wb") as f:
                    f.write(r.content)
            except Exception as e:
                print(f"Failed to download {filename}: {e}")
                continue

        # Track case for report/ZIP
        all_cases.append({
            "filename": filename,
            "status": status,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        if status == "NEW":
            new_cases.append(filename)

    # === Create ZIP safely ===
    added = set()
    try:
        with ZipFile(ZIP_FILE, "w") as zipf:
            for case in all_cases:
                path = os.path.join(DATA_DIR, case["filename"])
                if case["filename"] not in added and os.path.exists(path):
                    zipf.write(path, arcname=case["filename"])
                    added.add(case["filename"])
    except Exception as e:
        print("Error creating ZIP:", e)

    # === Generate simple HTML report ===
    html_lines = ["<html><body><h1>Bailii Cases Report</h1><ul>"]
    for case in all_cases:
        html_lines.append(
            f'<li>{case["timestamp"]} - <b>{case["status"]}</b> - '
            f'<a href="/files/{case["filename"]}">{case["filename"]}</a></li>'
        )
    html_lines.append(f'</ul><p><a href="/download-zip">Download All as ZIP</a></p>')
    html_lines.append("</body></html>")

    with open(REPORT_FILE, "w") as f:
        f.write("\n".join(html_lines))

    print(f"Scraping complete. {len(new_cases)} new cases downloaded.")
    return new_cases

# === Flask webhook endpoint ===
@app.route("/webhook", methods=["POST"])
def webhook_trigger():
    """Triggered by ChangeDetection webhook."""
    # Run scraping in a background thread so HTTP responds immediately
    threading.Thread(target=scrape_cases).start()
    return jsonify({"status": "Scraping started"}), 202

# Serve report and files
@app.route("/report", methods=["GET"])
def serve_report():
    if os.path.exists(REPORT_FILE):
        with open(REPORT_FILE, "r") as f:
            return f.read()
    return "Report not generated yet.", 404

@app.route("/files/<path:filename>", methods=["GET"])
def serve_file(filename):
    filepath = os.path.join(DATA_DIR, filename)
    if os.path.exists(filepath):
        return app.send_static_file(filepath)
    return "File not found", 404

@app.route("/download-zip", methods=["GET"])
def download_zip():
    if os.path.exists(ZIP_FILE):
        return app.send_static_file(ZIP_FILE)
    return "ZIP not found", 404

if __name__ == "__main__":
    # Run Flask on port 8080 for Railway compatibility
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
