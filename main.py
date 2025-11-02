from flask import Flask, send_from_directory, render_template_string, send_file
import os
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from io import BytesIO
from zipfile import ZipFile

app = Flask(__name__)

DATA_DIR = "/app/data/bailii_ky"
JSON_FILE = os.path.join(DATA_DIR, "downloaded_files.json")
BASE_URL = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"

os.makedirs(DATA_DIR, exist_ok=True)

def load_downloaded_files():
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, "r") as f:
            data = json.load(f)
            # Auto-fix: convert list to dict if needed
            if isinstance(data, list):
                data = {f: "" for f in data}
        return data
    return {}

def save_downloaded_files(downloaded_files):
    with open(JSON_FILE, "w") as f:
        json.dump(downloaded_files, f)

def download_cases():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/117.0.0.0 Safari/537.36"
    }
    r = requests.get(BASE_URL, headers=headers)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    links = soup.select("a[href$='.html']")

    downloaded_files = load_downloaded_files()
    new_files = []

    for a in links:
        filename = a.get("href").split("/")[-1]
        filepath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(filepath):
            resp = requests.get(BASE_URL + filename, headers=headers)
            with open(filepath, "wb") as f:
                f.write(resp.content)
            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            downloaded_files[filename] = timestamp
            new_files.append(filename)

    save_downloaded_files(downloaded_files)
    return downloaded_files, new_files

@app.route("/run-download")
def run_download():
    try:
        downloaded_files, new_files = download_cases()
    except Exception as e:
        return f"Error: {e}"

    # Generate HTML
    html = "<h2>Downloaded Cases</h2><ul>"
    for filename, ts in downloaded_files.items():
        status = "NEW" if filename in new_files else "EXISTING"
        html += f'<li><a href="/cases/{filename}" target="_blank">{filename}</a> - {ts} - {status}</li>'
    html += "</ul>"
    html += '<a href="/download-zip"><button>Download All as ZIP</button></a>'
    return html

@app.route("/cases/<path:filename>")
def serve_case(filename):
    return send_from_directory(DATA_DIR, filename)

@app.route("/download-zip")
def download_zip():
    memory_file = BytesIO()
    with ZipFile(memory_file, "w") as zf:
        for filename in os.listdir(DATA_DIR):
            filepath = os.path.join(DATA_DIR, filename)
            zf.write(filepath, arcname=filename)
    memory_file.seek(0)
    return send_file(memory_file, attachment_filename="bailii_cases.zip", as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
