import os
import json
import requests
from bs4 import BeautifulSoup
from flask import Flask, send_from_directory, send_file, request
from datetime import datetime
from zipfile import ZipFile
from io import BytesIO

app = Flask(__name__)

# --- Configuration ---
BASE_URL = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"
DOWNLOAD_FOLDER = "/app/data/bailii_ky"  # Railway volume mount
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/117.0.0.0 Safari/537.36"
}
STATE_FILE = os.path.join(DOWNLOAD_FOLDER, "downloaded_files.json")

os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# Load previous state
if os.path.exists(STATE_FILE):
    with open(STATE_FILE, "r") as f:
        downloaded_files = json.load(f)  # { "filename": "timestamp" }
else:
    downloaded_files = {}

def save_state():
    with open(STATE_FILE, "w") as f:
        json.dump(downloaded_files, f)


def download_cases():
    """Download new cases and return list of new downloads."""
    r = requests.get(BASE_URL, headers=HEADERS)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    links = soup.select("a[href$='.html']")  # All HTML case links

    new_downloads = []
    for a in links:
        href = a.get("href")
        case_url = BASE_URL + href
        local_path = os.path.join(DOWNLOAD_FOLDER, href)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        if href in downloaded_files:
            continue  # Skip already downloaded

        try:
            resp = requests.get(case_url, headers=HEADERS)
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                f.write(resp.content)

            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            downloaded_files[href] = timestamp
            new_downloads.append(href)
        except Exception as e:
            print(f"Failed to download {case_url}: {e}")

    save_state()
    return new_downloads


@app.route("/run-download")
def run_download():
    try:
        new_files = download_cases()
        all_files = sorted(downloaded_files.keys())
        existing_files = [f for f in all_files if f not in new_files]

        # Build HTML lists with timestamps
        def make_list(files, color="black"):
            if not files:
                return "<p>None</p>"
            html = "<ul>"
            for f in files:
                ts = downloaded_files.get(f, "")
                html += f"<li><a href='/files/{f}' target='_blank'>{f}</a> - <em>{ts}</em></li>"
            html += "</ul>"
            return html

        new_html = make_list(new_files, color="green")
        existing_html = make_list(existing_files)

        html_template = f"""
        <html>
        <head>
            <title>Bailii KY Download Status</title>
        </head>
        <body>
            <h1 style='color:green;'>Newly Downloaded Cases</h1>
            {new_html}

            <h2>Previously Downloaded Cases</h2>
            {existing_html}

            <form action="/download-all-zip" method="get">
                <button type="submit">Download All Cases as ZIP</button>
            </form>
        </body>
        </html>
        """
        return html_template
    except Exception as e:
        return f"<p style='color:red;'>Error: {e}</p>", 500


@app.route("/files/<path:filename>")
def serve_file(filename):
    """Serve individual case files."""
    return send_from_directory(DOWNLOAD_FOLDER, filename, as_attachment=True)


@app.route("/download-all-zip")
def download_all_zip():
    """Return a zip of all downloaded cases."""
    try:
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, "w") as zipf:
            for root, _, files in os.walk(DOWNLOAD_FOLDER):
                for file in files:
                    if file == "downloaded_files.json":
                        continue
                    abs_path = os.path.join(root, file)
                    rel_path = os.path.relpath(abs_path, DOWNLOAD_FOLDER)
                    zipf.write(abs_path, arcname=rel_path)
        zip_buffer.seek(0)
        return send_file(zip_buffer, mimetype="application/zip", as_attachment=True, download_name="bailii_cases.zip")
    except Exception as e:
        return f"<p style='color:red;'>Error creating ZIP: {e}</p>", 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
