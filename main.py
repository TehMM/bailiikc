import os
import json
import requests
from bs4 import BeautifulSoup
from flask import Flask, send_from_directory

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

# Ensure download folder exists
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# Load previously downloaded files
if os.path.exists(STATE_FILE):
    with open(STATE_FILE, "r") as f:
        downloaded_files = set(json.load(f))
else:
    downloaded_files = set()


def save_state():
    with open(STATE_FILE, "w") as f:
        json.dump(list(downloaded_files), f)


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

            downloaded_files.add(href)
            new_downloads.append(href)
        except Exception as e:
            print(f"Failed to download {case_url}: {e}")

    save_state()
    return new_downloads


@app.route("/run-download")
def run_download():
    try:
        new_files = download_cases()
        existing_files = sorted(downloaded_files - set(new_files))

        # HTML lists
        new_html = "<ul>" + "".join(f"<li>{f}</li>" for f in new_files) + "</ul>" if new_files else "<p>No new cases</p>"
        existing_html = "<ul>" + "".join(f"<li><a href='/files/{f}' target='_blank'>{f}</a></li>" for f in existing_files) + "</ul>" if existing_files else "<p>No existing cases</p>"

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
        </body>
        </html>
        """
        return html_template
    except Exception as e:
        return f"<p style='color:red;'>Error: {e}</p>", 500


@app.route("/files/<path:filename>")
def serve_file(filename):
    """Serve downloaded case files from the volume."""
    return send_from_directory(DOWNLOAD_FOLDER, filename, as_attachment=True)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)