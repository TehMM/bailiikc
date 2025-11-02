import os
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify

app = Flask(__name__)

# --- Configuration ---
BASE_URL = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"
DOWNLOAD_FOLDER = "/app/data/bailii_ky"  # Ensure this is mounted to a Railway volume
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/117.0.0.0 Safari/537.36"
}

# Ensure download folder exists
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

def download_cases():
    r = requests.get(BASE_URL, headers=HEADERS)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    links = soup.select("a[href$='.html']")  # All HTML case links

    downloaded = []
    for a in links:
        href = a.get("href")
        case_url = BASE_URL + href
        local_path = os.path.join(DOWNLOAD_FOLDER, href)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        if os.path.exists(local_path):
            continue  # Skip if already downloaded

        try:
            resp = requests.get(case_url, headers=HEADERS)
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                f.write(resp.content)
            downloaded.append(href)
        except Exception as e:
            print(f"Failed to download {case_url}: {e}")

    return downloaded

@app.route("/run-download", methods=["GET"])
def run_download():
    try:
        downloaded_files = download_cases()
        return jsonify({
            "status": "success",
            "downloaded_count": len(downloaded_files),
            "files": downloaded_files
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == "__main__":
    # Use Railway's PORT env variable, default to 5000 if not set
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)