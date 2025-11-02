from flask import Flask, jsonify
import os
import requests
from bs4 import BeautifulSoup

# -----------------------
# Configuration
# -----------------------
INDEX_URL = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"
DOWNLOAD_FOLDER = "/app/data/bailii_ky"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
}

# Ensure download folder exists
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# -----------------------
# Flask App
# -----------------------
app = Flask(__name__)

@app.route("/run-download", methods=["GET"])
def run_download():
    """Endpoint to trigger download of all case files"""
    try:
        r = requests.get(INDEX_URL, headers=HEADERS)
        r.raise_for_status()
    except Exception as e:
        return jsonify({"status": "error", "message": f"Failed to fetch index: {e}"}), 500

    soup = BeautifulSoup(r.text, "html.parser")
    links = soup.select("a[href$='.html']")  # all HTML case links

    downloaded = []
    for a in links:
        case_url = a.get("href")
        case_name = case_url.split("/")[-1]
        full_path = os.path.join(DOWNLOAD_FOLDER, case_name)

        # Skip if already exists
        if os.path.exists(full_path):
            continue

        # Construct full URL
        if not case_url.startswith("http"):
            case_url = os.path.join("https://www.bailii.org", case_url.lstrip("/"))

        try:
            resp = requests.get(case_url, headers=HEADERS)
            resp.raise_for_status()
            with open(full_path, "w", encoding="utf-8") as f:
                f.write(resp.text)
            downloaded.append(case_name)
        except Exception as e:
            print(f"Failed to download {case_name}: {e}")

    return jsonify({
        "status": "success",
        "total_cases": len(links),
        "downloaded": downloaded
    })

# -----------------------
# Run Flask
# -----------------------
if __name__ == "__main__":
    # Port 5000 internally; Railway will map to 8080
    app.run(host="0.0.0.0", port=5000)