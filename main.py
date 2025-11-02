# main.py
from flask import Flask, request, jsonify
import os
import requests
from bs4 import BeautifulSoup

app = Flask(__name__)

# Directory to save downloaded HTML files
DOWNLOAD_DIR = "/app/data/bailii_ky"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Bailii URL to scrape
BAILII_URL = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"

# User-Agent header to avoid 403
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
}

def download_all_cases():
    """
    Downloads all HTML cases from Bailii and saves them to DOWNLOAD_DIR
    """
    print(f"Fetching index: {BAILII_URL}")
    r = requests.get(BAILII_URL, headers=HEADERS)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    # Only links ending in .html (individual cases)
    links = soup.select("a[href$='.html']")

    print(f"Found {len(links)} cases")
    for link in links:
        href = link.get("href")
        filename = os.path.basename(href)
        file_path = os.path.join(DOWNLOAD_DIR, filename)

        # Bailii uses relative URLs
        file_url = requests.compat.urljoin(BAILII_URL, href)

        print(f"Downloading {file_url} → {file_path}")
        try:
            case_r = requests.get(file_url, headers=HEADERS)
            case_r.raise_for_status()
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(case_r.text)
        except Exception as e:
            print(f"Failed to download {file_url}: {e}")

@app.route("/run-download", methods=["POST"])
def run_download():
    """
    Trigger endpoint for HTTP POST requests
    """
    try: