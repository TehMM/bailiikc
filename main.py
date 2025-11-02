from flask import Flask, jsonify
import os, requests
from bs4 import BeautifulSoup

app = Flask(__name__)

DATA_DIR = "/app/data/bailii_ky"
os.makedirs(DATA_DIR, exist_ok=True)

INDEX_URL = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"

@app.route("/")
def status():
    files = os.listdir(DATA_DIR)
    return jsonify({"status": "running", "files_count": len(files)})

@app.route("/run-download", methods=["POST"])
def run_download():
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(INDEX_URL, headers=headers)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        links = soup.select("a[href$='.html']")
        downloaded = 0

        for a in links:
            filename = a.get("href").strip("/").replace("/", "_")
            path = os.path.join(DATA_DIR, filename)
            if os.path.exists(path):
                continue
            file_url = INDEX_URL.rstrip("/") + "/" + a.get("href")
            resp = requests.get(file_url, headers=headers)
            with open(path, "w", encoding="utf-8") as f:
                f.write(resp.text)
            downloaded += 1

        return jsonify({"downloaded": downloaded, "total_files": len(os.listdir(DATA_DIR))})
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)