import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from zipfile import ZipFile

# =========================
# Configuration
# =========================
BASE_URL = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"
DATA_DIR = "/app/data/bailii_ky"
REPORT_FILE = os.path.join(DATA_DIR, "report.html")
ZIP_FILE = os.path.join(DATA_DIR, "all_cases.zip")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/117.0.0.0 Safari/537.36"
}

os.makedirs(DATA_DIR, exist_ok=True)

# =========================
# Download Cases
# =========================
def download_cases():
    print("Fetching index page...")
    r = requests.get(BASE_URL, headers=HEADERS)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Grab HTML + PDF links
    links = soup.select("a[href$='.html'], a[href$='.pdf']")

    downloaded = []

    for a in links:
        href = a.get('href')
        filename = href.split('/')[-1]
        file_path = os.path.join(DATA_DIR, filename)
        is_new = False

        # Download only if file is missing
        if not os.path.exists(file_path):
            file_url = BASE_URL + href
            print(f"Downloading {filename} ...")
            file_resp = requests.get(file_url, headers=HEADERS)
            file_resp.raise_for_status()
            with open(file_path, 'wb') as f:
                f.write(file_resp.content)
            is_new = True
        downloaded.append({
            "filename": filename,
            "type": "PDF" if filename.lower().endswith(".pdf") else "HTML",
            "new": is_new,
            "timestamp": datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%d %H:%M:%S')
        })

    return downloaded

# =========================
# Generate ZIP
# =========================
def create_zip(files):
    with ZipFile(ZIP_FILE, 'w') as zipf:
        for f in files:
            zipf.write(os.path.join(DATA_DIR, f["filename"]), arcname=f["filename"])
    print(f"ZIP created at {ZIP_FILE}")

# =========================
# Generate HTML Report
# =========================
def generate_html_report(files):
    html = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Bailii KY Cases</title>
<style>
body { font-family: Arial, sans-serif; margin: 20px; }
.new { color: green; font-weight: bold; }
.existing { color: gray; }
table { border-collapse: collapse; width: 100%; }
th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }
th { background-color: #f0f0f0; }
a.button { display: inline-block; padding: 8px 12px; margin-bottom: 10px; background-color: #007BFF; color: #fff; text-decoration: none; border-radius: 4px; }
</style>
</head>
<body>
<h1>Bailii KY 2025 Cases</h1>
<a class="button" href="all_cases.zip" download>Download All as ZIP</a>
<table>
<tr><th>File</th><th>Type</th><th>Status</th><th>Timestamp</th></tr>
"""
    for f in files:
        status_class = "new" if f["new"] else "existing"
        html += f'<tr><td><a href="{f["filename"]}" target="_blank">{f["filename"]}</a></td>'
        html += f'<td>{f["type"]}</td>'
        html += f'<td class="{status_class}">{"NEW" if f["new"] else "EXISTING"}</td>'
        html += f'<td>{f["timestamp"]}</td></tr>\n'

    html += """
</table>
</body>
</html>
"""
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"HTML report generated at {REPORT_FILE}")

# =========================
# Main
# =========================
if __name__ == "__main__":
    files = download_cases()
    create_zip(files)
    generate_html_report(files)
    print("Done! Open report.html to view all cases and download links.")
