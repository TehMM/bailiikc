import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from flask import Flask, send_from_directory, render_template_string, redirect, url_for
from zipfile import ZipFile

# --- Configuration ---
DATA_DIR = "/app/data/bailii_ky"
BASE_URL = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"
ZIP_NAME = "all_pdfs.zip"

os.makedirs(DATA_DIR, exist_ok=True)

app = Flask(__name__)

# --- PDF Scraper ---
def scrape_pdfs():
    results = []
    existing_files = set(os.listdir(DATA_DIR))
    resp = requests.get(BASE_URL)
    soup = BeautifulSoup(resp.text, "html.parser")

    # Step 1: get links to case HTML pages
    case_links = [a["href"] for a in soup.select("a[href$='.html']")]

    for case_html in case_links:
        case_url = BASE_URL + case_html
        case_resp = requests.get(case_url)
        case_soup = BeautifulSoup(case_resp.text, "html.parser")

        # Step 2: find PDF link inside the case page
        pdf_tag = case_soup.find("a", href=lambda x: x and x.endswith(".pdf"))
        if not pdf_tag:
            continue

        pdf_href = pdf_tag["href"]
        pdf_filename = os.path.basename(pdf_href)
        pdf_path = os.path.join(DATA_DIR, pdf_filename)
        status = "EXISTING" if pdf_filename in existing_files else "NEW"

        if status == "NEW":
            pdf_url = BASE_URL + pdf_href
            try:
                r = requests.get(pdf_url)
                r.raise_for_status()
                with open(pdf_path, "wb") as f:
                    f.write(r.content)
            except Exception as e:
                results.append({"file": pdf_filename, "status": "ERROR", "error": str(e)})
                continue

        results.append({
            "file": pdf_filename,
            "status": status,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    return results

# --- Routes ---
@app.route("/run-download", methods=["GET", "POST"])
def run_download():
    pdfs = scrape_pdfs()
    # After scraping, recreate ZIP
    zip_path = os.path.join(DATA_DIR, ZIP_NAME)
    with ZipFile(zip_path, "w") as zipf:
        for filename in os.listdir(DATA_DIR):
            if filename.endswith(".pdf"):
                filepath = os.path.join(DATA_DIR, filename)
                zipf.write(filepath, filename)
    return redirect(url_for("report"))

@app.route("/files/<path:filename>")
def serve_file(filename):
    if os.path.exists(os.path.join(DATA_DIR, filename)):
        return send_from_directory(DATA_DIR, filename)
    return f"You selected a file which is not on our system: {filename}", 404

@app.route("/report")
def report():
    # Build report from current PDFs
    files = [f for f in os.listdir(DATA_DIR) if f.endswith(".pdf")]
    report_rows = ""
    for f in sorted(files):
        filepath = os.path.join(DATA_DIR, f)
        timestamp = datetime.fromtimestamp(os.path.getmtime(filepath)).strftime("%Y-%m-%d %H:%M:%S")
        report_rows += f"""
        <tr>
            <td>{f}</td>
            <td>{'EXISTING'}</td>
            <td>{timestamp}</td>
            <td><a href="/files/{f}" target="_blank">Open</a></td>
        </tr>
        """

    html = f"""
    <html>
    <head><title>Bailii PDF Report</title></head>
    <body>
    <h1>Bailii PDF Report</h1>
    <p><a href="/files/{ZIP_NAME}">Download All as ZIP</a></p>
    <table border="1" cellpadding="5">
        <tr>
            <th>Filename</th>
            <th>Status</th>
            <th>Timestamp</th>
            <th>Open</th>
        </tr>
        {report_rows}
    </table>
    <p><a href="/run-download">Run Scraper Now</a></p>
    </body>
    </html>
    """
    return html

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
