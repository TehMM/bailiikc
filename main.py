import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from flask import Flask, render_template_string, send_from_directory, request, send_file
import zipfile

BASE_URL = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"
DATA_DIR = "/app/data/bailii_ky"

app = Flask(__name__)

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def scrape_pdfs():
    results = []
    existing_files = set(os.listdir(DATA_DIR))
    resp = requests.get(BASE_URL)
    soup = BeautifulSoup(resp.text, "html.parser")
    links = soup.select("a[href$='.pdf']")  # grab only PDFs

    for a in links:
        href = a["href"]
        pdf_filename = os.path.basename(href)
        pdf_path = os.path.join(DATA_DIR, pdf_filename)
        status = "EXISTING" if pdf_filename in existing_files else "NEW"

        if status == "NEW":
            pdf_url = BASE_URL + href
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
            "timestamp": datetime.now().isoformat()
        })
    return results

@app.route("/run-download", methods=["GET", "POST"])
def run_download():
    results = scrape_pdfs()
    html = """
    <h2>Bailii PDF Report</h2>
    <a href="/download-all">Download All as ZIP</a>
    <table border=1>
        <tr><th>Filename</th><th>Status</th><th>Timestamp</th><th>Open</th></tr>
        {% for r in results %}
        <tr style="color: {{ 'green' if r.status=='NEW' else 'black' }}">
            <td>{{ r.file }}</td>
            <td>{{ r.status }}</td>
            <td>{{ r.timestamp }}</td>
            <td><a href="/files/{{ r.file }}" target="_blank">Open</a></td>
        </tr>
        {% endfor %}
    </table>
    """
    return render_template_string(html, results=results)

@app.route("/files/<path:filename>")
def serve_file(filename):
    return send_from_directory(DATA_DIR, filename, as_attachment=True)

@app.route("/download-all")
def download_all():
    zip_path = os.path.join(DATA_DIR, "all_pdfs.zip")
    with zipfile.ZipFile(zip_path, "w") as zf:
        for f in os.listdir(DATA_DIR):
            if f.endswith(".pdf"):
                zf.write(os.path.join(DATA_DIR, f), f)
    return send_file(zip_path, as_attachment=True)

# webhook trigger
@app.route("/webhook", methods=["POST"])
def webhook():
    if request.method == "POST":
        scrape_pdfs()
        return "Webhook triggered.", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
