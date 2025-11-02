import os
import requests
from bs4 import BeautifulSoup
from flask import Flask, render_template_string, send_file
from datetime import datetime
from zipfile import ZipFile
from io import BytesIO

app = Flask(__name__)

# Directory to store downloaded cases
DATA_DIR = "/app/data/bailii_ky"
os.makedirs(DATA_DIR, exist_ok=True)

BAILII_URL = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"

# HTML template for display
HTML_TEMPLATE = """
<!doctype html>
<html>
<head>
    <title>BAILII KY Cases</title>
</head>
<body>
<h1>BAILII KY Cases 2025</h1>
<p>Last run: {{ timestamp }}</p>
<a href="/download-zip"><button>Download All as ZIP</button></a>
<ul>
{% for case in cases %}
    <li>
        <a href="{{ case.href }}" target="_blank">{{ case.name }}</a>
        {% if case.new %}<strong>(NEW)</strong>{% else %}(EXISTING){% endif %}
        - {{ case.time }}
    </li>
{% endfor %}
</ul>
</body>
</html>
"""

@app.route("/run-download")
def run_download():
    # Use headers to avoid 403
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/117.0.0.0 Safari/537.36"
    }

    r = requests.get(BAILII_URL, headers=headers)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    links = soup.select("a[href$='.html']")  # all case HTML files

    downloaded_cases = []

    for a in links:
        case_name = a.get_text(strip=True)
        href = a.get("href")
        local_path = os.path.join(DATA_DIR, href.split('/')[-1])

        if not os.path.exists(local_path):
            # Download new case
            case_url = f"https://www.bailii.org{href}"
            resp = requests.get(case_url, headers=headers)
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                f.write(resp.content)
            is_new = True
        else:
            is_new = False

        downloaded_cases.append({
            "name": case_name,
            "href": f"/files/{href.split('/')[-1]}",
            "new": is_new,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    return render_template_string(HTML_TEMPLATE, cases=downloaded_cases,
                                  timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

@app.route("/files/<filename>")
def serve_file(filename):
    return send_file(os.path.join(DATA_DIR, filename))

@app.route("/download-zip")
def download_zip():
    memory_file = BytesIO()
    with ZipFile(memory_file, 'w') as zf:
        for root, dirs, files in os.walk(DATA_DIR):
            for file in files:
                file_path = os.path.join(root, file)
                zf.write(file_path, arcname=file)
    memory_file.seek(0)
    return send_file(memory_file, download_name="bailii_cases.zip",
                     as_attachment=True)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
