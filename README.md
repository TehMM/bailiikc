# Cayman Judicial PDF Scraper & Dashboard

This project provides a production-ready Flask dashboard and Selenium-based scraper for the Cayman Islands Judicial website. It collects non-criminal judgments, downloads the associated PDFs through the official AJAX flow, and presents status, logs, and downloads through a web UI.

## Features
- Headless Chromium + Selenium workflow that discovers the live WordPress AJAX nonce and session cookies.
- Robust CSV parser that excludes criminal cases and extracts download metadata from the `Actions` column.
- Selenium-executed AJAX requests to retrieve signed Box.com URLs, streamed to disk with duplicate detection.
- Persistent metadata (`/app/data/metadata.json`), scrape logs (`/app/data/pdfs/scrape_log.txt`), and PDF storage (`/app/data/pdfs`).
- Flask 3 web dashboard with configuration form, live log streaming via Server-Sent Events (SSE), PDF table, metadata export, and ZIP bundling.
- REST endpoints for metadata JSON and CSV export.

## Project Layout
```
app/
  main.py              # Flask application and routes
  scraper/
    config.py          # Constants and paths
    utils.py           # Logging, persistence, ZIP helpers
    parser.py          # CSV download and parsing
    selenium_client.py # Headless Chrome helpers (nonce, AJAX)
    downloader.py      # Download orchestration
    run.py             # High-level scrape coordinator
  templates/           # Jinja templates for UI
  static/style.css     # Basic styling
main.py                # Entry point (`python main.py`)
Dockerfile             # Container definition with Chromium + Chromedriver
requirements.txt
README.md
```

## Local Development
1. **Install dependencies**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. **Ensure Chromium/Chromedriver** are installed locally (or run inside Docker).
3. **Create the data directories** (the app will auto-create them when run):
   ```bash
   mkdir -p /app/data/pdfs
   ```
4. **Start the app**
   ```bash
   python main.py
   ```
5. Open <http://localhost:8080> in your browser, adjust scrape settings, and start a run.

## Docker Usage
Build and run the containerised app (Chromium + Chromedriver included):
```bash
docker build -t cayman-scraper .
docker run -p 8080:8080 -v $(pwd)/data:/app/data cayman-scraper
```
The mounted `./data` folder stores PDFs, logs, and metadata persistently.

## Railway Deployment
1. Push this repository to a Git provider and create a new Railway service from it.
2. In the **Deployments** tab, set the start command to `python main.py` (default).
3. Create a **Volume** and mount it at `/app/data` so PDFs and logs persist across deploys.
4. (Optional) Configure environment variables to adjust scraper defaults:
   - `PAGE_WAIT_SECONDS` – wait time after loading the judgments page (default `15`).
   - `ENTRY_CAP` – maximum number of CSV rows processed per run (default `25`).
   - `PER_DOWNLOAD_DELAY` – delay between downloads in seconds (default `1.0`).

## API Endpoints
- `GET /` – Home dashboard with scraper controls.
- `POST /scrape` – Trigger a scraping run synchronously.
- `GET /report` – Detailed report, live logs, and file list.
- `GET /logs/stream` – Server-Sent Events endpoint for real-time logs.
- `GET /files/<filename>` – Download a single PDF.
- `GET /download/all.zip` – Download all PDFs as a ZIP archive.
- `GET /api/metadata` – JSON metadata export.
- `GET /export/csv` – Metadata in CSV format.

## Troubleshooting
- **Chrome version mismatch**: The Docker image installs matching `chromium` and `chromium-driver`. For local development, ensure your Chromedriver version matches your Chrome/Chromium installation.
- **Nonce retrieval failure**: Increase `PAGE_WAIT_SECONDS` to give WordPress more time to render scripts.
- **AJAX errors / 403**: The site occasionally rotates nonces. Re-run the scrape to refresh cookies and nonce.
- **Empty downloads**: Signed Box URLs may expire quickly; the app retries by fetching a fresh URL on each run.

## License
This project is provided as-is with no warranty. Use responsibly and respect the target website's terms of service.
