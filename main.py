"""Entry point for running the Flask application."""
from __future__ import annotations

from app.main import app
from app.scraper.utils import ensure_dirs


if __name__ == "__main__":
    ensure_dirs()
    app.run(host="0.0.0.0", port=8080)
