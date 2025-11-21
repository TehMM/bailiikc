from app.main import app
from app.scraper import db
from app.scraper.utils import ensure_dirs
import os

if __name__ == "__main__":
    # Make sure data dirs exist
    ensure_dirs()
    db.initialize_schema()

    # Railway provides PORT; default to 8080 for local dev
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
