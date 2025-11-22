from app.main import app
import os

if __name__ == "__main__":
    # Direct import of app.main initialises directories and schema. The hosting
    # environment may provide PORT; default to 8080 for local development.
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
