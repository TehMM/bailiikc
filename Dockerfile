# Match your Playwright Python version (e.g. 1.48.0)
FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy

WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY . .

# Ensure data dir exists (Railway volume can mount over it)
RUN mkdir -p /app/data/pdfs

# Browsers are already available in this image; no --with-deps needed
# (If you want to be explicit / keep in sync with requirements.txt:)
# RUN python -m playwright install chromium

CMD ["python", "main.py"]

# Install basic system deps (Playwright will pull the rest with --with-deps)
RUN apt-get update && apt-get install -y \
    curl wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies (includes playwright)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browser inside the image
RUN python -m playwright install --with-deps chromium

# Copy project files
COPY . .

# Create data dir (Railway volume can mount here)
RUN mkdir -p /app/data/pdfs

# Default start command (Railway can override, but this is fine)
CMD ["python", "main.py"]
