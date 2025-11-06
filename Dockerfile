FROM python:3.11-slim

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
