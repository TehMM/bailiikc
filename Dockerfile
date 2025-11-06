# Use Playwright base image (includes Chromium + deps)
FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy

WORKDIR /app

# Install your Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Ensure data directory exists (Railway volume can mount over this)
RUN mkdir -p /app/data/pdfs

# Start the Flask web app
CMD ["python", "app/main.py"]
