# Playwright image matching your Python Playwright version
FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy

WORKDIR /app

# Install your Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your project
COPY . .

# Ensure data dir exists (your Railway volume can mount over this)
RUN mkdir -p /app/data/pdfs

# Start your scraper
CMD ["python", "main.py"]
