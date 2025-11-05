FROM python:3.11-slim

# Install Chrome + Chromedriver + basics
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      chromium chromium-driver ca-certificates fonts-liberation wget gnupg unzip curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Environment for chromium
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# Create data directory expected by the app
RUN mkdir -p /app/data/pdfs

EXPOSE 5000
CMD ["python", "main.py"]
