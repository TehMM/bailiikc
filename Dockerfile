# Use a lightweight Python base image
FROM python:3.11-slim

# Install dependencies for Chrome & Selenium
RUN apt-get update && \
    apt-get install -y wget gnupg unzip curl chromium chromium-driver && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set environment variables for Chrome
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Copy application files
COPY . /app

# Install Python requirements
RUN pip install --no-cache-dir -r requirements.txt

# Expose Flask port
EXPOSE 5000

# Run the Flask app
CMD ["python", "main.py"]
