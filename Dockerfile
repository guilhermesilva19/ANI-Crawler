# Use a slim Python base image
FROM python:3.11-slim

# Avoid interactive prompts during apt install
ENV DEBIAN_FRONTEND=noninteractive

# Install Chrome dependencies and Chrome itself
RUN apt-get update && \
    apt-get install -y wget unzip gnupg ca-certificates \
    fonts-liberation libnss3 libgconf-2-4 libxi6 libxcursor1 \
    libxdamage1 libxtst6 libappindicator1 libgtk-3-0 \
    --no-install-recommends && \
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    dpkg -i google-chrome-stable_current_amd64.deb || apt-get -fy install && \
    rm google-chrome-stable_current_amd64.deb && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set environment to headless mode
ENV DISPLAY=:99

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Add your code
COPY . /app
WORKDIR /app

# Run your crawler
CMD ["python", "crawler.py"]

FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Create necessary directories
RUN mkdir -p page_changes page_copies page_screenshots

# Run the application
CMD ["python", "main.py"] 