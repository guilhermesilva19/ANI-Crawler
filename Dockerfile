FROM python:3.10-slim

# Install Chrome dependencies
RUN apt-get update && apt-get install -y \
    wget unzip gnupg ca-certificates fonts-liberation \
    libnss3 libgconf-2-4 libxi6 libxcursor1 libxdamage1 \
    libxtst6 libappindicator1 libgtk-3-0 \
    && wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && dpkg -i google-chrome-stable_current_amd64.deb || apt-get -fy install \
    && rm google-chrome-stable_current_amd64.deb

# Set display
ENV DISPLAY=:99

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . /main
WORKDIR /main

# Create necessary directories and set permissions
RUN mkdir -p page_changes page_copies page_screenshots

# Run app
CMD ["python", "main.py"]