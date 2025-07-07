import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Proxy Configuration
PROXY_URL = os.getenv('PROXY_URL')
PROXY_USERNAME = os.getenv('PROXY_USERNAME')
PROXY_PASSWORD = os.getenv('PROXY_PASSWORD')
CHECK_PREFIX = os.getenv('CHECK_PREFIX')

# Google Drive Configuration
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets'
]
PAGE_CHANGES_FOLDER_ID = os.getenv('PAGE_CHANGES_FOLDER_ID')
TOP_PARENT_ID = os.getenv('FOLDER_PARENT_ID')

# Slack Configuration
SLACK_TOKEN = os.getenv('SLACK_TOKEN')
CHANNEL_ID = os.getenv('CHANNEL_ID')

# Directory Configuration
SCREENSHOT_DIR = "page_screenshots"
DATA_FILE = "crawl_progress.pkl"
NEXT_CRAWL_FILE = "next_crawl.pkl"
SCANNED_PAGES_FILE = "scanned_pages.txt"

# Base URL Configuration
BASE_URL = "https://www.education.gov.au"

# URLs to exclude from crawling
EXCLUDE_PREFIXES = [
    "https://www.education.gov.au/newsroom",
    "https://ministers.education.gov.au/clare"
]


# Browser Configuration
CHROME_OPTIONS = {
    "headless": True,
    "disable_gpu": True,
    "no_sandbox": True,
    "disable_dev_shm_usage": True,
    "disable_extensions": True,
    "disable_plugins": True,
    "disable_images": True,
    "disable_javascript": False,
    "window_size": (1920, 1080),
    "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/113.0.0.0 Safari/537.36"
}

# Target URLs
TARGET_URLS = [
    "https://www.education.gov.au/",
] 
