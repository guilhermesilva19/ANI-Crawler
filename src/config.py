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
GOOGLE_DRIVE_CREDENTIALS_FILE = os.getenv('GOOGLE_DRIVE_CREDENTIALS_FILE', 'credentials.json')
GOOGLE_DRIVE_TOKEN_FILE = os.getenv('GOOGLE_DRIVE_TOKEN_FILE', 'token.json')
GOOGLE_DRIVE_ROOT_FOLDER_ID = os.getenv('FOLDER_PARENT_ID')  # Use existing variable name

# Service Account Configuration (alternative to OAuth 2.0)
GOOGLE_SERVICE_ACCOUNT_TYPE = os.getenv('TYPE')
GOOGLE_SERVICE_ACCOUNT_PROJECT_ID = os.getenv('PROJECT_ID')
GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY_ID = os.getenv('PRIVATE_KEY_ID')
GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY = os.getenv('PRIVATE_KEY')
GOOGLE_SERVICE_ACCOUNT_CLIENT_EMAIL = os.getenv('CLIENT_EMAIL')
GOOGLE_SERVICE_ACCOUNT_CLIENT_ID = os.getenv('CLIENT_ID')
GOOGLE_SERVICE_ACCOUNT_AUTH_URI = os.getenv('AUTH_URI')
GOOGLE_SERVICE_ACCOUNT_TOKEN_URI = os.getenv('TOKEN_URI')
GOOGLE_SERVICE_ACCOUNT_AUTH_PROVIDER_X509_CERT_URL = os.getenv('AUTH_PROVIDER_x509_CERT_URL')
# GOOGLE_SERVICE_ACCOUNT_CLIENT_X509_CERT_URL = os.getenv('CLIENT_X509_CERT_URL')
GOOGLE_SERVICE_ACCOUNT_CLIENT_SECRET = os.getenv('CLIENT_SECRET')

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
BASE_URL = "https://www.ahpra.gov.au"

# URLs to exclude from crawling
EXCLUDE_PREFIXES = [
    "https://www.ahpra.gov.au/newsroom",
    "https://ministers.ahpra.gov.au/clare",
    "https://www.ahpra.gov.au/news"
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
    "https://www.ahpra.gov.au/",
] 

MONGODB_URI = os.getenv('MONGODB_URI')  # mongodb+srv://username:password@cluster.mongodb.net/
SITE_ID = os.getenv('SITE_ID', 'ato_gov_au')  # Unique identifier for this site
SITE_NAME = os.getenv('SITE_NAME', 'Department of ato')  # Human-readable site name