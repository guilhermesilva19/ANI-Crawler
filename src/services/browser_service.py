"""Browser service for web page interaction and screenshots."""

from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import os
from typing import Optional, Tuple
from urllib.parse import urlparse
from ..config import CHROME_OPTIONS, SCREENSHOT_DIR

__all__ = ['BrowserService']


class BrowserService:
    """Service class for browser automation and webpage interaction."""

    def __init__(self, proxy_options=None):
        """Initialize browser service with optional proxy settings."""
        self.driver = None
        self.proxy_options = proxy_options
        self.setup_driver()

    def setup_driver(self) -> None:
        """Set up the Selenium WebDriver with appropriate options."""
        chrome_options = Options()
        
        # Add Chrome options from config
        if CHROME_OPTIONS.get('headless'):
            chrome_options.add_argument("--headless=new")
        if CHROME_OPTIONS.get('disable_gpu'):
            chrome_options.add_argument("--disable-gpu")
        if CHROME_OPTIONS.get('no_sandbox'):
            chrome_options.add_argument("--no-sandbox")
        if CHROME_OPTIONS.get('window_size'):
            width, height = CHROME_OPTIONS['window_size']
            chrome_options.add_argument(f"--window-size={width},{height}")
        if CHROME_OPTIONS.get('user_agent'):
            chrome_options.add_argument(f"user-agent={CHROME_OPTIONS['user_agent']}")

        try:
            # Configure selenium-wire with proxy if provided
            seleniumwire_options = {}
            if self.proxy_options:
                seleniumwire_options['proxy'] = self.proxy_options

            self.driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=chrome_options,
                seleniumwire_options=seleniumwire_options if self.proxy_options else None
            )
        except Exception as e:
            print(f"\nError setting up WebDriver: {e}")
            raise

    def quit(self) -> None:
        """Safely quit the browser."""
        if self.driver:
            self.driver.quit()

    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Load a page and return its parsed content."""
        try:
            self.driver.get(url)
            time.sleep(10)  # Wait for page load
            return BeautifulSoup(self.driver.page_source, 'html.parser')
        except Exception as e:
            print(f"\nError loading page {url}: {e}")
            return None

    def scroll_full_page(self, pause_time: float = 1.5) -> None:
        """Scroll down incrementally until the bottom of the page is reached."""
        try:
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            while True:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(pause_time)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
        except Exception as e:
            print(f"\nError scrolling page: {e}")

    def save_screenshot(self, page_url: str) -> Tuple[str, str]:
        """Capture and save a full-page screenshot."""
        try:
            self.scroll_full_page()
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)

            # Create screenshot directory if it doesn't exist
            os.makedirs(SCREENSHOT_DIR, exist_ok=True)

            # Generate filename from URL
            safe_filename = self._get_safe_filename(page_url)
            screenshot_path = os.path.join(SCREENSHOT_DIR, f"{safe_filename}.png")

            # Set window size to capture full page
            total_height = self.driver.execute_script("return document.body.scrollHeight")
            self.driver.set_window_size(1920, total_height)
            time.sleep(2)

            # Take screenshot
            self.driver.save_screenshot(screenshot_path)
            
            return screenshot_path, safe_filename
        except Exception as e:
            print(f"\nError saving screenshot: {e}")
            return "", ""

    def _get_safe_filename(self, url: str) -> str:
        """Generate a safe filename from URL."""
        parsed = urlparse(url)
        filename = parsed.netloc + "_" + parsed.path.replace("/", "_").strip("_")
        return filename[:100]  # Limit length for safety 