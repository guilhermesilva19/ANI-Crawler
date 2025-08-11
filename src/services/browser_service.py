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
        if CHROME_OPTIONS.get('disable_dev_shm_usage'):
            chrome_options.add_argument("--disable-dev-shm-usage")
        if CHROME_OPTIONS.get('disable_extensions'):
            chrome_options.add_argument("--disable-extensions")
        if CHROME_OPTIONS.get('disable_plugins'):
            chrome_options.add_argument("--disable-plugins")
        if CHROME_OPTIONS.get('disable_images'):
            chrome_options.add_argument("--disable-images")
        if CHROME_OPTIONS.get('dns-prefetch-disable'):
            chrome_options.add_argument("--dns-prefetch-disable")
        if CHROME_OPTIONS.get('window_size'):
            width, height = CHROME_OPTIONS['window_size']
            chrome_options.add_argument(f"--window-size={width},{height}")
        if CHROME_OPTIONS.get('user_agent'):
            chrome_options.add_argument(f"user-agent={CHROME_OPTIONS['user_agent']}")
        
        # Additional Docker-specific Chrome options
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-features=TranslateUI")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        chrome_options.add_argument("--remote-debugging-port=9222")

        try:
            # Configure selenium-wire with proxy if provided
            seleniumwire_options = {}
            if self.proxy_options:
                seleniumwire_options['proxy'] = self.proxy_options

            # Try automatic download, fallback to manual path
            try:
                driver_path = ChromeDriverManager().install()
            except Exception as e:
                print(f"⚠️  ChromeDriver installation failed: {e}")
                driver_path = r"chromedriver\chromedriver-win64\chromedriver-win64\chromedriver.exe"

            self.driver = webdriver.Chrome(
                service=Service(driver_path),
                options=chrome_options,
                seleniumwire_options=seleniumwire_options if self.proxy_options else None
            )
            self.driver.set_script_timeout(1000)
        except Exception as e:
            print(f"\nError setting up WebDriver: {e}")
            raise

    def quit(self) -> None:
        """Safely quit the browser."""
        if self.driver:
            self.driver.quit()
            print(f"   🗑️  Browser instance terminated")

    def get_page(self, url: str) -> Tuple[Optional[BeautifulSoup], int]:
        """Load a page and return its parsed content along with HTTP status code."""
        try:
            self.driver.get(url)
            time.sleep(10)  # Wait for page load
            
            # Get final HTTP status from selenium-wire (after redirects)
            status_code = 200  # Default to success
            final_url = self.driver.current_url
            
            # Find the final request that matches the current URL
            for request in reversed(self.driver.requests):  # Check most recent first
                if request.response and request.url == final_url:
                    status_code = request.response.status_code
                    break
            
            # Check if page loaded successfully
            if status_code >= 400:
                print(f"\nHTTP {status_code} for {final_url}")
                return None, status_code
                
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            return soup, status_code
            
        except Exception as e:
            print(f"\nError loading page {url}: {e}")
            return None, 0  # 0 indicates connection/network error

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
        screenshot_path = ""
        safe_filename = ""
        
        try:
            # Validate driver is ready
            if not self.driver:
                raise Exception("WebDriver not initialized")
            
            # Scroll to capture full page
            self.scroll_full_page()
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)

            # Create screenshot directory if it doesn't exist
            os.makedirs(SCREENSHOT_DIR, exist_ok=True)
            print("make dir")

            # Generate filename from URL
            safe_filename = self._get_safe_filename(page_url)
            screenshot_path = os.path.join(SCREENSHOT_DIR, f"{safe_filename}.png")
            print("screenshot path")

            # Set window size to capture full page
            total_height = self.driver.execute_script("return document.body.scrollHeight")
            print("total height ==>", total_height)
            
            # Validate total_height is reasonable
            if total_height <= 0 or total_height > 10000:  # Sanity check
                print(f"⚠️  Invalid page height: {total_height}, using default")
                total_height = 1080
            
            self.driver.set_window_size(1920, total_height)
            print("-----")
            time.sleep(2)

            # Take screenshot
            print("before screenshot")
            self.driver.save_screenshot(screenshot_path)
            
            # Validate screenshot was created and has content
            if not os.path.exists(screenshot_path):
                raise Exception(f"Screenshot file was not created: {screenshot_path}")
            
            file_size = os.path.getsize(screenshot_path)
            if file_size == 0:
                raise Exception(f"Screenshot file is empty: {screenshot_path}")
            
            if file_size < 1000:  # PNG files should be at least 1KB
                print(f"⚠️  Screenshot file is very small ({file_size} bytes) - might be corrupted")
            
            print(f"✅ Screenshot saved successfully: {screenshot_path} ({file_size} bytes)")
            return screenshot_path, safe_filename
            
        except Exception as e:
            print(f"\nError saving screenshot: {e}")
            # Clean up any partially created file
            if screenshot_path and os.path.exists(screenshot_path):
                try:
                    os.remove(screenshot_path)
                    print(f"🗑️  Cleaned up failed screenshot: {screenshot_path}")
                except:
                    pass
            return "", ""

    def _get_safe_filename(self, url: str) -> str:
        """Generate a safe filename from URL."""
        import hashlib
        parsed = urlparse(url)
        # Include path and query to avoid collisions
        path_part = parsed.path.replace("/", "_").strip("_")
        query_part = parsed.query.replace("&", "_").replace("=", "-") if parsed.query else ""
        
        # Create base filename
        if path_part:
            base_name = f"{parsed.netloc}_{path_part}"
        else:
            base_name = f"{parsed.netloc}_index"
            
        if query_part:
            base_name += f"_{query_part}"
        
        # Add URL hash to ensure uniqueness
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        filename = f"{base_name}_{url_hash}"
        
        return filename[:100]  # Limit length for safety 