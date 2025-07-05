"""Core crawler implementation."""

import os
import time
import hashlib
from typing import Dict, Set, Optional, Tuple, List, Any
from urllib.parse import urlparse
from bs4 import BeautifulSoup

from src.services.browser_service import BrowserService
from src.services.drive_service import DriveService
from src.services.slack_service import SlackService
from src.services.sheets_service import SheetsService
from src.utils.content_comparison import compare_content, extract_links
from src.utils.state_manager import StateManager
from src.config import CHECK_PREFIX, PROXY_URL, PROXY_USERNAME, PROXY_PASSWORD, TOP_PARENT_ID

__all__ = ['Crawler']


class Crawler:
    """Main crawler class that handles webpage monitoring and change detection."""
    
    def __init__(self):
        self.state_manager = StateManager()
        self.drive_service = DriveService()
        self.slack_service = SlackService()
        
        # Initialize Google Sheets service for logging
        try:
            self.sheets_service = SheetsService()
            print(f"📊 Sheets logging enabled: {self.sheets_service.get_spreadsheet_url()}")
        except Exception as e:
            print(f"⚠️  Sheets service failed to initialize: {e}")
            print("📱 Continuing with Slack-only logging...")
            self.sheets_service = None
        
        # Setup proxy options if credentials are available
        self.proxy_options = None
        if all([PROXY_USERNAME, PROXY_PASSWORD, PROXY_URL]):
            self.proxy_options = {
                "http": f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_URL}",
                "https": f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_URL}",
            }
            print(f"\nProxy configured: {self.proxy_options['http']}")
        
        # Initialize browser once for entire session
        self.browser_service = BrowserService(self.proxy_options)

    def generate_filename(self, url: str) -> str:
        """Generate a unique filename for a URL."""
        # Ensure page_copies directory exists
        os.makedirs("page_copies", exist_ok=True)
        
        url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
        base_url = urlparse(url).netloc.replace('.', '_')
        return f"page_copies/{base_url}_{url_hash}.html"

    def process_page(self, url: str) -> None:
        """Process a single page: fetch, compare, and store changes."""
        try:
            # Fetch and parse page
            soup, status_code = self.browser_service.get_page(url)
            
            # Check for deleted page before processing
            is_deleted_page = self.state_manager.update_url_status(url, status_code)
            if is_deleted_page:
                # Get last successful access time for the alert
                url_status = self.state_manager.url_status.get(url, {})
                last_success = url_status.get('last_success')
                
                # Send deleted page alert to Slack
                self.slack_service.send_deleted_page_alert(url, status_code, last_success)
                
                # Log to Google Sheets
                if self.sheets_service:
                    self.sheets_service.log_deleted_page_alert(url, status_code, last_success)
                
                print(f"\nDeleted page detected: {url} (Status: {status_code})")
                return  # Don't process further
            
            if not soup:
                # Page failed to load but not classified as deleted yet
                print(f"\nFailed to load page {url} (Status: {status_code})")
                return

            # Generate filenames
            filename = self.generate_filename(url)
            old_file = filename + ".old"

            # Create folder structure in Drive
            safe_filename = self.browser_service._get_safe_filename(url)
            folder_id, _ = self.drive_service.get_or_create_folder(safe_filename, TOP_PARENT_ID)
            html_folder_id, _ = self.drive_service.get_or_create_folder("HTML", folder_id)
            screenshot_folder_id, _ = self.drive_service.get_or_create_folder("SCREENSHOT", folder_id)

            # Save current version
            with open(filename, "w", encoding="utf-8") as f:
                f.write(soup.prettify())

            # Take screenshot
            screenshot_path, _ = self.browser_service.save_screenshot(url)
            if screenshot_path:
                screenshot_url = self.drive_service.upload_file(screenshot_path, screenshot_folder_id)
                os.remove(screenshot_path)

            # Handle file versions in Drive
            new_file_id = self.drive_service.find_file(os.path.basename(filename), html_folder_id)
            old_file_id = self.drive_service.find_file(os.path.basename(old_file), html_folder_id)

            # Check if this is a new page
            is_new_page = not old_file_id and not self.state_manager.was_visited(url)
            
            if is_new_page:
                # Send new page notification using format_change_message
                blocks = self.slack_service.format_change_message(
                    url,
                    [], [], [],  # No content changes for new page
                    {'added_links': set(), 'removed_links': set(), 'added_pdfs': set(), 'removed_pdfs': set()},
                    f"https://drive.google.com/drive/folders/{screenshot_folder_id}",
                    f"https://drive.google.com/drive/folders/{html_folder_id}",
                    is_new_page=True
                )
                self.slack_service.send_message(blocks)
                
                # Log to Google Sheets
                if self.sheets_service:
                    self.sheets_service.log_new_page_alert(
                        url,
                        f"https://drive.google.com/drive/folders/{screenshot_folder_id}",
                        f"https://drive.google.com/drive/folders/{html_folder_id}"
                    )
            elif old_file_id:
                # Compare versions for existing page
                self.drive_service.download_file(old_file_id, old_file)
                with open(old_file, "r", encoding="utf-8") as f:
                    old_content = f.read()
                with open(filename, "r", encoding="utf-8") as f:
                    new_content = f.read()

                # Compare content with enhanced detection
                added, deleted, changed = compare_content(old_content, new_content)

                # Extract and compare links
                old_links = extract_links(url, BeautifulSoup(old_content, 'html.parser'), CHECK_PREFIX)
                new_links = extract_links(url, BeautifulSoup(new_content, 'html.parser'), CHECK_PREFIX)

                # Find changes in links
                added_links = new_links - old_links
                removed_links = old_links - new_links
                added_pdfs = {link for link in added_links if link.lower().endswith('.pdf')}
                removed_pdfs = {link for link in removed_links if link.lower().endswith('.pdf')}

                links_changes = {
                    'added_links': added_links - added_pdfs,
                    'removed_links': removed_links - removed_pdfs,
                    'added_pdfs': added_pdfs,
                    'removed_pdfs': removed_pdfs
                }

                # Format changes for notification
                added_text = self.format_change_blocks(added, "Added")
                deleted_text = self.format_change_blocks(deleted, "Deleted")
                changed_text = self.format_change_pairs(changed)

                # If there are any changes, send notification
                if any([added_text, deleted_text, changed_text]) or any(links_changes.values()):
                    blocks = self.slack_service.format_change_message(
                        url,
                        added_text,
                        deleted_text,
                        changed_text,
                        links_changes,
                        f"https://drive.google.com/drive/folders/{screenshot_folder_id}",
                        f"https://drive.google.com/drive/folders/{html_folder_id}",
                        is_new_page=False
                    )
                    self.slack_service.send_message(blocks)
                    
                    # Log to Google Sheets
                    if self.sheets_service:
                        # Create description from changes
                        changes_desc = self._format_changes_for_sheets(added_text, deleted_text, changed_text, links_changes)
                        self.sheets_service.log_changed_page_alert(
                            url,
                            changes_desc,
                            f"https://drive.google.com/drive/folders/{screenshot_folder_id}",
                            f"https://drive.google.com/drive/folders/{html_folder_id}"
                        )

                # Clean up old files
                os.remove(old_file)

            # Upload new version and rename old version
            if new_file_id:
                self.drive_service.rename_file(new_file_id, os.path.basename(old_file))
            self.drive_service.upload_file(filename, html_folder_id)
            os.remove(filename)

            # Extract new links to crawl
            new_links = extract_links(url, soup, CHECK_PREFIX)
            self.state_manager.add_new_urls(new_links)

            # Update state
            self.state_manager.add_visited_url(url)
            self.state_manager.log_scanned_page(url)

        except Exception as e:
            #self.slack_service.send_error(str(e), url)
            print(f"\nError processing page {url}: {e}")

    def format_change_blocks(self, changes: List[Dict[str, Any]], change_type: str) -> List[Dict[str, Any]]:
        """Format changes into blocks for notification."""
        return changes  # Changes are already in the correct format

    def format_change_pairs(self, changes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format change pairs into blocks for notification."""
        return changes  # Changes are already in the correct format

    def _format_changes_for_sheets(self, added: List[Dict[str, Any]], deleted: List[Dict[str, Any]], 
                                  changed: List[Dict[str, Any]], links_changes: Dict[str, Set[str]]) -> str:
        """Format changes into a concise description for Google Sheets."""
        parts = []
        
        # Text changes
        if added:
            parts.append(f"Added {len(added)} text sections")
        if deleted:
            parts.append(f"Removed {len(deleted)} text sections")
        if changed:
            parts.append(f"Modified {len(changed)} text sections")
        
        # Link changes
        if links_changes.get('added_links'):
            parts.append(f"Added {len(links_changes['added_links'])} links")
        if links_changes.get('removed_links'):
            parts.append(f"Removed {len(links_changes['removed_links'])} links")
        if links_changes.get('added_pdfs'):
            parts.append(f"Added {len(links_changes['added_pdfs'])} PDFs")
        if links_changes.get('removed_pdfs'):
            parts.append(f"Removed {len(links_changes['removed_pdfs'])} PDFs")
        
        return "; ".join(parts) if parts else "Page content changed"

    def run(self) -> None:
        """Main crawl loop."""
        try:
            while True:
                url = self.state_manager.get_next_url()
                if not url:
                    print("\nNo URLs remaining. Waiting for recrawl...")
                    time.sleep(300)  # Wait 5 minutes before checking again
                    continue

                # Clean URL
                url = url.rstrip("/")
                
                # Skip if URL should be excluded
                if '#' in url or (CHECK_PREFIX and url.startswith(CHECK_PREFIX)):
                    continue

                print(f"\nCrawling: {url}")
                self.process_page(url)
                
                # Polite delay between requests
                time.sleep(30)

        except KeyboardInterrupt:
            print("\nCrawling interrupted by user.")
        except Exception as e:
            self.slack_service.send_error(f"Critical crawler error: {str(e)}")
            print(f"\nCritical error: {e}")
        finally:
            if self.browser_service:
                self.browser_service.quit() 