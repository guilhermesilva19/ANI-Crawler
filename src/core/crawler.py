"""Core crawler implementation."""

import os
import time
import hashlib
from datetime import datetime
from typing import Dict, Set, Optional, Tuple, List, Any
from urllib.parse import urlparse
from bs4 import BeautifulSoup

from src.services.browser_service import BrowserService
from src.services.drive_service import DriveService
from src.services.slack_service import SlackService
from src.services.sheets_service import SheetsService
from src.services.scheduler_service import SchedulerService
from src.utils.content_comparison import compare_content, extract_links
from src.utils.mongo_state_adapter import MongoStateAdapter
from src.config import CHECK_PREFIX, PROXY_URL, PROXY_USERNAME, PROXY_PASSWORD, TOP_PARENT_ID

__all__ = ['Crawler']


class Crawler:
    """Main crawler class that handles webpage monitoring and change detection."""
    
    def __init__(self):
        self.state_manager = MongoStateAdapter()
        self.drive_service = DriveService()
        if not self.drive_service.service:
            raise Exception("Failed to initialize Google Drive service - check credentials")
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
        
        
        # Initialize and start daily dashboard scheduler
        try:
            self.scheduler_service = SchedulerService()
            self.scheduler_service.set_state_manager(self.state_manager)
            self.scheduler_service.start_scheduler()
        except Exception as e:
            print(f"⚠️  Scheduler service failed to initialize: {e}")
            print("📱 Continuing without daily dashboard reports...")
            self.scheduler_service = None

    def generate_filename(self, url: str) -> str:
        """Generate a unique filename for a URL."""
        # Ensure page_copies directory exists
        os.makedirs("page_copies", exist_ok=True)
        
        url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
        base_url = urlparse(url).netloc.replace('.', '_')
        return f"page_copies/{base_url}_{url_hash}.html"

    def process_page(self, url: str) -> None:
        """Process a single page: fetch, compare, and store changes."""
        start_time = time.time()
        page_type = "normal"
        
        # Create fresh browser instance for this page to prevent degradation
        page_browser = BrowserService(self.proxy_options)
        
        try:
            # Fetch and parse page
            soup, status_code = page_browser.get_page(url)
            
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
                
                # Record performance for deleted page
                page_type = "deleted"
                crawl_time = time.time() - start_time
                self.state_manager.record_page_crawl(url, crawl_time, page_type)
                return  # Don't process further
            
            if not soup:
                # Page failed to load but not classified as deleted yet
                print(f"\nFailed to load page {url} (Status: {status_code})")
                
                # Record performance for failed page
                page_type = "failed"
                crawl_time = time.time() - start_time
                self.state_manager.record_page_crawl(url, crawl_time, page_type)
                return

            # Intelligent file type categorization - only monitor availability for non-HTML content
            file_type = self._categorize_file_type(url)
            if file_type != "webpage":
                print(f"\n{file_type.title()} available: {url}")
                self.state_manager.add_visited_url(url)
                crawl_time = time.time() - start_time
                self.state_manager.record_page_crawl(url, crawl_time, file_type)
                return

            # Generate filenames and prepare safe filename for Drive
            filename = self.generate_filename(url)
            old_file = filename + ".old"
            safe_filename = page_browser._get_safe_filename(url)
            
            # Track created folders for rollback if needed
            created_folder_ids = []

            # PHASE 1: Complete all risky local operations BEFORE creating Drive folders
            # Save current version locally first
            with open(filename, "w", encoding="utf-8") as f:
                f.write(soup.prettify())

            # Take screenshot locally (most likely to fail)
            screenshot_path, _ = page_browser.save_screenshot(url)

            # PHASE 2: Only create Drive folders after local operations succeed
            folder_id, folder_status = self.drive_service.get_or_create_folder(safe_filename, TOP_PARENT_ID)
            if folder_status == 'new':
                created_folder_ids.append(folder_id)
                
            html_folder_id, html_status = self.drive_service.get_or_create_folder("HTML", folder_id)
            if html_status == 'new':
                created_folder_ids.append(html_folder_id)
                
            screenshot_folder_id, screenshot_status = self.drive_service.get_or_create_folder("SCREENSHOT", folder_id)
            if screenshot_status == 'new':
                created_folder_ids.append(screenshot_folder_id)

            # PHASE 3: Upload files to Drive (now that folders exist and local files are ready)
            if screenshot_path:
                screenshot_url = self.drive_service.upload_file(screenshot_path, screenshot_folder_id)
                os.remove(screenshot_path)

            # Handle file versions in Drive
            new_file_id = self.drive_service.find_file(os.path.basename(filename), html_folder_id)
            old_file_id = self.drive_service.find_file(os.path.basename(old_file), html_folder_id)

            # Check if this is a new page
            is_new_page = not old_file_id and not self.state_manager.was_visited(url)
            
            if is_new_page:
                page_type = "new"
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
                    page_type = "changed"
                    
                    # Prepare detailed change information for storage
                    change_details = {
                        "added_text": [{"text": item.get("new_text", "")} for item in added_text] if added_text else [],
                        "deleted_text": [{"text": item.get("new_text", "")} for item in deleted_text] if deleted_text else [],
                        "changed_text": [{"text": item.get("new_text", "")} for item in changed_text] if changed_text else [],
                        "added_links": list(links_changes.get('added_links', set())),
                        "removed_links": list(links_changes.get('removed_links', set())),
                        "added_pdfs": list(links_changes.get('added_pdfs', set())),
                        "removed_pdfs": list(links_changes.get('removed_pdfs', set())),
                        "screenshot_url": f"https://drive.google.com/drive/folders/{screenshot_folder_id}",
                        "html_url": f"https://drive.google.com/drive/folders/{html_folder_id}",
                        "change_summary": self._format_changes_for_sheets(added_text, deleted_text, changed_text, links_changes)
                    }
                    
                    # Store detailed changes in MongoDB
                    self.state_manager.store_page_changes(url, change_details)
                    
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
            
            # Record performance metrics
            crawl_time = time.time() - start_time
            change_details_for_perf = change_details if 'change_details' in locals() else None
            self.state_manager.record_page_crawl(url, crawl_time, page_type, change_details_for_perf)

        except Exception as e:
            # Rollback any newly created folders to prevent orphans
            if 'created_folder_ids' in locals():
                for folder_id in created_folder_ids:
                    try:
                        self.drive_service.delete_folder(folder_id)
                        print(f"🗑️  Cleaned up orphaned folder: {folder_id}")
                    except Exception as cleanup_error:
                        print(f"⚠️  Could not clean up folder {folder_id}: {cleanup_error}")
            
            self.slack_service.send_error(str(e), url)
            print(f"\nError processing page {url}: {e}")
            
            # Record performance for errored page
            crawl_time = time.time() - start_time
            self.state_manager.record_page_crawl(url, crawl_time, "failed")
        finally:
            # cleanup the page-specific browser instance
            if 'page_browser' in locals():
                page_browser.quit()

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
            pages_processed_this_session = 0
            
            while True:
                url = self.state_manager.get_next_url()
                if not url:
                    # Check if we completed a full cycle
                    if pages_processed_this_session > 0:
                        print(f"\n🎉 Completed crawl cycle! Processed {pages_processed_this_session} pages this session.")
                        self.state_manager.complete_cycle()
                        pages_processed_this_session = 0
                    
                    print("\nNo URLs remaining. Waiting for recrawl...")
                    time.sleep(300)  # Wait 5 minutes before checking again
                    continue

                # Clean URL
                url = url.rstrip("/")
                # Skip if URL should be excluded
                if (CHECK_PREFIX and url.startswith(CHECK_PREFIX)):
                    continue
                print(f"\nCrawling: {url}")
                self.process_page(url)
                pages_processed_this_session += 1
                
                # Show progress every 10 pages
                if pages_processed_this_session % 10 == 0:
                    stats = self.state_manager.get_progress_stats()
                    print(f"\n📊 Progress: {stats['completed_pages']}/{stats['total_known_pages']} ({stats['progress_percent']}%) - {stats['pages_per_hour']:.0f} pages/hour")
                    if stats['eta_datetime']:
                        print(f"⏰ ETA: {stats['eta_datetime'].strftime('%I:%M %p today' if stats['eta_datetime'].date() == datetime.now().date() else '%b %d at %I:%M %p')}")
                
                # Rescue stuck URLs every 50 pages (roughly every 25-30 minutes)
                if pages_processed_this_session % 50 == 0:
                    self.state_manager.rescue_stuck_urls(stuck_minutes=60)
                
                # Polite delay between requests
                time.sleep(30)
        except KeyboardInterrupt:
            print("\nCrawling interrupted by user.")
        except Exception as e:
            self.slack_service.send_error(f"Critical crawler error: {str(e)}")
            print(f"\nCritical error: {e}")
        finally:
            # Cleanup services
            if hasattr(self, 'scheduler_service') and self.scheduler_service:
                self.scheduler_service.stop_scheduler()

    def _categorize_file_type(self, url: str) -> str:
        """Intelligently categorize file types based on URL and content patterns."""
        url_lower = url.lower()
        
        # Check for download/file patterns first (most common on education.gov.au)
        if '/download/' in url_lower or '/downloads/' in url_lower or '/files/' in url_lower or '/attachments/' in url_lower:
            return "document"  # Keep consistent with existing 11k URLs
        
        # Document files - handle both .extension and /extension patterns
        elif (url_lower.endswith(('.pdf', '/pdf')) or '.pdf' in url_lower):
            return "document"  # Keep consistent - don't create new "pdf" category
        elif (url_lower.endswith(('.doc', '.docx', '/doc', '/docx')) or any(ext in url_lower for ext in ['.doc', '.docx'])):
            return "document"
        elif (url_lower.endswith(('.xls', '.xlsx', '.csv')) or any(ext in url_lower for ext in ['.xls', '.xlsx', '.csv'])):
            return "document"
        elif (url_lower.endswith(('.ppt', '.pptx')) or any(ext in url_lower for ext in ['.ppt', '.pptx'])):
            return "document"
        elif (url_lower.endswith(('.txt', '.rtf')) or any(ext in url_lower for ext in ['.txt', '.rtf'])):
            return "document"
        
        # Media files - also keep as document for consistency
        elif (url_lower.endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp')) or 
              any(ext in url_lower for ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp'])):
            return "document"
        elif (url_lower.endswith(('.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm')) or
              any(ext in url_lower for ext in ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm'])):
            return "document"
        elif (url_lower.endswith(('.mp3', '.wav', '.flac', '.aac', '.ogg')) or
              any(ext in url_lower for ext in ['.mp3', '.wav', '.flac', '.aac', '.ogg'])):
            return "document"
        
        # Archive files
        elif (url_lower.endswith(('.zip', '.rar', '.7z', '.tar', '.gz', '.bz2')) or
              any(ext in url_lower for ext in ['.zip', '.rar', '.7z', '.tar', '.gz', '.bz2'])):
            return "document"
        
        # Default to webpage for HTML content
        else:
            return "webpage"
