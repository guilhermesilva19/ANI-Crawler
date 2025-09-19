"""Core crawler implementation."""

import os
import time
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Set, Optional, Tuple, List, Any
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup


import gc
import re
import requests


from concurrent.futures import ThreadPoolExecutor, as_completed
from src.services.browser_service import BrowserService
from src.services.drive_service import DriveService
from src.services.slack_service import SlackService
from src.services.sheets_service import SheetsService
from src.services.scheduler_service import SchedulerService
from src.utils.content_comparison import compare_content, extract_links
from src.utils.mongo_state_adapter import MongoStateAdapter
from src.config import CHECK_PREFIX, PROXY_URL, PROXY_USERNAME, PROXY_PASSWORD, TOP_PARENT_ID, EXCLUDE_PREFIXES

__all__ = ['Crawler']


class Crawler:
    """Main crawler class that handles webpage monitoring and change detection."""
    
    def __init__(self):
        self.state_manager = MongoStateAdapter()
        
        # Memory optimization settings for Render deployment
        self.max_memory_mb = int(os.getenv('MAX_MEMORY_MB', '512'))  # Default 512MB limit
        self.memory_check_interval = 50  # Check memory every 50 pages
        self.gc_threshold = 0.8  # Force garbage collection at 80% memory usage
        
        # Initialize Google Drive service (optional)
        try:
            self.drive_service = DriveService()
            if not self.drive_service.service:
                print("⚠️  Google Drive service not available - continuing without file uploads")
                self.drive_service = None
            else:
                print("✅ Google Drive service initialized successfully")
                # Initialize upload tracking
                self.upload_stats = {
                    'successful': 0,
                    'failed': 0,
                    'quota_errors': 0,
                    'other_errors': 0
                }
        except Exception as e:
            print(f"⚠️  Google Drive service failed to initialize: {e}")
            print("📁 Continuing without file uploads...")
            self.drive_service = None
        
        # Initialize Slack service (optional)
        try:
            self.slack_service = SlackService()
            print("✅ Slack service initialized successfully")
        except Exception as e:
            print(f"⚠️  Slack service failed to initialize: {e}")
            print("💬 Continuing without Slack notifications...")
            self.slack_service = None
        
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

    def check_page_headers(self, url: str) -> Tuple[bool, Optional[Dict]]:
        """Check ETags/Last-Modified headers to determine if page changed.
        
        Returns:
            (needs_update, header_info) - needs_update=True if page should be crawled
        """
        try:
            # Add common headers to avoid being blocked
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            # Add conditional headers if we have cached info
            cached_info = self.etag_cache.get(url)
            if cached_info:
                if cached_info.get('etag'):
                    headers['If-None-Match'] = cached_info['etag']
                if cached_info.get('last_modified'):
                    headers['If-Modified-Since'] = cached_info['last_modified']
            
            # Make HEAD request first (more efficient)
            response = requests.head(url, headers=headers, timeout=10, allow_redirects=True)
            
            # Handle 304 Not Modified
            if response.status_code == 304:
                print(f"📅 Page unchanged (304): {url}")
                if hasattr(self, 'upload_stats'):
                    self.upload_stats['etag_hits'] += 1
                return False, None
            
            # Get header info for caching
            header_info = {
                'etag': response.headers.get('ETag'),
                'last_modified': response.headers.get('Last-Modified'),
                'last_check': datetime.now(),
                'status_code': response.status_code
            }
            
            # Cache the header info
            self.etag_cache[url] = header_info
            
            # Check if content has actually changed
            if cached_info:
                etag_unchanged = (header_info.get('etag') and 
                                cached_info.get('etag') == header_info.get('etag'))
                modified_unchanged = (header_info.get('last_modified') and 
                                    cached_info.get('last_modified') == header_info.get('last_modified'))
                
                if etag_unchanged or modified_unchanged:
                    print(f"📅 Page unchanged (headers): {url}")
                    if hasattr(self, 'upload_stats'):
                        self.upload_stats['etag_hits'] += 1
                    return False, header_info
            
            print(f"🔄 Page may have changed: {url}")
            return True, header_info
            
        except Exception as e:
            print(f"⚠️  Header check failed for {url}: {e}")
            # On error, assume page needs checking
            return True, None

    def get_domain_priority(self, url: str) -> int:
        """Get priority level for a URL based on domain and content."""
        domain = urlparse(url).netloc.lower()
        
        # Special handling for health domain
        if 'health.gov.au' in domain:
            return get_health_url_priority(url)
        
        # Check priority domains
        for priority_domain, priority in self.priority_domains.items():
            if priority_domain in domain:
                return priority
        
        # Default priority for unknown domains
        return 3

    def should_crawl_now(self, url: str, last_crawled: Optional[datetime]) -> bool:
        """Determine if URL should be crawled now based on priority and frequency."""
        if not last_crawled:
            return True  # Never crawled before
        
        priority = self.get_domain_priority(url)
        frequency_hours = self.crawl_frequencies.get(priority, 24)
        
        time_since_crawl = datetime.now() - last_crawled
        should_crawl = time_since_crawl.total_seconds() >= (frequency_hours * 3600)
        
        if not should_crawl:
            next_crawl = last_crawled + timedelta(hours=frequency_hours)
            print(f"⏰ Skipping {url} (priority {priority}) - next crawl at {next_crawl.strftime('%H:%M')}")
        
        return should_crawl

    def can_process_domain(self, url: str) -> bool:
        """Check if domain can be processed now based on rate limiting."""
        domain = urlparse(url).netloc
        last_request = self.domain_last_request[domain]
        delay = self.domain_delays[domain]
        
        time_since_last = (datetime.now() - last_request).total_seconds()
        can_process = time_since_last >= delay
        
        if not can_process:
            wait_time = delay - time_since_last
            print(f"⏳ Domain {domain} rate limited - wait {wait_time:.1f}s")
        
        return can_process

    def update_domain_timing(self, url: str):
        """Update domain timing after processing."""
        domain = urlparse(url).netloc
        self.domain_last_request[domain] = datetime.now()

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
        
        # Notify third-party API about the crawl attempt with URL, timestamp, and RAM usage
        start_timestamp_utc = datetime.utcnow().isoformat() + "Z"
        try:
            ram_mb = None
            try:
                import psutil  # type: ignore
                process = psutil.Process(os.getpid())
                ram_mb = int(process.memory_info().rss / (1024 * 1024))
            except Exception:
                # Fallback to 0 if psutil unavailable or any error occurs
                ram_mb = 0

            text_value = f"URL={url} crawl_started | timestamp={start_timestamp_utc} | ram_mb={ram_mb}"
            print("requesting log")
            requests.post(
                "https://ca55da625cee.ngrok-free.app/log",
                data={"log": text_value},
                timeout=5,
            )
        except Exception:
            # Silently ignore any telemetry errors to avoid impacting crawl
            pass

        try:
            # Fetch and parse page
            print("BEFORE GET PAGE", url)
            soup, status_code = page_browser.get_page(url)
            print("AFTER GET PAGE")
            # Check for deleted page before processing
            is_deleted_page = self.state_manager.update_url_status(url, status_code)
            print("AFTER UPDATE URL STATUS", is_deleted_page)
            if is_deleted_page:
                # Get last successful access time for the alert
                url_status = self.state_manager.url_status.get(url, {})
                last_success = url_status.get('last_success')
                
                # Send deleted page alert to Slack
                if self.slack_service:
                    self.slack_service.send_deleted_page_alert(url, status_code, last_success)
                
                # Log to Google Sheets
                if self.sheets_service:
                    self.sheets_service.log_deleted_page_alert(url, status_code, last_success)
                
                print(f"\nDeleted page detected: {url} (Status: {status_code})")
                
                # Record performance for deleted page
                page_type = "deleted"
                crawl_time = time.time() - start_time
                self.state_manager.record_page_crawl(url, crawl_time, page_type)
                
                # CRITICAL FIX: Mark as visited to prevent duplicate processing in same cycle
                self.state_manager.add_visited_url(url)
                return  # Don't process further
            
            if not soup:
                # Page failed to load but not classified as deleted yet
                print(f"\nFailed to load page {url} (Status: {status_code})")
                
                # Record performance for failed page
                page_type = "failed"
                crawl_time = time.time() - start_time
                self.state_manager.record_page_crawl(url, crawl_time, page_type)
                
                # CRITICAL FIX: Mark as visited to prevent duplicate processing in same cycle
                self.state_manager.add_visited_url(url)
                return

            # Intelligent file type categorization - only monitor availability for non-HTML content
            file_type = self._categorize_file_type(url)
            if file_type != "webpage":
                print(f"\n{file_type.title()} available: {url}")
                self.state_manager.add_visited_url(url)
                crawl_time = time.time() - start_time
                self.state_manager.record_page_crawl(url, crawl_time, file_type)
                return

            # Validate soup object before processing
            if not soup or not hasattr(soup, 'prettify'):
                print(f"❌ Invalid soup object for {url} - skipping")
                self.state_manager.add_visited_url(url)
                crawl_time = time.time() - start_time
                self.state_manager.record_page_crawl(url, crawl_time, "failed")
                return
            
            # Check if soup has meaningful content
            soup_text = soup.get_text(strip=True)
            if len(soup_text) < 50:  # Very short content might be an error page
                print(f"⚠️  Very short content for {url} ({len(soup_text)} chars) - might be error page")
            
            print(f"📄 Processing page: {url} (content length: {len(soup_text)} chars)")
            
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
            
            # Verify file was written correctly and has content
            if not os.path.exists(filename) or os.path.getsize(filename) == 0:
                raise Exception(f"Failed to save page content to {filename}")
            
            # Additional content validation - ensure HTML has meaningful content
            with open(filename, "r", encoding="utf-8") as f:
                content = f.read()
                if len(content.strip()) < 100:  # Too short to be meaningful HTML
                    raise Exception(f"File content too short ({len(content)} chars) - likely empty or corrupted")
                if "<html" not in content.lower() and "<!doctype" not in content.lower():
                    raise Exception(f"File doesn't appear to be valid HTML content")
            
            print(f"📄 Page content saved: {filename} ({len(content)} chars)")
            
            # Take screenshot locally (most likely to fail)
            screenshot_path, _ = page_browser.save_screenshot(url)
            
            # Verify screenshot was created
            if screenshot_path and (not os.path.exists(screenshot_path) or os.path.getsize(screenshot_path) == 0):
                print(f"⚠️  Screenshot failed or empty: {screenshot_path}")
                screenshot_path = None

            # PHASE 2: Only create Drive folders after local operations succeed
            if self.drive_service:
                folder_id, folder_status = self.drive_service.get_or_create_folder(safe_filename, TOP_PARENT_ID)
                if folder_status == 'new':
                    created_folder_ids.append(folder_id)
                    
                html_folder_id, html_status = self.drive_service.get_or_create_folder("HTML", folder_id)
                if html_status == 'new':
                    created_folder_ids.append(html_folder_id)
                    
                screenshot_folder_id, screenshot_status = self.drive_service.get_or_create_folder("SCREENSHOT", folder_id)
                if screenshot_status == 'new':
                    created_folder_ids.append(screenshot_folder_id)

                # PHASE 3: Defer screenshot upload until we know if page is new/changed
                screenshot_url = None

                # Store Drive folder URLs in database (for both discovery AND recrawl)
                folder_ids = {
                    'main_folder_id': folder_id,
                    'html_folder_id': html_folder_id,
                    'screenshot_folder_id': screenshot_folder_id
                }
                self.state_manager.update_drive_folders(url, folder_ids)

                # Handle file versions in Drive
                new_file_id = self.drive_service.find_file(os.path.basename(filename), html_folder_id)
                old_file_id = self.drive_service.find_file(os.path.basename(old_file), html_folder_id)
            else:
                # Basic mode: use local storage only
                folder_id = html_folder_id = screenshot_folder_id = None
                new_file_id = old_file_id = None
                screenshot_url = None
                folder_ids = {}
                print(f"📁 Drive service not available - using local storage only")

            # Check if this is a new page
            # FIXED: Don't rely on was_visited() since pages might be marked visited before upload
            # Instead, check if we have an old file in Drive to compare against
            is_new_page = not old_file_id
            
            has_changes = False
            if is_new_page:
                page_type = "new"
                has_changes = True  # Always upload new pages
                print(f"🆕 New page detected: {url} - will upload to Drive")
                
                # Send new page notification using format_change_message
                if self.slack_service:
                    if self.drive_service:
                        blocks = self.slack_service.format_change_message(
                            url,
                            [], [], [],  # No content changes for new page
                            {'added_links': set(), 'removed_links': set(), 'added_pdfs': set(), 'removed_pdfs': set()},
                            f"https://drive.google.com/drive/folders/{screenshot_folder_id}",
                            f"https://drive.google.com/drive/folders/{html_folder_id}",
                            is_new_page=True
                        )
                    else:
                        blocks = self.slack_service.format_change_message(
                            url,
                            [], [], [],  # No content changes for new page
                            {'added_links': set(), 'removed_links': set(), 'added_pdfs': set(), 'removed_pdfs': set()},
                            "Local storage only",
                            "Local storage only",
                            is_new_page=True
                        )
                    self.slack_service.send_message(blocks)
                
                # Log to Google Sheets
                if self.sheets_service:
                    if self.drive_service:
                        self.sheets_service.log_new_page_alert(
                            url,
                            f"https://drive.google.com/drive/folders/{screenshot_folder_id}",
                            f"https://drive.google.com/drive/folders/{html_folder_id}"
                        )
                    else:
                        self.sheets_service.log_new_page_alert(
                            url,
                            "Local storage only",
                            "Local storage only"
                        )
            elif old_file_id and self.drive_service:
                # Compare versions for existing page
                print(f"🔄 Existing page detected: {url} - comparing for changes")
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
                    has_changes = True
                    print(f"📝 Changes detected in {url} - will upload updated version")
                    
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
                    
                    if self.slack_service:
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
                else:
                    print(f"✅ No changes detected in {url} - skipping upload")
                    page_type = "unchanged"
                    has_changes = False

                # Clean up old files
                os.remove(old_file)

            # Upload new version and rename old version ONLY when page is new or changed
            upload_success = False
            print("BEFORE CHECK HAS CHANGES")
            if has_changes and self.drive_service:
                try:
                    # Add delay before upload to prevent hitting API quotas
                    print(f"📤 4. Preparing to upload files...")
                    print(f"   ⏳ API quota protection delay: 3 seconds...")
                    
                    if new_file_id:
                        self.drive_service.rename_file(new_file_id, os.path.basename(old_file))
                    
                    # Upload HTML file with validation
                    print(f"📤 Starting HTML file upload...")
                    html_upload_result = self.drive_service.upload_file(filename, html_folder_id)
                    if not html_upload_result:
                        raise Exception(f"Failed to upload HTML file: {filename}")
                    
                    # Add delay between uploads to prevent quota issues
                    print(f"   ⏳ Inter-upload delay: 2 seconds...")

                    
                    # Upload screenshot only if new/changed and available
                    if screenshot_path:
                        print(f"📤 Starting screenshot upload...")
                        screenshot_upload_result = self.drive_service.upload_file(screenshot_path, screenshot_folder_id)
                        if not screenshot_upload_result:
                            print(f"⚠️  Screenshot upload failed: {screenshot_path}")
                    
                    upload_success = True
                    print(f"✅ Files uploaded successfully to Drive")
                    
                    # Track successful uploads
                    if hasattr(self, 'upload_stats'):
                        self.upload_stats['successful'] += 1
                        print(f"📊 Upload stats: {self.upload_stats['successful']} successful, {self.upload_stats['failed']} failed")
                    
                except Exception as upload_error:
                    print(f"❌ Upload failed: {upload_error}")
                    
                    # Track failed uploads and categorize errors
                    if hasattr(self, 'upload_stats'):
                        self.upload_stats['failed'] += 1
                        if 'quota' in str(upload_error).lower() or 'rate' in str(upload_error).lower():
                            self.upload_stats['quota_errors'] += 1
                            print(f"⚠️  Quota-related error detected - consider reducing upload frequency")
                        else:
                            self.upload_stats['other_errors'] += 1
                        print(f"📊 Upload stats: {self.upload_stats['successful']} successful, {self.upload_stats['failed']} failed")
                    
                    # Don't delete local files if upload failed
                    upload_success = False
            
            # Clean up local files ONLY after successful upload
            if upload_success:
                if screenshot_path and os.path.exists(screenshot_path):
                    os.remove(screenshot_path)
                    print(f"🗑️  Local screenshot cleaned up: {screenshot_path}")
                os.remove(filename)
                print(f"🗑️  Local HTML file cleaned up: {filename}")
            else:
                # Keep files for debugging if upload failed
                print(f"📁 Keeping local files for debugging (upload failed)")
                if screenshot_path and os.path.exists(screenshot_path):
                    print(f"   📸 Screenshot: {screenshot_path}")
                print(f"   📄 HTML: {filename}")

            # Extract new links to crawl
            new_links = extract_links(url, soup, CHECK_PREFIX)
            self.state_manager.add_new_urls(new_links)

            # CRITICAL FIX: Only mark as visited AFTER successful upload
            # This ensures pages get uploaded before being marked as "done"
            if upload_success:
                # Update state only after successful upload
                self.state_manager.add_visited_url(url)
                self.state_manager.log_scanned_page(url)
                
                # Record performance metrics
                crawl_time = time.time() - start_time
                change_details_for_perf = change_details if 'change_details' in locals() else None
                self.state_manager.record_page_crawl(url, crawl_time, page_type, change_details_for_perf)
                
                print(f"✅ Page {url} completed and uploaded successfully")
            else:
                # If upload failed, don't mark as visited - it will be retried
                print(f"⚠️  Page {url} upload failed - will be retried in next cycle")
                # Still record the failed attempt for performance tracking
                crawl_time = time.time() - start_time
                self.state_manager.record_page_crawl(url, crawl_time, "failed")

        except Exception as e:
            # Rollback any newly created folders to prevent orphans
            if 'created_folder_ids' in locals() and self.drive_service:
                for folder_id in created_folder_ids:
                    try:
                        self.drive_service.delete_folder(folder_id)
                        print(f"🗑️  Cleaned up orphaned folder: {folder_id}")
                    except Exception as cleanup_error:
                        print(f"⚠️  Could not clean up folder {folder_id}: {cleanup_error}")
            
            if self.slack_service:
                self.slack_service.send_error(str(e), url)
            print(f"\nError processing page {url}: {e}")
            
            # Record performance for errored page
            crawl_time = time.time() - start_time
            self.state_manager.record_page_crawl(url, crawl_time, "failed")
        finally:
            # cleanup the page-specific browser instance
            if 'page_browser' in locals():
                page_browser.quit()

            # Send finish log with started and ended timestamps and duration
            try:
                end_timestamp_utc = datetime.utcnow().isoformat() + "Z"
                duration_sec = int(time.time() - start_time)
                finish_text = (
                    f"URL={url} crawl_finished | started={start_timestamp_utc} | ended={end_timestamp_utc} | "
                    f"duration_sec={duration_sec} | type={page_type}"
                )
                requests.post(
                    "https://ca55da625cee.ngrok-free.app/log",
                    data={"log": finish_text},
                    timeout=5,
                )
            except Exception:
                # Ignore telemetry errors
                pass

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
        """Main crawl loop with threading and concurrent task handling."""
        try:
            pages_processed_this_session = 0
            # Create ThreadPoolExecutor with a maximum number of workers (adjust this value as needed)
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = []  # List to keep track of the future tasks

                while True:
                    url = self.state_manager.get_next_url()
                    if not url:
                        # Check if we completed a full cycle
                        if pages_processed_this_session > 0:
                            print(f"\n🎉 Completed crawl cycle! Processed {pages_processed_this_session} pages this session.")
                            self.state_manager.complete_cycle()
                            pages_processed_this_session = 0
                        
                        print("\nNo URLs remaining. Waiting for recrawl...")
                        print("⏳ Waiting 3 seconds before checking for new URLs...")

                        continue
                    
                    # Clean URL and filter based on conditions
                    url = url.rstrip("/")
                    if (CHECK_PREFIX and url.startswith(CHECK_PREFIX)):
                        continue
                    if any(url.startswith(prefix) for prefix in EXCLUDE_PREFIXES):
                        continue
                    
                    year_match = re.search(r'/(\d{4})/', url)
                    if year_match:
                        year = int(year_match.group(1))
                        if year <= 2014:
                            print(f"⏭️ Skipping old URL (year {year}): {url}")
                            continue

                    # Submit the task for processing and collect the future
                    future = executor.submit(self.process_page, url)
                    futures.append(future)  # Add future to the list

                    # Show progress and handle completed tasks
                    for future in as_completed(futures):
                        try:
                            future.result()  # Wait for the task to finish and process results
                            pages_processed_this_session += 1
                        except Exception as exc:
                            print(f"❌ Error processing a page: {exc}")

                    # Show progress every 10 pages
                    if pages_processed_this_session % 10 == 0:
                        stats = self.state_manager.get_progress_stats()
                        print(f"\n📊 Progress: {stats['completed_pages']}/{stats['total_known_pages']} ({stats['progress_percent']}%) - {stats['pages_per_hour']:.0f} pages/hour")
                        if stats['eta_datetime']:
                            print(f"⏰ ETA: {stats['eta_datetime'].strftime('%I:%M %p today' if stats['eta_datetime'].date() == datetime.now().date() else '%b %d at %I:%M %p')}")

                    # Memory optimization for Render deployment
                    if pages_processed_this_session % self.memory_check_interval == 0:
                        self._check_and_optimize_memory()

                    # Rescue stuck URLs every 50 pages (roughly every 25-30 minutes)
                    if pages_processed_this_session % 50 == 0:
                        self.state_manager.rescue_stuck_urls(stuck_minutes=60)


        except KeyboardInterrupt:
            print("\nCrawling interrupted by user.")
        except Exception as e:
            print(f"Error: {e}")


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

    def _check_and_optimize_memory(self) -> None:
        """Monitor and optimize memory usage to prevent Render failures."""
        try:
            import psutil
            process = psutil.Process(os.getpid())
            memory_mb = process.memory_info().rss / (1024 * 1024)
            memory_percent = memory_mb / self.max_memory_mb
            
            print(f"🧠 Memory: {memory_mb:.1f}MB / {self.max_memory_mb}MB ({memory_percent:.1%})")
            
            # Force garbage collection if memory usage is high
            if memory_percent > self.gc_threshold:
                print("🔄 High memory usage detected - forcing garbage collection...")
                gc.collect()
                
                # Check memory after GC
                memory_mb_after = process.memory_info().rss / (1024 * 1024)
                freed_mb = memory_mb - memory_mb_after
                print(f"✅ Freed {freed_mb:.1f}MB of memory")
                
                # If still high, consider more aggressive cleanup
                if memory_mb_after / self.max_memory_mb > 0.9:
                    print("⚠️  Critical memory usage - clearing performance history...")
                    self.state_manager._clear_old_performance_data()
                    
        except ImportError:
            # psutil not available, skip memory monitoring
            pass
        except Exception as e:
            print(f"⚠️  Memory monitoring error: {e}")
