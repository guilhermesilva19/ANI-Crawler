import pickle
import os
from datetime import datetime, timedelta
from typing import Set, Dict, Optional, Tuple, List
import pytz
from src.config import DATA_FILE, NEXT_CRAWL_FILE, SCANNED_PAGES_FILE, TARGET_URLS

__all__ = ['StateManager']

class StateManager:
    def __init__(self):
        self.visited_urls: Set[str] = set()
        self.remaining_urls: Set[str] = set(TARGET_URLS)
        self.next_crawl: Dict[str, datetime] = {}
        # Track URL status for deleted page detection
        self.url_status: Dict[str, Dict] = {}  # {url: {'status': int, 'last_success': datetime, 'error_count': int}}
        
        # Progress tracking for dashboard
        self.total_pages_estimate: int = 5196  # From sitemap analysis
        self.cycle_start_time: Optional[datetime] = None
        self.current_cycle: int = 1
        self.is_first_cycle: bool = True
        self.daily_stats: Dict[str, Dict] = {}  # {date: {pages_crawled, new_pages, changed_pages, failed_pages}}
        self.performance_history: List[Dict] = []  # Track crawling speed over time
        
        # Timezone for daily stats (must match dashboard report timezone)
        self.aest_tz = pytz.timezone('Australia/Sydney')
        
        self.load_progress()

    def load_progress(self) -> None:
        """Load saved crawl progress from files."""
        try:
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE, "rb") as f:
                    data = pickle.load(f)
                    # Handle different data formats
                    if isinstance(data, tuple) and len(data) == 2:
                        # Old format: (visited_urls, remaining_urls)
                        self.visited_urls, self.remaining_urls = data
                        self.url_status = {}
                        self._initialize_progress_tracking()
                    elif isinstance(data, tuple) and len(data) == 3:
                        # Medium format: (visited_urls, remaining_urls, url_status)
                        self.visited_urls, self.remaining_urls, self.url_status = data
                        self._initialize_progress_tracking()
                    elif isinstance(data, dict):
                        # New format: full state dictionary
                        self.visited_urls = data.get('visited_urls', set())
                        self.remaining_urls = data.get('remaining_urls', set(TARGET_URLS))
                        self.url_status = data.get('url_status', {})
                        self.total_pages_estimate = data.get('total_pages_estimate', 5196)
                        self.cycle_start_time = data.get('cycle_start_time')
                        self.current_cycle = data.get('current_cycle', 1)
                        self.is_first_cycle = data.get('is_first_cycle', True)
                        self.daily_stats = data.get('daily_stats', {})
                        self.performance_history = data.get('performance_history', [])
                    
                if not self.remaining_urls:
                    self.remaining_urls.update(TARGET_URLS)
                    print("\nNo remaining URLs found. Resetting crawl.")
                else:
                    print("\nResuming from previous session...")
                    
                # Initialize cycle start time if not set
                if self.cycle_start_time is None:
                    self.cycle_start_time = datetime.now()

            if os.path.exists(NEXT_CRAWL_FILE):
                with open(NEXT_CRAWL_FILE, "rb") as f:
                    self.next_crawl = pickle.load(f)
        except Exception as e:
            print(f"\nError loading progress: {e}")
            # Reset state if loading fails
            self.visited_urls = set()
            self.remaining_urls = set(TARGET_URLS)
            self.next_crawl = {}
            self.url_status = {}
            self._initialize_progress_tracking()
    
    def _initialize_progress_tracking(self) -> None:
        """Initialize progress tracking fields for backward compatibility."""
        if not hasattr(self, 'cycle_start_time') or self.cycle_start_time is None:
            self.cycle_start_time = datetime.now()
        if not hasattr(self, 'current_cycle'):
            self.current_cycle = 1
        if not hasattr(self, 'is_first_cycle'):
            self.is_first_cycle = True
        if not hasattr(self, 'daily_stats'):
            self.daily_stats = {}
        if not hasattr(self, 'performance_history'):
            self.performance_history = []

    def save_progress(self) -> None:
        """Save current crawl progress to files."""
        try:
            # Save in new dictionary format with all progress tracking data
            state_data = {
                'visited_urls': self.visited_urls,
                'remaining_urls': self.remaining_urls,
                'url_status': self.url_status,
                'total_pages_estimate': self.total_pages_estimate,
                'cycle_start_time': self.cycle_start_time,
                'current_cycle': self.current_cycle,
                'is_first_cycle': self.is_first_cycle,
                'daily_stats': self.daily_stats,
                'performance_history': self.performance_history
            }
            
            with open(DATA_FILE, "wb") as f:
                pickle.dump(state_data, f)
            with open(NEXT_CRAWL_FILE, "wb") as f:
                pickle.dump(self.next_crawl, f)
            print("\nProgress saved.")
        except Exception as e:
            print(f"\nError saving progress: {e}")

    def log_scanned_page(self, page_url: str) -> None:
        """Log scanned pages to a file with timestamp."""
        try:
            with open(SCANNED_PAGES_FILE, "a", encoding="utf-8") as f:
                f.write(f"{datetime.now()} - {page_url}\n")
        except Exception as e:
            print(f"\nError logging scanned page: {e}")

    def was_visited(self, url: str) -> bool:
        """Check if a URL has been visited before."""
        return url in self.visited_urls

    def add_visited_url(self, url: str) -> None:
        """Add URL to visited set and update next crawl time."""
        self.visited_urls.add(url)
        self.next_crawl[url] = datetime.now()
        self.save_progress()

    def update_url_status(self, url: str, status_code: int) -> bool:
        """Update URL status and return True if this indicates a deleted page."""
        now = datetime.now()
        
        # Initialize if first time seeing this URL
        if url not in self.url_status:
            self.url_status[url] = {
                'status': status_code,
                'last_success': now if status_code < 400 else None,
                'error_count': 0
            }
            return False  # New URL, not deleted
        
        previous_status = self.url_status[url]
        
        # Update current status
        if status_code < 400:
            # Successful access
            self.url_status[url] = {
                'status': status_code,
                'last_success': now,
                'error_count': 0
            }
            return False
        else:
            # Error status
            self.url_status[url]['status'] = status_code
            self.url_status[url]['error_count'] += 1
            
            # Check if this is a newly deleted page
            had_previous_success = previous_status.get('last_success') is not None
            is_permanent_error = status_code in [404, 410]  # Not Found, Gone
            multiple_failures = self.url_status[url]['error_count'] >= 2
            
            # Return True if this appears to be a deleted page
            return had_previous_success and (is_permanent_error or multiple_failures)

    def add_new_urls(self, urls: Set[str]) -> None:
        """Add new URLs to remaining set if not visited."""
        new_urls = urls - self.visited_urls
        self.remaining_urls.update(new_urls)
        self.save_progress()

    def get_next_url(self) -> Optional[str]:
        """Get the next URL to crawl."""
        if not self.remaining_urls:
            # Check for URLs that need recrawling
            now = datetime.now()
            for url, last_crawl in self.next_crawl.items():
                if self.should_recrawl(url):
                    self.remaining_urls.add(url)
            
            if not self.remaining_urls:
                return None

        return self.remaining_urls.pop()

    def should_recrawl(self, url: str, recrawl_days: int = 3) -> bool:
        """Check if a URL should be recrawled based on last crawl time."""
        if url not in self.next_crawl:
            return True
        last_crawl = self.next_crawl[url]
        return (datetime.now() - last_crawl).days >= recrawl_days

    def get_crawl_stats(self) -> Dict[str, int]:
        """Get current crawl statistics."""
        return {
            "visited_urls": len(self.visited_urls),
            "remaining_urls": len(self.remaining_urls),
            "total_known_urls": len(self.visited_urls) + len(self.remaining_urls)
        }
    
    def record_page_crawl(self, url: str, crawl_time_seconds: float, page_type: str = "normal", change_details: Dict = None) -> None:
        """Record a page crawl for performance tracking."""
        # Use AEST timezone for daily stats to match dashboard report
        today = datetime.now(self.aest_tz).strftime("%Y-%m-%d")
        
        # Initialize today's stats if needed
        if today not in self.daily_stats:
            self.daily_stats[today] = {
                'pages_crawled': 0,
                'new_pages': 0,
                'changed_pages': 0,
                'failed_pages': 0,
                'deleted_pages': 0,
                'document_pages': 0,
                'total_time': 0.0
            }
        
        # Update daily stats
        self.daily_stats[today]['pages_crawled'] += 1
        self.daily_stats[today]['total_time'] += crawl_time_seconds
        
        if page_type == "new":
            self.daily_stats[today]['new_pages'] += 1
        elif page_type == "changed":
            self.daily_stats[today]['changed_pages'] += 1
        elif page_type == "failed":
            self.daily_stats[today]['failed_pages'] += 1
        elif page_type == "deleted":
            self.daily_stats[today]['deleted_pages'] += 1
        elif page_type == "document":
            self.daily_stats[today]['document_pages'] += 1
        
        # Update performance history (keep last 100 entries)
        perf_entry = {
            'timestamp': datetime.now(),
            'url': url,
            'crawl_time': crawl_time_seconds,
            'page_type': page_type
        }
        
        # Add change details if provided
        if change_details:
            perf_entry['change_details'] = change_details
            
        self.performance_history.append(perf_entry)
        
        # Keep only recent history to prevent memory bloat
        if len(self.performance_history) > 100:
            self.performance_history = self.performance_history[-100:]
    
    def store_page_changes(self, url: str, change_details: Dict) -> None:
        """Store detailed page change information (file-based fallback)."""
        # For file-based storage, we'll just log this information
        # In production, this should use MongoDB
        try:
            import json
            change_log_file = "page_changes.jsonl"
            
            change_record = {
                "timestamp": datetime.now().isoformat(),
                "url": url,
                "change_details": change_details
            }
            
            with open(change_log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(change_record) + "\n")
                
            print(f"📝 Logged change details for {url} to {change_log_file}")
        except Exception as e:
            print(f"Error storing page changes: {e}")
    
    def get_progress_stats(self) -> Dict:
        """Get comprehensive progress statistics for dashboard."""
        completed_pages = len(self.visited_urls)
        remaining_pages = len(self.remaining_urls)
        
        # Calculate progress percentage based on discovered pages (queue-based)
        total_known_pages = completed_pages + remaining_pages
        if total_known_pages > 0:
            progress_percent = (completed_pages / total_known_pages) * 100
        else:
            progress_percent = 0.0
        
        # Calculate average processing time per page for backward compatibility
        recent_performance_entries = self.performance_history[-20:] if self.performance_history else []
        average_processing_time = sum(entry['crawl_time'] for entry in recent_performance_entries) / len(recent_performance_entries) if recent_performance_entries else 15.0
        
        # Calculate estimated time to completion using interval-based throughput analysis
        if remaining_pages > 0:
            # Primary method: Use interval-based throughput calculation
            interval_based_throughput = self._calculate_throughput_from_intervals()
            
            if interval_based_throughput > 0:
                # Calculate completion time based on actual processing intervals
                estimated_completion_hours = remaining_pages / interval_based_throughput
                eta_datetime = datetime.now() + timedelta(hours=estimated_completion_hours)
            else:
                # Fallback method: Use individual page processing times
                # Note: Uses remaining pages count for accurate estimation
                estimated_completion_seconds = remaining_pages * average_processing_time
                eta_datetime = datetime.now() + timedelta(seconds=estimated_completion_seconds)
        else:
            eta_datetime = None
        
        # Calculate current processing rate using interval analysis when available
        interval_based_throughput = self._calculate_throughput_from_intervals()
        if interval_based_throughput > 0:
            pages_per_hour = interval_based_throughput
        elif average_processing_time > 0:
            # Fallback: Use processing time only (less accurate)
            pages_per_hour = 3600 / average_processing_time
        else:
            pages_per_hour = 0
        
        # Get today's stats using AEST timezone to match dashboard report
        today = datetime.now(self.aest_tz).strftime("%Y-%m-%d")
        today_stats = self.daily_stats.get(today, {
            'pages_crawled': 0, 'new_pages': 0, 'changed_pages': 0, 'failed_pages': 0
        })
        
        # Calculate cycle duration
        cycle_duration = datetime.now() - self.cycle_start_time if self.cycle_start_time else timedelta(0)
        
        return {
            'completed_pages': completed_pages,
            'total_known_pages': total_known_pages,
            'remaining_pages': remaining_pages,
            'progress_percent': round(progress_percent, 1),
            'avg_crawl_time_seconds': round(avg_crawl_time, 1),
            'pages_per_hour': round(pages_per_hour, 0),
            'eta_datetime': eta_datetime,
            'cycle_number': self.current_cycle,
            'is_first_cycle': self.is_first_cycle,
            'cycle_duration_days': cycle_duration.days,
            'today_stats': today_stats,
            'total_discovered': total_known_pages
        }
    
    def complete_cycle(self) -> None:
        """Mark current cycle as complete and prepare for next cycle."""
        self.current_cycle += 1
        self.is_first_cycle = False
        self.cycle_start_time = datetime.now()
        
        # Update total pages estimate based on actual discovery
        actual_total = len(self.visited_urls) + len(self.remaining_urls)
        if actual_total > self.total_pages_estimate:
            print(f"\n📊 Updating total pages estimate: {self.total_pages_estimate} → {actual_total}")
            self.total_pages_estimate = actual_total
        
        self.save_progress()
        print(f"\n🔄 Cycle {self.current_cycle - 1} completed. Starting cycle {self.current_cycle}")
    
    def update_total_pages_estimate(self, new_estimate: int) -> None:
        """Update the total pages estimate."""
        if new_estimate != self.total_pages_estimate:
            print(f"📊 Updating total pages estimate: {self.total_pages_estimate} → {new_estimate}")
            self.total_pages_estimate = new_estimate
            self.save_progress()
    
    def _calculate_throughput_from_intervals(self) -> float:
        """
        Calculate actual crawling throughput based on time intervals between page completions.
        
        This method analyzes the time elapsed between consecutive page crawls to determine
        the true processing rate, including system delays and processing overhead.
        
        Returns:
            float: Pages processed per hour based on actual interval analysis.
                   Returns 0.0 if insufficient data is available.
        """
        if len(self.performance_history) < 2:
            return 0.0
        
        # Extract recent performance entries for analysis
        recent_performance_data = self.performance_history[-20:]
        chronologically_sorted_data = sorted(recent_performance_data, key=lambda entry: entry['timestamp'])
        
        total_interval_duration = 0.0
        validated_interval_count = 0
        
        # Analyze time intervals between consecutive page completions
        for current_index in range(1, len(chronologically_sorted_data)):
            current_completion_time = chronologically_sorted_data[current_index]['timestamp']
            previous_completion_time = chronologically_sorted_data[current_index - 1]['timestamp']
            interval_duration_seconds = (current_completion_time - previous_completion_time).total_seconds()
            
            # Validate interval duration to exclude outliers and system interruptions
            # Acceptable range: 10 seconds to 10 minutes per page
            if 10 <= interval_duration_seconds <= 600:
                total_interval_duration += interval_duration_seconds
                validated_interval_count += 1
        
        # Calculate throughput rate
        if validated_interval_count > 0:
            average_interval_duration = total_interval_duration / validated_interval_count
            pages_processed_per_hour = 3600 / average_interval_duration
            return pages_processed_per_hour
        
        return 0.0
    
 