"""Clean MongoDB-only state management for ANI-Crawler."""

import hashlib
from datetime import datetime, timedelta
from typing import Set, Dict, Optional, List
import pytz
try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, DuplicateKeyError
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    MongoClient = None

from src.config import MONGODB_URI, SITE_ID, TARGET_URLS

__all__ = ['MongoStateAdapter']


class MongoStateAdapter:
    """Clean MongoDB-only state management for multi-tenant crawler."""
    
    def __init__(self):
        """Initialize MongoDB state management."""
        if not MONGODB_AVAILABLE:
            raise ImportError("MongoDB dependencies required. Install: pip install pymongo>=4.10.1")
        
        if not MONGODB_URI:
            raise ValueError("MONGODB_URI environment variable required")
        
        if not SITE_ID:
            raise ValueError("SITE_ID environment variable required")
        
        # Initialize state attributes
        self.visited_urls: Set[str] = set()
        self.remaining_urls: Set[str] = set(TARGET_URLS)
        self.next_crawl: Dict[str, datetime] = {}
        self.url_status: Dict[str, Dict] = {}
        self.total_pages_estimate: int = 5196
        self.cycle_start_time: Optional[datetime] = None
        self.current_cycle: int = 1
        self.is_first_cycle: bool = True
        self.daily_stats: Dict[str, Dict] = {}
        self.performance_history: List[Dict] = []
        self.aest_tz = pytz.timezone('Australia/Sydney')
        
        # MongoDB setup
        self.site_id = SITE_ID
        self.client = None
        self.db = None
        
        # Initialize connection and load data
        self._initialize_connection()
        self.load_progress()
    
    def _initialize_connection(self):
        """Initialize MongoDB connection."""
        try:
            self.client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client.crawler_data
            self._ensure_indexes()
            print(f"✅ MongoDB connected: {self.site_id}")
        except ConnectionFailure as e:
            raise ConnectionError(f"Failed to connect to MongoDB: {e}")
    
    def _ensure_indexes(self):
        """Create optimized indexes for performance."""
        # Site-specific indexes for multi-tenancy
        self.db.url_states.create_index([("site_id", 1), ("url", 1)], unique=True)
        self.db.url_states.create_index([("site_id", 1), ("status", 1)])
        self.db.daily_stats.create_index([("site_id", 1), ("date", 1)], unique=True)
        self.db.performance_history.create_index([("site_id", 1), ("timestamp", -1)])
        # Auto-cleanup old performance data (30 days)
        self.db.performance_history.create_index("timestamp", expireAfterSeconds=2592000)
    
    def load_progress(self) -> None:
        """Load saved crawl progress from MongoDB."""
        try:
            # Load site state
            site_doc = self.db.site_states.find_one({"site_id": self.site_id})
            if site_doc:
                self.total_pages_estimate = site_doc.get('total_pages_estimate', 5196)
                self.cycle_start_time = site_doc.get('cycle_start_time')
                self.current_cycle = site_doc.get('current_cycle', 1)
                self.is_first_cycle = site_doc.get('is_first_cycle', True)
                print("📊 Resuming from previous session...")
            else:
                # Initialize new site
                self._initialize_site_state()
                print("🆕 Initializing new site...")
            
            # Load URL states into memory sets
            self._load_url_states()
            
            # Load daily stats
            self._load_daily_stats()
            
            # Load recent performance history
            self._load_performance_history()
            
            # Initialize cycle start time if not set
            if self.cycle_start_time is None:
                self.cycle_start_time = datetime.now()
                self.save_progress()
                
        except Exception as e:
            print(f"Error loading from MongoDB: {e}")
            # Initialize fresh state on error
            self._initialize_fresh_state()
    
    def _initialize_site_state(self):
        """Initialize state for a new site."""
        self.db.site_states.insert_one({
            "site_id": self.site_id,
            "total_pages_estimate": self.total_pages_estimate,
            "cycle_start_time": datetime.now(),
            "current_cycle": 1,
            "is_first_cycle": True,
            "created_at": datetime.now()
        })
    
    def _load_url_states(self):
        """Load URL states into memory with validation."""
        # Clear memory and rebuild from DB to ensure consistency
        old_visited_count = len(self.visited_urls)
        old_remaining_count = len(self.remaining_urls)
        
        self.visited_urls = set()
        self.remaining_urls = set()
        self.next_crawl = {}
        self.url_status = {}
        
        # Load visited URLs
        visited_docs = self.db.url_states.find({
            "site_id": self.site_id,
            "status": "visited"
        }, {"url": 1, "last_crawled": 1})
        
        for doc in visited_docs:
            self.visited_urls.add(doc['url'])
            self.next_crawl[doc['url']] = doc.get('last_crawled', datetime.now())
        
        # Load remaining URLs
        remaining_docs = self.db.url_states.find({
            "site_id": self.site_id,
            "status": "remaining"
        }, {"url": 1})
        
        for doc in remaining_docs:
            self.remaining_urls.add(doc['url'])
        
        # If no remaining URLs, initialize with targets
        if not self.remaining_urls:
            self.remaining_urls.update(TARGET_URLS)
            # Add to database
            for url in TARGET_URLS:
                self.db.url_states.update_one(
                    {"site_id": self.site_id, "url": url},
                    {"$setOnInsert": {
                        "site_id": self.site_id,
                        "url": url,
                        "status": "remaining",
                        "first_seen": datetime.now()
                    }},
                    upsert=True
                )
        
        # Load URL status info
        status_docs = self.db.url_states.find({
            "site_id": self.site_id,
            "status_info": {"$exists": True}
        }, {"url": 1, "status_info": 1})
        
        for doc in status_docs:
            if 'status_info' in doc:
                self.url_status[doc['url']] = doc['status_info']
        
        # Log any significant discrepancies for monitoring
        new_visited_count = len(self.visited_urls)
        new_remaining_count = len(self.remaining_urls)
        
        if abs(old_visited_count - new_visited_count) > 10 or abs(old_remaining_count - new_remaining_count) > 10:
            print(f"📊 Memory sync: visited {old_visited_count}→{new_visited_count}, remaining {old_remaining_count}→{new_remaining_count}")
    
    def _load_daily_stats(self):
        """Load daily statistics."""
        stats_docs = self.db.daily_stats.find({"site_id": self.site_id})
        for doc in stats_docs:
            self.daily_stats[doc['date']] = doc['stats']
    
    def _load_performance_history(self):
        """Load recent performance history (last 100 entries)."""
        perf_docs = self.db.performance_history.find({
            "site_id": self.site_id
        }).sort("timestamp", -1).limit(100)
        
        self.performance_history = list(perf_docs)
        self.performance_history.reverse()  # Maintain chronological order
    
    def _initialize_fresh_state(self):
        """Initialize fresh state on error."""
        self.visited_urls = set()
        self.remaining_urls = set(TARGET_URLS)
        self.next_crawl = {}
        self.url_status = {}
        self.cycle_start_time = datetime.now()
    
    def save_progress(self) -> None:
        """Save current crawl progress to MongoDB."""
        try:
            # Update site state
            self.db.site_states.update_one(
                {"site_id": self.site_id},
                {
                    "$set": {
                        "total_pages_estimate": self.total_pages_estimate,
                        "cycle_start_time": self.cycle_start_time,
                        "current_cycle": self.current_cycle,
                        "is_first_cycle": self.is_first_cycle,
                        "updated_at": datetime.now()
                    }
                },
                upsert=True
            )
            
            # Sync daily stats
            for date, stats in self.daily_stats.items():
                self.db.daily_stats.update_one(
                    {"site_id": self.site_id, "date": date},
                    {"$set": {"stats": stats}},
                    upsert=True
                )
            
            print("💾 Progress saved to MongoDB.")
        except Exception as e:
            print(f"Error saving progress: {e}")
    
    def log_scanned_page(self, page_url: str) -> None:
        """Log scanned page to MongoDB."""
        try:
            # Store in MongoDB audit collection
            self.db.audit_log.insert_one({
                "site_id": self.site_id,
                "timestamp": datetime.now(),
                "page_url": page_url,
                "action": "scanned"
            })
        except Exception as e:
            print(f"Error logging scanned page: {e}")
    
    def was_visited(self, url: str) -> bool:
        """Check if a URL has been visited before."""
        return url in self.visited_urls
    
    def add_visited_url(self, url: str) -> None:
        """Add URL to visited with database-first approach."""
        # Update database first
        result = self.db.url_states.update_one(
            {"site_id": self.site_id, "url": url},
            {
                "$set": {
                    "status": "visited",
                    "last_crawled": datetime.now(),
                    "updated_at": datetime.now()
                },
                "$setOnInsert": {
                    "site_id": self.site_id,
                    "url": url,
                    "first_seen": datetime.now()
                }
            },
            upsert=True
        )
        
        # Only update memory if database update succeeded
        if result.acknowledged:
            self.visited_urls.add(url)
            self.next_crawl[url] = datetime.now()
            self.remaining_urls.discard(url)
            self.save_progress()
        else:
            print(f"⚠️  Failed to update database for URL: {url}")
            # Try to reload from database to maintain consistency
            self._load_url_states()
    
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
            # Sync to MongoDB
            self.db.url_states.update_one(
                {"site_id": self.site_id, "url": url},
                {"$set": {"status_info": self.url_status[url]}},
                upsert=True
            )
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
            # Sync to MongoDB
            self.db.url_states.update_one(
                {"site_id": self.site_id, "url": url},
                {"$set": {"status_info": self.url_status[url]}}
            )
            return False
        else:
            # Error status
            self.url_status[url]['status'] = status_code
            self.url_status[url]['error_count'] += 1
            
            # Check if this is a newly deleted page
            had_previous_success = previous_status.get('last_success') is not None
            is_permanent_error = status_code in [404, 410]  # Not Found, Gone
            multiple_failures = self.url_status[url]['error_count'] >= 2
            
            # Sync to MongoDB
            self.db.url_states.update_one(
                {"site_id": self.site_id, "url": url},
                {"$set": {"status_info": self.url_status[url]}}
            )
            
            # Return True if this appears to be a deleted page
            return had_previous_success and (is_permanent_error or multiple_failures)
    
    def add_new_urls(self, urls: Set[str]) -> None:
        """Add new URLs to remaining set with MongoDB sync."""
        new_urls = urls - self.visited_urls
        if not new_urls:
            return
            
        self.remaining_urls.update(new_urls)
        
        # Batch insert to MongoDB
        documents = []
        for url in new_urls:
            documents.append({
                "site_id": self.site_id,
                "url": url,
                "status": "remaining",
                "first_seen": datetime.now()
            })
        
        if documents:
            try:
                self.db.url_states.insert_many(documents, ordered=False)
            except Exception:
                # Handle duplicates gracefully
                pass
        
        self.save_progress()
    
    def get_next_url(self) -> Optional[str]:
        """Get next URL directly from database, not memory."""
        # Try to get a remaining URL
        url_doc = self.db.url_states.find_one_and_update(
            {"site_id": self.site_id, "status": "remaining"},
            {"$set": {"status": "in_progress", "updated_at": datetime.now()}},
            return_document=True
        )
        
        if url_doc:
            # Update memory to stay in sync
            url = url_doc['url']
            self.remaining_urls.discard(url)
            return url
        
        # No remaining URLs, check for recrawl candidates
        cutoff_date = datetime.now() - timedelta(days=3)
        recrawl_doc = self.db.url_states.find_one_and_update(
            {
                "site_id": self.site_id, 
                "status": "visited",
                "last_crawled": {"$lt": cutoff_date}
            },
            {"$set": {"status": "in_progress", "updated_at": datetime.now()}},
            return_document=True
        )
        
        if recrawl_doc:
            # Update memory to stay in sync
            url = recrawl_doc['url']
            self.remaining_urls.add(url)  # Add back for memory consistency
            return url
            
        return None
    
    def should_recrawl(self, url: str, recrawl_days: int = 3) -> bool:
        """Check if a URL should be recrawled."""
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
        """Record a page crawl with MongoDB sync."""
        # Use AEST timezone for daily stats
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
        
        # Sync to MongoDB
        perf_entry['site_id'] = self.site_id
        self.db.performance_history.insert_one(perf_entry)
        
        # Sync daily stats to MongoDB
        self.db.daily_stats.update_one(
            {"site_id": self.site_id, "date": today},
            {"$set": {"stats": self.daily_stats[today]}},
            upsert=True
        )
    
    def store_page_changes(self, url: str, change_details: Dict) -> None:
        """Store detailed page change information in MongoDB."""
        try:
            change_record = {
                "site_id": self.site_id,
                "url": url,
                "timestamp": datetime.now(),
                "change_details": change_details
            }
            
            # Store in dedicated page_changes collection
            self.db.page_changes.insert_one(change_record)
            
            # Also update URL state with last change info
            self.db.url_states.update_one(
                {"site_id": self.site_id, "url": url},
                {
                    "$set": {
                        "last_change": change_record,
                        "updated_at": datetime.now()
                    }
                }
            )
            
            print(f"📝 Stored change details for {url}")
        except Exception as e:
            print(f"Error storing page changes: {e}")

    def update_drive_folders(self, url: str, folder_ids: Dict[str, str]) -> None:
        """Store Google Drive folder information for a URL."""
        try:
            drive_data = {
                "main_folder_id": folder_ids.get('main_folder_id'),
                "html_folder_id": folder_ids.get('html_folder_id'), 
                "screenshot_folder_id": folder_ids.get('screenshot_folder_id'),
                "main_folder_url": f"https://drive.google.com/drive/folders/{folder_ids.get('main_folder_id')}",
                "html_folder_url": f"https://drive.google.com/drive/folders/{folder_ids.get('html_folder_id')}",
                "screenshot_folder_url": f"https://drive.google.com/drive/folders/{folder_ids.get('screenshot_folder_id')}"
            }
            
            self.db.url_states.update_one(
                {"site_id": self.site_id, "url": url},
                {"$set": {"drive_folders": drive_data}},
                upsert=False  # Only update existing documents
            )
        except Exception as e:
            print(f"Error storing Drive folder URLs: {e}")
    
    def get_progress_stats(self) -> Dict:
        """Get comprehensive progress statistics."""
        completed_pages = len(self.visited_urls)
        remaining_pages = len(self.remaining_urls)
        
        # Calculate progress percentage based on discovered pages
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
        
        # Get today's stats using AEST timezone
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
            'avg_crawl_time_seconds': round(average_processing_time, 1),
            'pages_per_hour': round(pages_per_hour, 0),
            'eta_datetime': eta_datetime,
            'cycle_number': self.current_cycle,
            'is_first_cycle': self.is_first_cycle,
            'cycle_duration_days': cycle_duration.days,
            'today_stats': today_stats,
            'total_discovered': total_known_pages
        }
    
    def complete_cycle(self) -> None:
        """Mark current cycle as complete."""
        self.current_cycle += 1
        self.is_first_cycle = False
        self.cycle_start_time = datetime.now()
        
        # Update total pages estimate based on actual discovery
        actual_total = len(self.visited_urls) + len(self.remaining_urls)
        if actual_total > self.total_pages_estimate:
            print(f"📊 Updating total pages estimate: {self.total_pages_estimate} → {actual_total}")
            self.total_pages_estimate = actual_total
        
        self.save_progress()
        print(f"🔄 Cycle {self.current_cycle - 1} completed. Starting cycle {self.current_cycle}")
    
    def update_total_pages_estimate(self, new_estimate: int) -> None:
        """Update the total pages estimate."""
        if new_estimate != self.total_pages_estimate:
            print(f"📊 Updating total pages estimate: {self.total_pages_estimate} → {new_estimate}")
            self.total_pages_estimate = new_estimate
            self.save_progress()
    
    def rescue_stuck_urls(self, stuck_minutes: int = 60) -> int:
        """
        Rescue URLs that have been stuck in 'in_progress' status for too long.
        
        Args:
            stuck_minutes: How many minutes before considering a URL stuck (default: 60)
            
        Returns:
            Number of URLs rescued
        """
        try:
            cutoff_time = datetime.now() - timedelta(minutes=stuck_minutes)
            
            # Update stuck URLs back to remaining (don't count in stats)
            result = self.db.url_states.update_many(
                {
                    "site_id": self.site_id,
                    "status": "in_progress",
                    "updated_at": {"$lt": cutoff_time}
                },
                {
                    "$set": {
                        "status": "remaining",
                        "updated_at": datetime.now(),
                    },
                    "$inc": {"rescue_count": 1}  # Track rescues separately
                }
            )
            
            rescued_count = result.modified_count
            
            if rescued_count > 0:
                print(f"🚑 Rescued {rescued_count} stuck URLs (stuck > {stuck_minutes} min)")
                # Reload memory from database to sync
                self._load_url_states()
            
            return rescued_count
            
        except Exception as e:
            print(f"❌ Error rescuing stuck URLs: {e}")
            return 0

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
    
    def _calculate_throughput_from_intervals(self) -> float:
        """
        Calculate pages per hour by counting pages crawled in last 60 minutes.
        
        Simple and accurate method that counts actual pages processed,
        naturally handling all delays, restarts, and interruptions.
        
        Returns:
            float: Pages processed per hour based on last hour's activity.
        """
        try:
            # Calculate 1 hour ago
            one_hour_ago = datetime.now() - timedelta(hours=1)
            
            # Count pages crawled in last hour from MongoDB
            pages_last_hour = self.db.performance_history.count_documents({
                "site_id": self.site_id,
                "timestamp": {"$gte": one_hour_ago}
            })
            
            return float(pages_last_hour)  # Already pages per hour!
            
        except Exception as e:
            print(f"Error calculating pages per hour: {e}")
            return 0.0 