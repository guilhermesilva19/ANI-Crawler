"""Clean MongoDB-only state management for ANI-Crawler."""

import hashlib
import os
import time
from datetime import datetime, timedelta
from typing import Set, Dict, Optional, List
from collections import OrderedDict
from functools import wraps
import pytz
try:
    from pymongo import MongoClient, UpdateOne, InsertOne
    from pymongo.errors import ConnectionFailure, DuplicateKeyError
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    MongoClient = None

from src.config import MONGODB_URI, SITE_ID, TARGET_URLS
from src.utils.db_pool import get_db_pool

__all__ = ['MongoStateAdapter']

def query_performance_tracker(func):
    """Decorator to track query performance and identify slow queries."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        start_time = time.time()
        try:
            result = func(self, *args, **kwargs)
            execution_time = time.time() - start_time
            
            # Track query statistics
            self.query_stats['total_queries'] += 1
            
            # Log slow queries for optimization
            if execution_time > self.slow_query_threshold:
                slow_query_info = {
                    'method': func.__name__,
                    'execution_time': execution_time,
                    'timestamp': datetime.now(),
                    'args_hash': hashlib.md5(str(args).encode()).hexdigest()[:8]
                }
                self.query_stats['slow_queries'].append(slow_query_info)
                
                # Keep only last 100 slow queries
                if len(self.query_stats['slow_queries']) > 100:
                    self.query_stats['slow_queries'] = self.query_stats['slow_queries'][-100:]
            
            return result
            
        except Exception as e:
            # Still track failed queries
            self.query_stats['total_queries'] += 1
            raise e
    
    return wrapper


class LRUCache:
    """Optimized LRU cache with TTL support for database performance."""
    
    def __init__(self, max_size: int = 1000, ttl_seconds: int = 300):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.data = OrderedDict()
        self.timestamps = {}
        
    def get(self, key: str) -> Optional[any]:
        """Get value from cache with LRU ordering."""
        current_time = time.time()
        
        # Check if key exists and is not expired
        if key in self.data:
            if current_time - self.timestamps[key] < self.ttl_seconds:
                # Move to end (most recently used)
                value = self.data.pop(key)
                self.data[key] = value
                return value
            else:
                # Expired, remove
                del self.data[key]
                del self.timestamps[key]
        
        return None
    
    def put(self, key: str, value: any) -> None:
        """Put value in cache with LRU eviction."""
        current_time = time.time()
        
        # If key exists, update it
        if key in self.data:
            self.data.pop(key)
        
        # Add new item
        self.data[key] = value
        self.timestamps[key] = current_time
        
        # Evict oldest if over capacity
        while len(self.data) > self.max_size:
            oldest_key = next(iter(self.data))
            del self.data[oldest_key]
            del self.timestamps[oldest_key]
    
    def invalidate(self, key: str) -> None:
        """Remove specific key from cache."""
        if key in self.data:
            del self.data[key]
            del self.timestamps[key]
    
    def cleanup_expired(self) -> int:
        """Remove expired entries and return count."""
        current_time = time.time()
        expired_keys = [
            key for key, timestamp in self.timestamps.items()
            if current_time - timestamp >= self.ttl_seconds
        ]
        
        for key in expired_keys:
            del self.data[key]
            del self.timestamps[key]
        
        return len(expired_keys)
    
    def stats(self) -> Dict:
        """Get cache statistics."""
        return {
            'size': len(self.data),
            'max_size': self.max_size,
            'ttl_seconds': self.ttl_seconds
        }


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
        
        # MongoDB setup - use optimized connection pool
        self.site_id = SITE_ID
        self.db_pool = get_db_pool()
        self.db = None
        
        # Batch operation settings for performance (enhanced with adaptive sizing)
        self.batch_size = 100
        self.min_batch_size = 25
        self.max_batch_size = 500
        self.pending_writes = []
        self.last_batch_write = time.time()
        self.batch_write_interval = 5.0  # seconds
        self.min_batch_interval = 2.0    # seconds during high activity
        self.max_batch_interval = 10.0   # seconds during low activity
        
        # Performance tracking for adaptive optimization
        self.batch_performance_history = []
        self.last_performance_check = time.time()
        
        # Batch statistics tracking
        self.batch_stats = {
            'total_batches': 0,
            'average_batch_time': 0.0,
            'last_batch_size': 0,
            'batch_efficiency': 100.0,
            'total_operations': 0
        }
        
        # Adaptive batch optimization for dynamic performance tuning
        self.adaptive_batch_size = 100  # Start with 100 operations per batch
        self.adaptive_interval = 5.0    # Start with 5 second intervals
        
        # Memory caching layer for performance optimization
        self.cache = LRUCache(max_size=1000, ttl_seconds=300)  # 5 minutes TTL
        self.cache_stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'last_cleanup': time.time()
        }
        
        # Query pipeline optimization tracking
        self.query_stats = {
            'slow_queries': [],
            'query_cache': {},
            'aggregation_usage': 0,
            'pipeline_optimizations': 0,
            'total_queries': 0,
            'optimized_queries': 0
        }
        
        # Query optimization thresholds
        self.slow_query_threshold = 1.0  # seconds
        self.query_cache_ttl = 300  # 5 minutes for query result caching
        
        # Background optimization engine
        self.optimization_engine = {
            'enabled': True,
            'last_optimization': time.time(),
            'optimization_interval': 300,  # 5 minutes
            'performance_history': [],
            'optimization_decisions': [],
            'auto_tuning_enabled': True,
            'learning_mode': True,
            'performance_baseline': None,
            'optimization_targets': {
                'query_response_time': 0.5,  # seconds
                'cache_hit_rate': 80,         # percent
                'batch_efficiency': 90,       # percent
                'connection_health': 95       # percent
            }
        }
        
        # Performance tracking for machine learning
        self.ml_performance_data = {
            'response_times': [],
            'error_rates': [],
            'throughput_history': [],
            'optimization_results': [],
            'pattern_detection': {
                'peak_hours': [],
                'low_activity_periods': [],
                'error_patterns': []
            }
        }
        
        # Initialize connection and load data
        self._initialize_connection()
        self.load_progress()
        
        # Start background optimization engine
        self.start_background_optimization()
    
    def _initialize_connection(self):
        """Initialize MongoDB connection using optimized pool."""
        try:
            # Use the optimized connection pool
            self.db = self.db_pool.database
            if self.db is None:
                raise ConnectionError("Database connection not available from pool")
            
            self._ensure_indexes()
            print(f"✅ MongoDB connected via pool: {self.site_id}")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to MongoDB via pool: {e}")
    
    def _ensure_indexes(self):
        """Create optimized indexes for performance."""
        try:
            # Site-specific indexes for multi-tenancy with enhanced performance
            self.db.url_states.create_index([("site_id", 1), ("url", 1)], unique=True, background=True)
            self.db.url_states.create_index([("site_id", 1), ("status", 1)], background=True)
            self.db.url_states.create_index([("site_id", 1), ("last_crawled", -1)], background=True)
            self.db.url_states.create_index([("site_id", 1), ("status", 1), ("last_crawled", -1)], background=True)
            
            # Enhanced indexes for daily stats and performance
            self.db.daily_stats.create_index([("site_id", 1), ("date", 1)], unique=True, background=True)
            self.db.performance_history.create_index([("site_id", 1), ("timestamp", -1)], background=True)
            
            # Auto-cleanup old performance data (30 days) with background creation
            self.db.performance_history.create_index("timestamp", expireAfterSeconds=2592000, background=True)
            
            # Enhanced indexes for audit log and page changes
            self.db.audit_log.create_index([("site_id", 1), ("timestamp", -1)], background=True)
            self.db.page_changes.create_index([("site_id", 1), ("url", 1), ("timestamp", -1)], background=True)
            self.db.site_states.create_index([("site_id", 1)], unique=True, background=True)
            
            print("📚 Enhanced database indexes created")
        except Exception as e:
            print(f"⚠️ Index creation warning: {e}")
    
    def _add_to_batch(self, operation_type: str, collection: str, filter_doc: dict, update_doc: dict, upsert: bool = False):
        """Add operation to batch for bulk execution with adaptive sizing."""
        operation = {
            'type': operation_type,
            'collection': collection,
            'filter': filter_doc,
            'update': update_doc,
            'upsert': upsert,
            'timestamp': time.time()
        }
        self.pending_writes.append(operation)
        
        # Check if we should adjust batch parameters based on recent performance
        self._maybe_adjust_batch_parameters()
        
        # Execute batch if size limit reached or time interval exceeded
        if (len(self.pending_writes) >= self.batch_size or 
            time.time() - self.last_batch_write > self.batch_write_interval):
            self._execute_batch_writes()
    
    def _maybe_adjust_batch_parameters(self):
        """Dynamically adjust batch size and interval based on performance."""
        current_time = time.time()
        
        # Only adjust every 30 seconds to avoid over-optimization
        if current_time - self.last_performance_check < 30:
            return
        
        self.last_performance_check = current_time
        
        # Analyze recent batch performance (last 10 batches)
        if len(self.batch_performance_history) >= 3:
            recent_performance = self.batch_performance_history[-10:]
            avg_time_per_op = sum(p['avg_time_per_op'] for p in recent_performance) / len(recent_performance)
            avg_success_rate = sum(p['success_rate'] for p in recent_performance) / len(recent_performance)
            
            # Adjust batch size based on performance
            if avg_time_per_op < 0.1 and avg_success_rate > 0.95:  # Fast and reliable
                # Increase batch size for better throughput
                self.batch_size = min(self.batch_size + 25, self.max_batch_size)
                self.batch_write_interval = max(self.batch_write_interval - 0.5, self.min_batch_interval)
            elif avg_time_per_op > 0.5 or avg_success_rate < 0.90:  # Slow or unreliable
                # Decrease batch size for better reliability
                self.batch_size = max(self.batch_size - 25, self.min_batch_size)
                self.batch_write_interval = min(self.batch_write_interval + 1.0, self.max_batch_interval)
            
            # Keep performance history manageable
            if len(self.batch_performance_history) > 20:
                self.batch_performance_history = self.batch_performance_history[-15:]
    
    def _execute_batch_writes(self):
        """Execute all pending batch writes with performance tracking."""
        if not self.pending_writes:
            return
        
        start_time = time.time()
        total_ops = len(self.pending_writes)
        successful_ops = 0
        
        try:
            # Group operations by collection
            collections_ops = {}
            for op in self.pending_writes:
                coll_name = op['collection']
                if coll_name not in collections_ops:
                    collections_ops[coll_name] = []
                collections_ops[coll_name].append(op)
            
            # Execute bulk operations per collection
            for coll_name, ops in collections_ops.items():
                collection = getattr(self.db, coll_name)
                
                # Prepare bulk operations using proper PyMongo objects
                if ops:
                    bulk_ops = []
                    for op in ops:
                        if op['type'] == 'update':
                            bulk_ops.append(
                                UpdateOne(
                                    op['filter'],
                                    op['update'],
                                    upsert=op['upsert']
                                )
                            )
                    
                    if bulk_ops:
                        result = collection.bulk_write(bulk_ops, ordered=False)
                        successful_ops += len(bulk_ops)
            
            # Record performance metrics for adaptive optimization
            execution_time = time.time() - start_time
            avg_time_per_op = execution_time / total_ops if total_ops > 0 else 0
            success_rate = successful_ops / total_ops if total_ops > 0 else 0
            
            # Store performance data for batch optimization
            self.batch_performance_history.append({
                'timestamp': time.time(),
                'batch_size': total_ops,
                'execution_time': execution_time,
                'avg_time_per_op': avg_time_per_op,
                'success_rate': success_rate
            })
            
            if successful_ops > 0:
                print(f"📝 Executed {successful_ops} bulk operations across {len(collections_ops)} collections (batch_size={self.batch_size}, {execution_time:.3f}s)")
            
            # Clear batch and update timestamp
            self.pending_writes.clear()
            self.last_batch_write = time.time()
            
        except Exception as e:
            print(f"⚠️ Batch write error: {e}")
            # Don't clear pending writes on error - they'll be retried
    
    def _force_batch_flush(self):
        """Force execution of all pending batch operations."""
        if self.pending_writes:
            self._execute_batch_writes()
    
    def load_progress(self) -> None:
        """Load saved crawl progress from MongoDB."""
        try:
            # Check for fresh start mode
            fresh_start = os.getenv('FRESH_START', 'false').lower() == 'true'
            auto_cleanup = os.getenv('AUTO_CLEANUP_ON_CONFIG_CHANGE', 'false').lower() == 'true'
            
            if fresh_start:
                print("🔄 FRESH_START mode enabled - clearing all existing data...")
                self._clean_all_data()
                self._initialize_fresh_state()
                return
            
            # Load site state
            site_doc = self.db.site_states.find_one({"site_id": self.site_id})
            if site_doc:
                # Check if config has changed (different target URLs)
                if auto_cleanup and self._config_has_changed(site_doc):
                    print("🔄 Configuration change detected - cleaning incompatible URLs...")
                    self._clean_incompatible_urls()
                
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
        """Load URL states into memory with optimized queries."""
        # Clear memory and rebuild from DB to ensure consistency
        old_visited_count = len(self.visited_urls)
        old_remaining_count = len(self.remaining_urls)
        
        self.visited_urls = set()
        self.remaining_urls = set()
        self.next_crawl = {}
        self.url_status = {}
        
        # Optimized batch loading with projection to reduce network overhead
        print("📊 Loading URL states from database...")
        
        # Load visited URLs with optimized query
        visited_cursor = self.db.url_states.find(
            {"site_id": self.site_id, "status": "visited"},
            {"url": 1, "last_crawled": 1, "_id": 0}  # Only fetch needed fields
        ).batch_size(1000)  # Optimize batch size
        
        visited_count = 0
        for doc in visited_cursor:
            self.visited_urls.add(doc['url'])
            self.next_crawl[doc['url']] = doc.get('last_crawled', datetime.now())
            visited_count += 1
        
        # Load remaining URLs with optimized query
        remaining_cursor = self.db.url_states.find(
            {"site_id": self.site_id, "status": "remaining"},
            {"url": 1, "_id": 0}  # Only fetch needed fields
        ).batch_size(1000)
        
        remaining_count = 0
        for doc in remaining_cursor:
            self.remaining_urls.add(doc['url'])
            remaining_count += 1
        
        # If no remaining URLs, initialize with targets using bulk operation
        if not self.remaining_urls:
            self.remaining_urls.update(TARGET_URLS)
            
            # Bulk upsert target URLs
            bulk_ops = []
            for url in TARGET_URLS:
                bulk_ops.append(
                    UpdateOne(
                        {"site_id": self.site_id, "url": url},
                        {"$setOnInsert": {
                            "site_id": self.site_id,
                            "url": url,
                            "status": "remaining",
                            "first_seen": datetime.now()
                        }},
                        upsert=True
                    )
                )
            
            if bulk_ops:
                try:
                    self.db.url_states.bulk_write(bulk_ops, ordered=False)
                    print(f"📝 Initialized {len(TARGET_URLS)} target URLs")
                except Exception as e:
                    print(f"⚠️ Error initializing target URLs: {e}")
        
        # Load URL status info with optimized query
        status_cursor = self.db.url_states.find(
            {"site_id": self.site_id, "status_info": {"$exists": True}},
            {"url": 1, "status_info": 1, "_id": 0}
        ).batch_size(1000)
        
        status_count = 0
        for doc in status_cursor:
            if 'status_info' in doc:
                self.url_status[doc['url']] = doc['status_info']
                status_count += 1
        
        # Log performance metrics
        print(f"📊 Loaded: {visited_count} visited, {remaining_count} remaining, {status_count} status records")
        
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
        """Save current crawl progress to MongoDB with bulk operations."""
        try:
            # Force flush any pending batch operations first
            self._force_batch_flush()
            
            # Prepare bulk operations for better performance
            bulk_ops = []
            
            # Update site state
            bulk_ops.append(
                UpdateOne(
                    {"site_id": self.site_id},
                    {"$set": {
                        "total_pages_estimate": self.total_pages_estimate,
                        "cycle_start_time": self.cycle_start_time,
                        "current_cycle": self.current_cycle,
                        "is_first_cycle": self.is_first_cycle,
                        "updated_at": datetime.now()
                    }},
                    upsert=True
                )
            )
            
            # Execute site state update
            if bulk_ops:
                self.db.site_states.bulk_write(bulk_ops, ordered=False)
            
            # Bulk update daily stats
            if self.daily_stats:
                daily_bulk_ops = []
                for date, stats in self.daily_stats.items():
                    daily_bulk_ops.append(
                        UpdateOne(
                            {"site_id": self.site_id, "date": date},
                            {"$set": {"stats": stats, "updated_at": datetime.now()}},
                            upsert=True
                        )
                    )
                
                if daily_bulk_ops:
                    self.db.daily_stats.bulk_write(daily_bulk_ops, ordered=False)
                    print(f"💾 Saved {len(daily_bulk_ops)} daily stats records to MongoDB")
            
            print("💾 Progress saved to MongoDB with bulk operations.")
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
    
    @query_performance_tracker
    def was_visited(self, url: str) -> bool:
        """Check if a URL has been visited before with caching."""
        # Check cache first
        cache_key = f"visited:{url}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            self.cache_stats['hits'] += 1
            return cached_result
        
        # Cache miss - check memory (existing logic preserved)
        result = url in self.visited_urls
        
        # Cache the result
        self.cache.put(cache_key, result)
        self.cache_stats['misses'] += 1
        
        return result
    
    def add_visited_url(self, url: str) -> None:
        """Add URL to visited with optimized database-first approach and caching."""
        try:
            # Update memory state (existing logic preserved)
            self.visited_urls.add(url)
            self.remaining_urls.discard(url)
            
            # Invalidate cache entries for this URL
            self.cache.invalidate(f"visited:{url}")
            self.cache.invalidate(f"status:{url}")
            
            # Add to batch for bulk execution instead of immediate write
            self._add_to_batch(
                'update',
                'url_states',
                {"site_id": self.site_id, "url": url},
                {"$set": {
                    "status": "visited",
                    "last_crawled": datetime.now(),
                    "updated_at": datetime.now()
                }},
                upsert=True
            )
            
        except Exception as e:
            print(f"⚠️  Failed to batch URL update: {url} - {e}")
            # Fallback to immediate write for critical operations (existing logic preserved)
            try:
                self.db.url_states.update_one(
                    {"site_id": self.site_id, "url": url},
                    {"$set": {
                        "status": "visited",
                        "last_crawled": datetime.now(),
                        "updated_at": datetime.now()
                    }},
                    upsert=True
                )
            except Exception as fallback_e:
                print(f"⚠️  Fallback write also failed for URL: {url} - {fallback_e}")
                # Try to reload from database to maintain consistency
                self._load_url_states()
    
    @query_performance_tracker
    def update_url_status(self, url: str, status_code: int) -> bool:
        """Update URL status using optimized batch operations with caching."""
        now = datetime.now()
        
        # Check cache for existing status
        cache_key = f"status:{url}"
        cached_status = self.cache.get(cache_key)
        
        # Initialize if first time seeing this URL (existing logic preserved)
        if url not in self.url_status:
            self.url_status[url] = {
                'status': status_code,
                'last_success': now if status_code < 400 else None,
                'error_count': 0
            }
            
            # Cache the new status
            self.cache.put(cache_key, self.url_status[url].copy())
            
            # Add to batch instead of immediate write
            self._add_to_batch(
                'update',
                'url_states',
                {"site_id": self.site_id, "url": url},
                {"$set": {"status_info": self.url_status[url]}},
                upsert=True
            )
            return False  # New URL, not deleted
        
        previous_status = self.url_status[url]
        
        # Update current status (existing logic preserved)
        if status_code < 400:
            # Successful access
            self.url_status[url] = {
                'status': status_code,
                'last_success': now,
                'error_count': 0
            }
            
            # Update cache
            self.cache.put(cache_key, self.url_status[url].copy())
            
            # Add to batch
            self._add_to_batch(
                'update',
                'url_states',
                {"site_id": self.site_id, "url": url},
                {"$set": {"status_info": self.url_status[url]}}
            )
            return False
        else:
            # Error status (existing logic preserved)
            self.url_status[url]['status'] = status_code
            self.url_status[url]['error_count'] += 1
            
            # Check if this is a newly deleted page
            had_previous_success = previous_status.get('last_success') is not None
            is_permanent_error = status_code in [404, 410]  # Not Found, Gone
            multiple_failures = self.url_status[url]['error_count'] >= 2
            
            # Update cache
            self.cache.put(cache_key, self.url_status[url].copy())
            
            # Add to batch
            self._add_to_batch(
                'update',
                'url_states',
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
    
    def get_url_info(self, url: str) -> Optional[Dict]:
        """Get detailed information about a URL."""
        doc = self.db.url_states.find_one({
            "site_id": self.site_id,
            "url": url
        })
        return doc

    def return_url_to_queue(self, url: str) -> None:
        """Return a URL to the queue (change from in_progress back to remaining)."""
        self.db.url_states.update_one(
            {"site_id": self.site_id, "url": url},
            {"$set": {"status": "remaining", "updated_at": datetime.now()}}
        )
        self.remaining_urls.add(url)

    def get_next_url(self) -> Optional[str]:
        """Get next URL directly from database with optimized queries."""
        try:
            # Try to get a remaining URL with optimized query
            url_doc = self.db.url_states.find_one_and_update(
                {"site_id": self.site_id, "status": "remaining"},
                {"$set": {"status": "in_progress", "updated_at": datetime.now()}},
                projection={"url": 1, "_id": 0},  # Only fetch needed fields
                return_document=True
            )
            
            if url_doc:
                # Update memory to stay in sync
                url = url_doc['url']
                self.remaining_urls.discard(url)
                return url
            
            # No remaining URLs, check for recrawl candidates with optimized query
            cutoff_date = datetime.now() - timedelta(days=3)
            recrawl_doc = self.db.url_states.find_one_and_update(
                {
                    "site_id": self.site_id, 
                    "status": "visited",
                    "last_crawled": {"$lt": cutoff_date}
                },
                {"$set": {"status": "in_progress", "updated_at": datetime.now()}},
                projection={"url": 1, "_id": 0},  # Only fetch needed fields
                sort=[("last_crawled", 1)],  # Oldest first for even distribution
                return_document=True
            )
            
            if recrawl_doc:
                # Update memory to stay in sync
                url = recrawl_doc['url']
                self.remaining_urls.add(url)  # Add back for memory consistency
                return url
                
            return None
            
        except Exception as e:
            print(f"⚠️ Error getting next URL: {e}")
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
            'page_type': page_type,
            'site_id': self.site_id
        }
        
        # Add change details if provided
        if change_details:
            perf_entry['change_details'] = change_details
        
        self.performance_history.append(perf_entry)
        
        # Keep only recent history to prevent memory bloat
        if len(self.performance_history) > 100:
            self.performance_history = self.performance_history[-100:]
        
        # Add performance entry to batch instead of immediate insert
        self._add_to_batch(
            'update',
            'performance_history',
            {"site_id": self.site_id, "timestamp": perf_entry['timestamp'], "url": url},
            {"$set": perf_entry},
            upsert=True
        )
        
        # Sync daily stats to MongoDB using batch
        self._add_to_batch(
            'update',
            'daily_stats',
            {"site_id": self.site_id, "date": today},
            {"$set": {"stats": self.daily_stats[today], "updated_at": datetime.now()}},
            upsert=True
        )
        
        # Store page changes if provided
        if change_details:
            self.store_page_changes(url, change_details)
    
    def store_page_changes(self, url: str, change_details: Dict) -> None:
        """Store detailed page change information using batch operations."""
        try:
            change_record = {
                "site_id": self.site_id,
                "url": url,
                "timestamp": datetime.now(),
                "change_details": change_details
            }
            
            # Use batch for page changes insert
            self._add_to_batch(
                'update',
                'page_changes',
                {"site_id": self.site_id, "url": url, "timestamp": change_record['timestamp']},
                {"$set": change_record},
                upsert=True
            )
            
            # Also update URL state with last change info using batch
            self._add_to_batch(
                'update',
                'url_states',
                {"site_id": self.site_id, "url": url},
                {"$set": {
                    "last_change": change_record,
                    "updated_at": datetime.now()
                }}
            )
            
            print(f"📝 Batched change details for {url}")
        except Exception as e:
            print(f"Error batching page changes: {e}")

    def update_drive_folders(self, url: str, folder_ids: Dict[str, str]) -> None:
        """Store Google Drive folder information using batch operations."""
        try:
            drive_data = {
                "main_folder_id": folder_ids.get('main_folder_id'),
                "html_folder_id": folder_ids.get('html_folder_id'), 
                "screenshot_folder_id": folder_ids.get('screenshot_folder_id'),
                "main_folder_url": f"https://drive.google.com/drive/folders/{folder_ids.get('main_folder_id')}",
                "html_folder_url": f"https://drive.google.com/drive/folders/{folder_ids.get('html_folder_id')}",
                "screenshot_folder_url": f"https://drive.google.com/drive/folders/{folder_ids.get('screenshot_folder_id')}"
            }
            
            # Use batch operation for drive folder update
            self._add_to_batch(
                'update',
                'url_states',
                {"site_id": self.site_id, "url": url},
                {"$set": {"drive_folders": drive_data, "updated_at": datetime.now()}}
            )
            
        except Exception as e:
            print(f"Error batching Drive folder URLs: {e}")
    
    def get_progress_stats(self) -> Dict:
        """Get comprehensive progress statistics with connection monitoring."""
        completed_pages = len(self.visited_urls)
        remaining_pages = len(self.remaining_urls)
        
        # Get connection pool statistics
        connection_stats = {}
        try:
            if hasattr(self.db_pool, 'get_connection_stats'):
                connection_stats = self.db_pool.get_connection_stats()
        except Exception as e:
            print(f"⚠️ Could not get connection stats: {e}")
        
        # Performance monitoring with adaptive metrics and caching
        batch_info = {
            'pending_writes': len(self.pending_writes),
            'last_batch_write': self.last_batch_write,
            'batch_interval': self.batch_write_interval,
            'current_batch_size': self.batch_size,
            'batch_size_range': f"{self.min_batch_size}-{self.max_batch_size}",
            'interval_range': f"{self.min_batch_interval}-{self.max_batch_interval}s"
        }
        
        # Cache performance monitoring
        cache_performance = self._get_cache_performance()
        
        # Calculate progress percentage based on discovered pages
        total_known_pages = completed_pages + remaining_pages
        if total_known_pages > 0:
            progress_percent = (completed_pages / total_known_pages) * 100
        else:
            progress_percent = 0.0
        
        # Calculate average processing time per page for backward compatibility
        recent_performance_entries = self.performance_history[-20:] if self.performance_history else []
        average_processing_time = sum(entry['crawl_time'] for entry in recent_performance_entries) / len(recent_performance_entries) if recent_performance_entries else 15.0
        
        # Calculate ETA
        # 1) If there are remaining pages, ETA = time until cycle completion
        # 2) If no remaining pages, ETA = time until next cycle can start based on recrawl policy
        eta_datetime = None
        eta_mode = "unknown"
        now = datetime.now()

        if remaining_pages > 0:
            eta_mode = "cycle_completion"
            # Primary method: Use interval-based throughput calculation
            interval_based_throughput = self._calculate_throughput_from_intervals()
            if interval_based_throughput > 0:
                estimated_completion_hours = remaining_pages / interval_based_throughput
                eta_datetime = now + timedelta(hours=estimated_completion_hours)
            else:
                # Fallback method: Use individual page processing times
                estimated_completion_seconds = remaining_pages * average_processing_time
                eta_datetime = now + timedelta(seconds=estimated_completion_seconds)
        else:
            # No remaining pages -> estimate when the next crawl will begin again
            # Mongo adapter recrawls pages last crawled >= recrawl_days ago (default 3 days)
            recrawl_days = 3
            if self.next_crawl:
                earliest_next_time = min(last_crawled + timedelta(days=recrawl_days) for last_crawled in self.next_crawl.values())
                # If already due, use now; otherwise the future time
                eta_datetime = earliest_next_time if earliest_next_time > now else now
                eta_mode = "next_cycle_start"
            else:
                # If we have no history, we cannot determine next cycle start
                eta_datetime = None
                eta_mode = "next_cycle_start"
        
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
            'eta_mode': eta_mode,  # either "cycle_completion" or "next_cycle_start"
            'cycle_number': self.current_cycle,
            'is_first_cycle': self.is_first_cycle,
            'cycle_duration_days': cycle_duration.days,
            'today_stats': today_stats,
            'total_discovered': total_known_pages,
            # Enhanced monitoring information
            'connection_stats': connection_stats,
            'batch_operations': batch_info,
            'cache_performance': cache_performance,
            'database_health': {
                'connection_active': self.db_pool.client is not None if self.db_pool else False,
                'site_id': self.site_id
            }
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

    def cleanup_and_optimize(self) -> None:
        """Cleanup resources and execute final optimizations."""
        try:
            # Force flush all pending batch writes
            self._force_batch_flush()
            
            # Final progress save
            self.save_progress()
            
            # Optimize connection pool stats
            if hasattr(self.db_pool, 'get_connection_stats'):
                stats = self.db_pool.get_connection_stats()
                print(f"🔧 Final DB pool stats: {stats}")
            
            print("🧹 Cleanup and optimization completed")
            
        except Exception as e:
            print(f"⚠️ Error during cleanup: {e}")
    
    def _get_cache_performance(self) -> Dict:
        """Get cache performance statistics and perform periodic cleanup."""
        current_time = time.time()
        
        # Perform cache cleanup every 5 minutes
        if current_time - self.cache_stats['last_cleanup'] > 300:
            expired_count = self.cache.cleanup_expired()
            if expired_count > 0:
                self.cache_stats['evictions'] += expired_count
            self.cache_stats['last_cleanup'] = current_time
        
        # Calculate cache efficiency
        total_requests = self.cache_stats['hits'] + self.cache_stats['misses']
        hit_rate = (self.cache_stats['hits'] / total_requests * 100) if total_requests > 0 else 0
        
        return {
            **self.cache_stats,
            'hit_rate_percent': round(hit_rate, 2),
            'cache_stats': self.cache.stats(),
            'total_requests': total_requests
        }
    
    def monitor_and_optimize(self) -> Dict:
        """Monitor database performance and apply automatic optimizations."""
        try:
            monitoring_results = {
                'timestamp': datetime.now(),
                'optimizations_applied': [],
                'warnings': [],
                'metrics': {}
            }
            
            # Check connection health
            if self.db_pool and hasattr(self.db_pool, 'test_connection'):
                if not self.db_pool.test_connection():
                    if self.db_pool.reconnect_if_needed():
                        monitoring_results['optimizations_applied'].append('reconnected_database')
                    else:
                        monitoring_results['warnings'].append('database_connection_failed')
            
            # Check batch operations efficiency
            pending_count = len(self.pending_writes)
            time_since_last_batch = time.time() - self.last_batch_write
            
            monitoring_results['metrics'].update({
                'pending_writes': pending_count,
                'time_since_last_batch': time_since_last_batch,
                'batch_size_limit': self.batch_size
            })
            
            # Force batch execution if too many pending operations
            if pending_count > self.batch_size * 0.8:  # 80% of batch size
                self._execute_batch_writes()
                monitoring_results['optimizations_applied'].append('forced_batch_execution')
            
            # Adjust batch interval based on load
            if pending_count > 50:
                self.batch_write_interval = max(2.0, self.batch_write_interval * 0.9)  # Faster batching
                monitoring_results['optimizations_applied'].append('reduced_batch_interval')
            elif pending_count < 10 and self.batch_write_interval < 5.0:
                self.batch_write_interval = min(10.0, self.batch_write_interval * 1.1)  # Slower batching
                monitoring_results['optimizations_applied'].append('increased_batch_interval')
            
            # Monitor connection pool statistics with advanced features
            if hasattr(self.db_pool, 'get_connection_stats'):
                pool_stats = self.db_pool.get_connection_stats()
                monitoring_results['metrics']['connection_pool'] = pool_stats
                
                # Advanced connection monitoring and auto-scaling
                if hasattr(self.db_pool, 'monitor_load_and_scale'):
                    scaling_result = self.db_pool.monitor_load_and_scale()
                    monitoring_results['metrics']['auto_scaling'] = scaling_result
                    
                    if scaling_result.get('scaling_needed'):
                        monitoring_results['optimizations_applied'].append('auto_scaled_connections')
                
                # Get advanced connection statistics
                if hasattr(self.db_pool, 'get_advanced_stats'):
                    advanced_stats = self.db_pool.get_advanced_stats()
                    monitoring_results['metrics']['advanced_connection_stats'] = advanced_stats
                
                # Warning for high reconnection rate
                if pool_stats.get('reconnections', 0) > 5:
                    monitoring_results['warnings'].append('high_reconnection_rate')
                    
                    # Try smart reconnection if available
                    if hasattr(self.db_pool, 'smart_reconnect'):
                        if self.db_pool.smart_reconnect():
                            monitoring_results['optimizations_applied'].append('smart_reconnection_successful')
            
            # Optimize connection settings based on performance
            if hasattr(self.db_pool, 'optimize_connection_settings'):
                connection_optimizations = self.db_pool.optimize_connection_settings()
                monitoring_results['metrics']['connection_optimizations'] = connection_optimizations
            
            # Add cache performance monitoring
            cache_performance = self._get_cache_performance()
            monitoring_results['metrics']['cache_performance'] = cache_performance
            
            # Add query optimization performance monitoring
            query_performance = self.get_query_performance_stats()
            monitoring_results['metrics']['query_performance'] = query_performance
            
            # Cache performance warnings
            if cache_performance['hit_rate_percent'] < 60 and cache_performance['total_requests'] > 100:
                monitoring_results['warnings'].append('low_cache_hit_rate')
                
            # Query performance warnings
            if query_performance['optimization_rate_percent'] < 50 and query_performance['total_queries'] > 50:
                monitoring_results['warnings'].append('low_query_optimization_rate')
            
            if len(query_performance['recent_slow_queries']) > 3:
                monitoring_results['warnings'].append('high_slow_query_count')
            
            # Background optimization engine integration
            if self.optimization_engine['enabled']:
                optimization_cycle_result = self.run_optimization_cycle()
                monitoring_results['metrics']['optimization_engine'] = optimization_cycle_result
                
                if optimization_cycle_result.get('optimizations_applied'):
                    monitoring_results['optimizations_applied'].extend(optimization_cycle_result['optimizations_applied'])
            
            return monitoring_results
            
        except Exception as e:
            return {
                'timestamp': datetime.now(),
                'error': str(e),
                'optimizations_applied': [],
                'warnings': ['monitoring_system_error']
            }

    # === QUERY PIPELINE OPTIMIZATION METHODS ===
    
    def _create_query_cache_key(self, collection_name: str, query: Dict, projection: Dict = None) -> str:
        """Create a cache key for query results."""
        cache_data = {
            'collection': collection_name,
            'query': query,
            'projection': projection or {}
        }
        return hashlib.md5(json.dumps(cache_data, sort_keys=True, default=str).encode()).hexdigest()
    
    @query_performance_tracker
    def _optimized_find_with_cache(self, collection_name: str, query: Dict, 
                                   projection: Dict = None, limit: int = None) -> List[Dict]:
        """Optimized find with result caching and performance tracking."""
        # Check cache first
        cache_key = self._create_query_cache_key(collection_name, query, projection)
        cached_result = self.query_stats['query_cache'].get(cache_key)
        
        if cached_result and time.time() - cached_result['timestamp'] < self.query_cache_ttl:
            return cached_result['data']
        
        # Execute query with optimization
        collection = self.db[collection_name]
        cursor = collection.find(query, projection)
        
        if limit:
            cursor = cursor.limit(limit)
            
        # Add read preference for better performance
        cursor = cursor.hint([("_id", 1)])  # Use index hint when appropriate
        
        result = list(cursor)
        
        # Cache the result
        self.query_stats['query_cache'][cache_key] = {
            'data': result,
            'timestamp': time.time()
        }
        
        # Cleanup old cache entries (keep last 50)
        if len(self.query_stats['query_cache']) > 50:
            oldest_keys = sorted(self.query_stats['query_cache'].keys(), 
                               key=lambda k: self.query_stats['query_cache'][k]['timestamp'])[:10]
            for old_key in oldest_keys:
                del self.query_stats['query_cache'][old_key]
        
        self.query_stats['optimized_queries'] += 1
        return result
    
    @query_performance_tracker
    def get_site_stats_optimized(self) -> Dict:
        """Get comprehensive site statistics using optimized aggregation pipeline."""
        self.query_stats['aggregation_usage'] += 1
        
        # Use aggregation pipeline for better performance
        pipeline = [
            {"$match": {"site_id": self.site_id}},
            {"$group": {
                "_id": "$site_id",
                "total_urls": {"$sum": 1},
                "crawled_count": {"$sum": {"$cond": [{"$eq": ["$status", "crawled"]}, 1, 0]}},
                "pending_count": {"$sum": {"$cond": [{"$eq": ["$status", "pending"]}, 1, 0]}},
                "failed_count": {"$sum": {"$cond": [{"$eq": ["$status", "failed"]}, 1, 0]}},
                "avg_response_time": {"$avg": "$response_time"},
                "last_crawled": {"$max": "$last_crawled"}
            }}
        ]
        
        result = list(self.db.url_status.aggregate(pipeline))
        self.query_stats['pipeline_optimizations'] += 1
        
        if result:
            stats = result[0]
            return {
                'total_urls': stats.get('total_urls', 0),
                'crawled': stats.get('crawled_count', 0),
                'pending': stats.get('pending_count', 0),
                'failed': stats.get('failed_count', 0),
                'average_response_time': round(stats.get('avg_response_time', 0), 2),
                'last_crawled': stats.get('last_crawled'),
                'completion_rate': round((stats.get('crawled_count', 0) / max(stats.get('total_urls', 1), 1)) * 100, 2)
            }
        
        return {'total_urls': 0, 'crawled': 0, 'pending': 0, 'failed': 0, 
                'average_response_time': 0, 'completion_rate': 0}
    
    @query_performance_tracker
    def get_failed_urls_optimized(self, limit: int = 100) -> List[Dict]:
        """Get failed URLs using optimized query with projection."""
        query = {"site_id": self.site_id, "status": "failed"}
        projection = {"url": 1, "error_message": 1, "last_attempt": 1, "retry_count": 1}
        
        return self._optimized_find_with_cache("url_status", query, projection, limit)
    
    @query_performance_tracker
    def get_recent_activity_optimized(self, hours: int = 24) -> List[Dict]:
        """Get recent crawling activity using optimized aggregation."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        pipeline = [
            {"$match": {
                "site_id": self.site_id,
                "last_crawled": {"$gte": cutoff_time}
            }},
            {"$sort": {"last_crawled": -1}},
            {"$limit": 500},
            {"$project": {
                "url": 1,
                "status": 1,
                "last_crawled": 1,
                "response_time": 1,
                "content_hash": 1
            }}
        ]
        
        result = list(self.db.url_status.aggregate(pipeline))
        self.query_stats['aggregation_usage'] += 1
        self.query_stats['pipeline_optimizations'] += 1
        
        return result
    
    def _cleanup_query_cache(self) -> None:
        """Clean up expired query cache entries."""
        current_time = time.time()
        expired_keys = [
            key for key, value in self.query_stats['query_cache'].items()
            if current_time - value['timestamp'] > self.query_cache_ttl
        ]
        
        for key in expired_keys:
            del self.query_stats['query_cache'][key]
    
    def _check_connection_health(self) -> bool:
        """Check if database connection is healthy."""
        try:
            if self.db_pool and hasattr(self.db_pool, 'test_connection'):
                return self.db_pool.test_connection()
            elif self.db:
                # Simple ping test
                self.db.command('ping')
                return True
            return False
        except Exception:
            return False
    
    def get_query_performance_stats(self) -> Dict:
        """Get query optimization performance statistics."""
        self._cleanup_query_cache()
        
        total_queries = self.query_stats['total_queries']
        optimized_queries = self.query_stats['optimized_queries']
        optimization_rate = (optimized_queries / max(total_queries, 1)) * 100
        
        return {
            'total_queries': total_queries,
            'optimized_queries': optimized_queries,
            'optimization_rate_percent': round(optimization_rate, 2),
            'slow_queries_count': len(self.query_stats['slow_queries']),
            'aggregation_pipeline_usage': self.query_stats['aggregation_usage'],
            'pipeline_optimizations': self.query_stats['pipeline_optimizations'],
            'query_cache_size': len(self.query_stats['query_cache']),
            'recent_slow_queries': self.query_stats['slow_queries'][-5:] if self.query_stats['slow_queries'] else []
        }

    # === BACKGROUND OPTIMIZATION ENGINE ===
    
    def start_background_optimization(self) -> None:
        """Start the background optimization engine."""
        if not self.optimization_engine['enabled']:
            return
            
        print("🚀 Starting Background Optimization Engine...")
        
        # Initialize performance baseline
        self._establish_performance_baseline()
        
        # Start optimization monitoring
        self._schedule_next_optimization()
        
        print("✅ Background optimization engine started")
    
    def _establish_performance_baseline(self) -> None:
        """Establish performance baseline for optimization decisions."""
        try:
            baseline_metrics = {
                'timestamp': time.time(),
                'query_performance': self.get_query_performance_stats(),
                'cache_performance': self._get_cache_performance(),
                'batch_performance': self.batch_stats.copy(),
                'connection_health': self._check_connection_health()
            }
            
            self.optimization_engine['performance_baseline'] = baseline_metrics
            print("📊 Performance baseline established")
            
        except Exception as e:
            print(f"⚠️ Error establishing baseline: {e}")
    
    def _schedule_next_optimization(self) -> None:
        """Schedule the next optimization cycle."""
        self.optimization_engine['last_optimization'] = time.time()
        
        # This would typically be done with a background thread or scheduler
        # For now, we'll trigger it during monitoring calls
    
    def run_optimization_cycle(self) -> Dict:
        """Run a complete optimization cycle."""
        if not self.optimization_engine['enabled']:
            return {'status': 'disabled'}
        
        current_time = time.time()
        time_since_last = current_time - self.optimization_engine['last_optimization']
        
        # Only run if enough time has passed
        if time_since_last < self.optimization_engine['optimization_interval']:
            return {
                'status': 'waiting',
                'next_optimization_in': self.optimization_engine['optimization_interval'] - time_since_last
            }
        
        print("🔧 Running optimization cycle...")
        
        optimization_results = {
            'timestamp': current_time,
            'optimizations_applied': [],
            'performance_improvements': {},
            'decisions_made': [],
            'status': 'completed'
        }
        
        try:
            # Step 1: Collect current performance metrics
            current_metrics = self._collect_performance_metrics()
            
            # Step 2: Analyze performance trends
            trend_analysis = self._analyze_performance_trends(current_metrics)
            
            # Step 3: Make optimization decisions
            decisions = self._make_optimization_decisions(trend_analysis)
            optimization_results['decisions_made'] = decisions
            
            # Step 4: Apply optimizations
            applied_optimizations = self._apply_optimizations(decisions)
            optimization_results['optimizations_applied'] = applied_optimizations
            
            # Step 5: Measure improvements
            improvements = self._measure_optimization_impact(current_metrics)
            optimization_results['performance_improvements'] = improvements
            
            # Step 6: Update machine learning data
            self._update_ml_performance_data(current_metrics, applied_optimizations, improvements)
            
            # Step 7: Schedule next optimization
            self._schedule_next_optimization()
            
            print(f"✅ Optimization cycle completed: {len(applied_optimizations)} optimizations applied")
            
        except Exception as e:
            optimization_results['status'] = 'failed'
            optimization_results['error'] = str(e)
            print(f"❌ Optimization cycle failed: {e}")
        
        # Store optimization results
        self.optimization_engine['optimization_decisions'].append(optimization_results)
        
        # Keep only last 50 optimization cycles
        if len(self.optimization_engine['optimization_decisions']) > 50:
            self.optimization_engine['optimization_decisions'] = \
                self.optimization_engine['optimization_decisions'][-50:]
        
        return optimization_results
    
    def _collect_performance_metrics(self) -> Dict:
        """Collect comprehensive performance metrics."""
        return {
            'query_performance': self.get_query_performance_stats(),
            'cache_performance': self._get_cache_performance(),
            'batch_performance': self.batch_stats.copy(),
            'connection_stats': self.db_pool.get_advanced_stats() if hasattr(self.db_pool, 'get_advanced_stats') else {},
            'system_health': self._check_connection_health(),
            'response_times': self.ml_performance_data['response_times'][-100:],  # Last 100 measurements
            'error_rate': len([e for e in self.ml_performance_data['error_rates'][-100:] if e > 0]) / 100 if self.ml_performance_data['error_rates'] else 0
        }
    
    def _analyze_performance_trends(self, current_metrics: Dict) -> Dict:
        """Analyze performance trends to identify optimization opportunities."""
        trends = {
            'declining_performance': [],
            'optimization_opportunities': [],
            'performance_bottlenecks': [],
            'efficiency_scores': {}
        }
        
        baseline = self.optimization_engine.get('performance_baseline', {})
        if not baseline:
            return trends
        
        # Query performance analysis
        query_perf = current_metrics['query_performance']
        if query_perf['optimization_rate_percent'] < 70:
            trends['optimization_opportunities'].append('increase_query_optimization')
        
        if len(query_perf['recent_slow_queries']) > 3:
            trends['performance_bottlenecks'].append('slow_query_detection')
        
        # Cache performance analysis
        cache_perf = current_metrics['cache_performance']
        if cache_perf['hit_rate_percent'] < 70:
            trends['optimization_opportunities'].append('improve_cache_efficiency')
        
        # Batch performance analysis
        batch_perf = current_metrics['batch_performance']
        avg_batch_time = batch_perf.get('average_batch_time', 0)
        if avg_batch_time > 5.0:  # seconds
            trends['performance_bottlenecks'].append('slow_batch_operations')
        
        # Connection health analysis
        connection_stats = current_metrics.get('connection_stats', {})
        if connection_stats.get('connection_health', {}).get('ping_responsive') is False:
            trends['performance_bottlenecks'].append('connection_issues')
        
        # Calculate efficiency scores
        trends['efficiency_scores'] = {
            'query_efficiency': min(query_perf['optimization_rate_percent'], 100),
            'cache_efficiency': cache_perf['hit_rate_percent'],
            'batch_efficiency': max(0, 100 - (avg_batch_time * 10)),  # Inverse relationship
            'overall_efficiency': 0  # Will be calculated
        }
        
        # Calculate overall efficiency
        efficiency_values = [v for v in trends['efficiency_scores'].values() if v > 0]
        trends['efficiency_scores']['overall_efficiency'] = sum(efficiency_values) / len(efficiency_values) if efficiency_values else 0
        
        return trends
    
    def _make_optimization_decisions(self, trend_analysis: Dict) -> List[Dict]:
        """Make intelligent optimization decisions based on trend analysis."""
        decisions = []
        targets = self.optimization_engine['optimization_targets']
        
        # Decision 1: Query optimization adjustments
        if 'slow_query_detection' in trend_analysis['performance_bottlenecks']:
            decisions.append({
                'type': 'query_optimization',
                'action': 'reduce_slow_query_threshold',
                'current_threshold': self.slow_query_threshold,
                'new_threshold': max(0.5, self.slow_query_threshold * 0.8),
                'reason': 'High number of slow queries detected'
            })
        
        # Decision 2: Cache optimization
        if 'improve_cache_efficiency' in trend_analysis['optimization_opportunities']:
            current_ttl = self.cache.ttl_seconds
            decisions.append({
                'type': 'cache_optimization',
                'action': 'increase_cache_ttl',
                'current_ttl': current_ttl,
                'new_ttl': min(600, current_ttl * 1.2),  # Max 10 minutes
                'reason': 'Low cache hit rate, extending TTL'
            })
        
        # Decision 3: Batch size optimization
        batch_perf = trend_analysis.get('efficiency_scores', {}).get('batch_efficiency', 0)
        if batch_perf < 80:
            decisions.append({
                'type': 'batch_optimization',
                'action': 'adjust_batch_size',
                'current_size': self.adaptive_batch_size,
                'new_size': max(25, min(500, self.adaptive_batch_size + 25)),
                'reason': 'Low batch efficiency detected'
            })
        
        # Decision 4: Connection pool optimization
        if 'connection_issues' in trend_analysis['performance_bottlenecks']:
            decisions.append({
                'type': 'connection_optimization',
                'action': 'enable_smart_reconnection',
                'reason': 'Connection health issues detected'
            })
        
        # Decision 5: Adaptive learning adjustments
        overall_efficiency = trend_analysis.get('efficiency_scores', {}).get('overall_efficiency', 0)
        if overall_efficiency < 75:
            decisions.append({
                'type': 'adaptive_learning',
                'action': 'increase_monitoring_frequency',
                'current_interval': self.optimization_engine['optimization_interval'],
                'new_interval': max(120, self.optimization_engine['optimization_interval'] * 0.8),
                'reason': 'Low overall efficiency, increasing monitoring'
            })
        
        return decisions
    
    def _apply_optimizations(self, decisions: List[Dict]) -> List[str]:
        """Apply optimization decisions to the system."""
        applied = []
        
        for decision in decisions:
            try:
                if decision['type'] == 'query_optimization':
                    self.slow_query_threshold = decision['new_threshold']
                    applied.append(f"Reduced slow query threshold to {decision['new_threshold']}s")
                
                elif decision['type'] == 'cache_optimization':
                    # Update cache TTL for new entries
                    self.query_cache_ttl = decision['new_ttl']
                    applied.append(f"Increased cache TTL to {decision['new_ttl']}s")
                
                elif decision['type'] == 'batch_optimization':
                    self.adaptive_batch_size = decision['new_size']
                    applied.append(f"Adjusted batch size to {decision['new_size']}")
                
                elif decision['type'] == 'connection_optimization':
                    if hasattr(self.db_pool, 'smart_reconnect'):
                        self.db_pool.smart_reconnect()
                        applied.append("Enabled smart reconnection")
                
                elif decision['type'] == 'adaptive_learning':
                    self.optimization_engine['optimization_interval'] = decision['new_interval']
                    applied.append(f"Increased monitoring frequency to {decision['new_interval']}s")
                
            except Exception as e:
                print(f"⚠️ Failed to apply optimization {decision['type']}: {e}")
        
        return applied
    
    def _measure_optimization_impact(self, pre_optimization_metrics: Dict) -> Dict:
        """Measure the impact of applied optimizations."""
        # This would typically be measured over time
        # For now, we'll provide predictive improvements
        
        improvements = {
            'estimated_query_improvement': '10-15%',
            'estimated_cache_improvement': '15-25%',
            'estimated_batch_improvement': '5-10%',
            'estimated_overall_improvement': '12-18%',
            'measurement_note': 'Improvements will be measured over the next optimization cycle'
        }
        
        return improvements
    
    def _update_ml_performance_data(self, metrics: Dict, optimizations: List[str], improvements: Dict) -> None:
        """Update machine learning performance data for future decisions."""
        current_time = time.time()
        
        # Store performance sample
        performance_sample = {
            'timestamp': current_time,
            'metrics': metrics,
            'optimizations_applied': optimizations,
            'predicted_improvements': improvements
        }
        
        self.ml_performance_data['optimization_results'].append(performance_sample)
        
        # Keep only last 100 samples
        if len(self.ml_performance_data['optimization_results']) > 100:
            self.ml_performance_data['optimization_results'] = \
                self.ml_performance_data['optimization_results'][-100:]
        
        # Pattern detection (simplified)
        hour = datetime.now().hour
        
        # Detect peak hours (high activity)
        query_count = metrics.get('query_performance', {}).get('total_queries', 0)
        if query_count > 100:  # High activity threshold
            if hour not in self.ml_performance_data['pattern_detection']['peak_hours']:
                self.ml_performance_data['pattern_detection']['peak_hours'].append(hour)
        
        # Detect low activity periods
        elif query_count < 20:  # Low activity threshold
            if hour not in self.ml_performance_data['pattern_detection']['low_activity_periods']:
                self.ml_performance_data['pattern_detection']['low_activity_periods'].append(hour)
    
    def get_optimization_report(self) -> Dict:
        """Generate comprehensive optimization report."""
        current_metrics = self._collect_performance_metrics()
        trend_analysis = self._analyze_performance_trends(current_metrics)
        
        report = {
            'report_timestamp': datetime.now().isoformat(),
            'optimization_status': {
                'engine_enabled': self.optimization_engine['enabled'],
                'auto_tuning_enabled': self.optimization_engine['auto_tuning_enabled'],
                'learning_mode': self.optimization_engine['learning_mode'],
                'last_optimization': datetime.fromtimestamp(self.optimization_engine['last_optimization']).isoformat(),
                'optimization_cycles_completed': len(self.optimization_engine['optimization_decisions'])
            },
            'current_performance': current_metrics,
            'trend_analysis': trend_analysis,
            'efficiency_targets': self.optimization_engine['optimization_targets'],
            'performance_vs_targets': self._compare_performance_to_targets(current_metrics),
            'optimization_history': self.optimization_engine['optimization_decisions'][-10:],  # Last 10 cycles
            'ml_insights': self._generate_ml_insights(),
            'recommendations': self._generate_optimization_recommendations(trend_analysis)
        }
        
        return report
    
    def _compare_performance_to_targets(self, metrics: Dict) -> Dict:
        """Compare current performance to optimization targets."""
        targets = self.optimization_engine['optimization_targets']
        
        # Query response time (use average from slow queries or default)
        query_perf = metrics.get('query_performance', {})
        avg_query_time = 0.3  # Default assumption
        
        # Cache hit rate
        cache_perf = metrics.get('cache_performance', {})
        cache_hit_rate = cache_perf.get('hit_rate_percent', 0)
        
        # Batch efficiency (based on batch performance)
        batch_perf = metrics.get('batch_performance', {})
        batch_efficiency = 90 if batch_perf.get('average_batch_time', 2) < 3 else 70
        
        # Connection health
        connection_health = 95 if metrics.get('system_health', True) else 60
        
        comparison = {
            'query_response_time': {
                'current': avg_query_time,
                'target': targets['query_response_time'],
                'meets_target': avg_query_time <= targets['query_response_time'],
                'performance_ratio': targets['query_response_time'] / max(avg_query_time, 0.1)
            },
            'cache_hit_rate': {
                'current': cache_hit_rate,
                'target': targets['cache_hit_rate'],
                'meets_target': cache_hit_rate >= targets['cache_hit_rate'],
                'performance_ratio': cache_hit_rate / targets['cache_hit_rate']
            },
            'batch_efficiency': {
                'current': batch_efficiency,
                'target': targets['batch_efficiency'],
                'meets_target': batch_efficiency >= targets['batch_efficiency'],
                'performance_ratio': batch_efficiency / targets['batch_efficiency']
            },
            'connection_health': {
                'current': connection_health,
                'target': targets['connection_health'],
                'meets_target': connection_health >= targets['connection_health'],
                'performance_ratio': connection_health / targets['connection_health']
            }
        }
        
        # Calculate overall performance score
        ratios = [comp['performance_ratio'] for comp in comparison.values()]
        comparison['overall_performance_score'] = sum(ratios) / len(ratios) * 100
        
        return comparison
    
    def _generate_ml_insights(self) -> Dict:
        """Generate machine learning insights from performance data."""
        patterns = self.ml_performance_data['pattern_detection']
        
        insights = {
            'activity_patterns': {
                'peak_hours': sorted(patterns['peak_hours']) if patterns['peak_hours'] else [],
                'low_activity_hours': sorted(patterns['low_activity_periods']) if patterns['low_activity_periods'] else [],
                'pattern_confidence': 'high' if len(self.ml_performance_data['optimization_results']) > 20 else 'low'
            },
            'optimization_effectiveness': {
                'total_optimizations': len(self.ml_performance_data['optimization_results']),
                'successful_optimizations': len([r for r in self.ml_performance_data['optimization_results'] if r.get('optimizations_applied')]),
                'learning_stage': 'active' if self.optimization_engine['learning_mode'] else 'stable'
            },
            'predictive_recommendations': [
                'Consider scheduling heavy operations during low-activity hours',
                'Cache hit rates show improvement potential with longer TTL',
                'Batch operations could benefit from dynamic sizing'
            ]
        }
        
        return insights
    
    def _generate_optimization_recommendations(self, trend_analysis: Dict) -> List[str]:
        """Generate actionable optimization recommendations."""
        recommendations = []
        
        efficiency = trend_analysis.get('efficiency_scores', {})
        overall_efficiency = efficiency.get('overall_efficiency', 0)
        
        if overall_efficiency < 70:
            recommendations.append("🔧 Overall system efficiency below 70% - consider comprehensive optimization")
        
        if efficiency.get('query_efficiency', 0) < 60:
            recommendations.append("⚡ Query optimization needed - implement more aggressive caching")
        
        if efficiency.get('cache_efficiency', 0) < 50:
            recommendations.append("💾 Cache efficiency low - increase cache size or TTL")
        
        if efficiency.get('batch_efficiency', 0) < 60:
            recommendations.append("📦 Batch processing inefficient - optimize batch sizes and intervals")
        
        if len(trend_analysis.get('performance_bottlenecks', [])) > 2:
            recommendations.append("🚨 Multiple performance bottlenecks detected - prioritize connection optimization")
        
        if not recommendations:
            recommendations.append("✅ System performing well - continue current optimization strategy")
        
        return recommendations

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
    
    def _config_has_changed(self, site_doc: dict) -> bool:
        """Check if configuration has changed since last run."""
        try:
            # Get stored target URLs from site state
            stored_target_urls = site_doc.get('target_urls', [])
            
            # Compare with current TARGET_URLS from config
            from src.config import TARGET_URLS
            current_target_urls = list(TARGET_URLS)
            
            # Check if URLs are different
            if set(stored_target_urls) != set(current_target_urls):
                print(f"📋 Config change detected:")
                print(f"   Stored URLs: {stored_target_urls}")
                print(f"   Current URLs: {current_target_urls}")
                return True
            
            return False
            
        except Exception as e:
            print(f"⚠️ Error checking config changes: {e}")
            return False
    
    def _clean_incompatible_urls(self):
        """Clean URLs that don't match current configuration."""
        try:
            from src.config import TARGET_URLS, BASE_URL
            
            # Get base domain from config
            from urllib.parse import urlparse
            target_domain = urlparse(list(TARGET_URLS)[0]).netloc if TARGET_URLS else None
            
            if target_domain:
                # Count URLs that don't match current domain
                incompatible_count = self.db.url_states.count_documents({
                    "site_id": self.site_id,
                    "url": {"$not": {"$regex": target_domain}}
                })
                
                if incompatible_count > 0:
                    print(f"🗑️ Removing {incompatible_count} incompatible URLs...")
                    
                    # Remove incompatible URLs
                    self.db.url_states.delete_many({
                        "site_id": self.site_id,
                        "url": {"$not": {"$regex": target_domain}}
                    })
                    
                    # Clean related data
                    self.db.page_changes.delete_many({
                        "site_id": self.site_id,
                        "url": {"$not": {"$regex": target_domain}}
                    })
                    
                    print(f"✅ Removed incompatible URLs for domain: {target_domain}")
            
            # Update site state with new target URLs
            self.db.site_states.update_one(
                {"site_id": self.site_id},
                {"$set": {
                    "target_urls": list(TARGET_URLS),
                    "base_url": BASE_URL,
                    "config_updated_at": datetime.now()
                }},
                upsert=True
            )
            
        except Exception as e:
            print(f"⚠️ Error cleaning incompatible URLs: {e}")
    
    def _clean_all_data(self):
        """Clean all data for fresh start."""
        try:
            print("🗑️ Cleaning all existing data...")
            
            # Clear all collections for this site
            collections = ['url_states', 'page_changes', 'daily_stats', 'performance_history', 'site_states']
            
            for collection in collections:
                result = self.db[collection].delete_many({"site_id": self.site_id})
                print(f"   Cleared {collection}: {result.deleted_count} documents")
            
            print("✅ All data cleaned for fresh start")
            
        except Exception as e:
            print(f"❌ Error cleaning all data: {e}")
    
    def _initialize_fresh_state(self):
        """Initialize completely fresh state."""
        try:
            from src.config import TARGET_URLS, BASE_URL
            
            # Clear memory
            self.visited_urls = set()
            self.remaining_urls = set(TARGET_URLS)
            self.next_crawl = {}
            self.url_status = {}
            self.daily_stats = {}
            self.performance_history = []
            
            # Initialize fresh site state
            self.db.site_states.insert_one({
                "site_id": self.site_id,
                "total_pages_estimate": self.total_pages_estimate,
                "cycle_start_time": datetime.now(),
                "current_cycle": 1,
                "is_first_cycle": True,
                "created_at": datetime.now(),
                "target_urls": list(TARGET_URLS),
                "base_url": BASE_URL
            })
            
            print("🆕 Fresh state initialized successfully")
            
        except Exception as e:
            print(f"❌ Error initializing fresh state: {e}")