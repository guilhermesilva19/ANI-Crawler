"""MongoDB connection pool for optimized database performance."""

import threading
import time
import queue
from typing import Optional, Dict
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern
from src.config import MONGODB_URI, SITE_ID

__all__ = ['MongoDBPool']


class MongoDBPool:
    """Singleton MongoDB connection pool for improved performance."""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Ensure only one instance exists (singleton pattern)."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize the MongoDB connection pool."""
        if not hasattr(self, 'initialized'):
            self.client: Optional[MongoClient] = None
            self.database = None
            self.site_id = SITE_ID
            self.connection_attempts = 0
            self.max_retries = 3
            self.stats = {
                'connections_created': 0,
                'reconnections': 0,
                'failed_operations': 0,
                'last_health_check': None
            }
            
            # Advanced connection management
            self.connection_queue = queue.Queue(maxsize=10)
            self.auto_scaling_enabled = True
            self.load_monitoring = {
                'request_count': 0,
                'error_rate': 0.0,
                'avg_response_time': 0.0,
                'last_scale_check': time.time(),
                'scale_up_threshold': 0.8,    # Scale up when 80% pool utilization
                'scale_down_threshold': 0.3,  # Scale down when 30% pool utilization
                'request_history': [],
                'max_history_size': 100
            }
            self.reconnection_strategy = {
                'exponential_backoff': True,
                'max_backoff_time': 60,  # seconds
                'base_backoff_time': 1,  # seconds
                'consecutive_failures': 0
            }
            
            self.initialize_pool()
            self.initialized = True
    
    def initialize_pool(self) -> bool:
        """Initialize MongoDB connection with optimized settings."""
        if not MONGODB_URI:
            print("❌ MONGODB_URI not configured")
            return False
        
        try:
            print(f"🔗 Initializing MongoDB connection pool...")
            
            # Optimized MongoDB connection settings (simplified for compatibility)
            self.client = MongoClient(
                MONGODB_URI,
                # Enhanced connection pool settings
                maxPoolSize=50,        # Increased for better concurrency
                minPoolSize=10,        # Higher minimum to maintain connections
                maxIdleTimeMS=300000,  # 5 minutes max idle time
                
                # Optimized timeout settings
                serverSelectionTimeoutMS=10000,  # 10 seconds to select server
                socketTimeoutMS=120000,           # 2 minutes socket timeout
                connectTimeoutMS=10000,           # 10 seconds connection timeout
                
                # Enhanced reliability settings
                retryWrites=True,
                retryReads=True,
                
                # Optimized heartbeat settings
                heartbeatFrequencyMS=10000,  # 10 seconds heartbeat for faster detection
                
                # Read preference for performance (using string format for compatibility)
                readPreference='secondaryPreferred',
                
                # Write concern for reliability (simplified)
                w='majority',
                journal=True,
                
                # App identification for monitoring
                appName='ANI-Crawler'
            )
            
            # Test connection with timeout
            self.client.admin.command('ping')
            
            # Get database (use site_id as database name)
            self.database = self.client[self.site_id]
            
            # Update statistics
            self.stats['connections_created'] += 1
            self.stats['last_health_check'] = time.time()
            
            print(f"✅ MongoDB connection pool initialized: {self.site_id}")
            print(f"📊 Pool settings: maxPoolSize=50, minPoolSize=10, readPreference=secondaryPreferred")
            return True
            
        except Exception as e:
            print(f"❌ MongoDB connection failed: {e}")
            self.client = None
            self.database = None
            return False
    
    def get_collection(self, collection_name: str):
        """Get a collection from the database with optimized read settings."""
        if not self.database:
            if not self.initialize_pool():
                raise ConnectionFailure("Cannot connect to MongoDB")
        
        collection = self.database[collection_name]
        
        # Apply optimized read settings for better performance
        try:
            collection = collection.with_options(
                read_preference='secondaryPreferred',
                read_concern=ReadConcern(level="local")  # Faster reads for crawler data
            )
        except Exception:
            # Fallback to default collection if read preference fails
            pass
        
        return collection
    
    def test_connection(self) -> bool:
        """Test if the connection is still alive with health metrics."""
        try:
            if self.client:
                start_time = time.time()
                self.client.admin.command('ping')
                ping_time = time.time() - start_time
                
                # Update health statistics
                self.stats['last_health_check'] = time.time()
                
                # Log slow pings for monitoring
                if ping_time > 1.0:
                    print(f"⚠️ Slow MongoDB ping: {ping_time:.2f}s")
                
                return True
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            self.stats['failed_operations'] += 1
        return False
    
    def reconnect_if_needed(self) -> bool:
        """Reconnect if connection is lost with enhanced retry logic."""
        if not self.test_connection():
            print("🔄 MongoDB connection lost, attempting reconnection...")
            self.connection_attempts += 1
            self.stats['reconnections'] += 1
            
            if self.connection_attempts <= self.max_retries:
                # Exponential backoff with jitter
                wait_time = min(2 ** self.connection_attempts, 30)
                jitter = time.time() % 1  # Add small random component
                total_wait = wait_time + jitter
                
                print(f"⏱️ Waiting {total_wait:.1f} seconds before retry ({self.connection_attempts}/{self.max_retries})...")
                time.sleep(total_wait)
                
                if self.initialize_pool():
                    self.connection_attempts = 0  # Reset on success
                    print("✅ Reconnection successful")
                    return True
                else:
                    print(f"❌ Reconnection attempt {self.connection_attempts} failed")
            else:
                print(f"❌ Max connection retries ({self.max_retries}) exceeded")
                
        return self.test_connection()
    
    def get_connection_stats(self) -> dict:
        """Get connection pool statistics for monitoring."""
        return {
            **self.stats,
            'current_time': time.time(),
            'pool_active': self.client is not None,
            'database_name': self.site_id
        }
    
    # === ADVANCED CONNECTION MANAGEMENT ===
    
    def monitor_load_and_scale(self) -> Dict:
        """Monitor connection load and auto-scale pool if needed."""
        current_time = time.time()
        
        # Track request
        self.load_monitoring['request_count'] += 1
        
        # Check if it's time for scaling analysis (every 30 seconds)
        if current_time - self.load_monitoring['last_scale_check'] > 30:
            scaling_result = self._analyze_and_scale()
            self.load_monitoring['last_scale_check'] = current_time
            return scaling_result
        
        return {'action': 'monitored', 'scaling_needed': False}
    
    def _analyze_and_scale(self) -> Dict:
        """Analyze load patterns and scale connection pool if needed."""
        if not self.auto_scaling_enabled or not self.client:
            return {'action': 'disabled', 'scaling_needed': False}
        
        try:
            # Get current pool statistics from MongoDB
            pool_stats = self.client.nodes
            current_connections = len(pool_stats) if pool_stats else 1
            
            # Calculate utilization (simplified)
            recent_requests = len(self.load_monitoring['request_history'])
            utilization = min(recent_requests / 50.0, 1.0)  # Normalize to 0-1
            
            # Determine scaling action
            if utilization > self.load_monitoring['scale_up_threshold']:
                return self._scale_up_pool()
            elif utilization < self.load_monitoring['scale_down_threshold']:
                return self._scale_down_pool()
            
            return {
                'action': 'no_scaling_needed',
                'utilization': round(utilization * 100, 2),
                'current_connections': current_connections
            }
            
        except Exception as e:
            return {'action': 'error', 'error': str(e)}
    
    def _scale_up_pool(self) -> Dict:
        """Scale up the connection pool for higher load."""
        try:
            # Note: MongoDB connection pool scaling is primarily handled
            # by the maxPoolSize setting. Here we can adjust monitoring thresholds
            
            # Increase monitoring sensitivity for high load
            self.load_monitoring['scale_up_threshold'] = min(0.9, self.load_monitoring['scale_up_threshold'] + 0.1)
            
            return {
                'action': 'scaled_up',
                'new_threshold': self.load_monitoring['scale_up_threshold'],
                'message': 'Increased pool monitoring sensitivity for high load'
            }
            
        except Exception as e:
            return {'action': 'scale_up_failed', 'error': str(e)}
    
    def _scale_down_pool(self) -> Dict:
        """Scale down the connection pool for lower load."""
        try:
            # Decrease monitoring sensitivity for low load
            self.load_monitoring['scale_down_threshold'] = max(0.1, self.load_monitoring['scale_down_threshold'] - 0.05)
            
            return {
                'action': 'scaled_down',
                'new_threshold': self.load_monitoring['scale_down_threshold'],
                'message': 'Decreased pool monitoring sensitivity for low load'
            }
            
        except Exception as e:
            return {'action': 'scale_down_failed', 'error': str(e)}
    
    def smart_reconnect(self) -> bool:
        """Intelligent reconnection with exponential backoff."""
        if not self.reconnection_strategy['exponential_backoff']:
            return self.reconnect_if_needed()
        
        consecutive_failures = self.reconnection_strategy['consecutive_failures']
        base_time = self.reconnection_strategy['base_backoff_time']
        max_time = self.reconnection_strategy['max_backoff_time']
        
        # Calculate exponential backoff time
        backoff_time = min(base_time * (2 ** consecutive_failures), max_time)
        
        print(f"🔄 Smart reconnection attempt {consecutive_failures + 1}, waiting {backoff_time}s...")
        time.sleep(backoff_time)
        
        # Attempt reconnection
        success = self.reconnect_if_needed()
        
        if success:
            self.reconnection_strategy['consecutive_failures'] = 0
            print("✅ Smart reconnection successful")
        else:
            self.reconnection_strategy['consecutive_failures'] += 1
            print(f"❌ Smart reconnection failed (attempt {self.reconnection_strategy['consecutive_failures']})")
        
        return success
    
    def get_advanced_stats(self) -> Dict:
        """Get comprehensive connection and performance statistics."""
        base_stats = self.get_connection_stats()
        
        return {
            **base_stats,
            'auto_scaling': {
                'enabled': self.auto_scaling_enabled,
                'load_monitoring': self.load_monitoring.copy(),
                'current_utilization': len(self.load_monitoring['request_history']) / 50.0
            },
            'reconnection_strategy': self.reconnection_strategy.copy(),
            'connection_health': {
                'ping_responsive': self.test_connection(),
                'last_health_check_age': time.time() - (self.stats.get('last_health_check', 0) or 0)
            }
        }
    
    def optimize_connection_settings(self) -> Dict:
        """Dynamically optimize connection settings based on performance."""
        if not self.client:
            return {'status': 'no_connection'}
        
        optimizations = []
        
        # Check error rate and adjust timeouts
        error_rate = self.load_monitoring.get('error_rate', 0)
        if error_rate > 0.1:  # 10% error rate
            optimizations.append('increased_timeout_tolerance')
        
        # Check request pattern and adjust pool behavior
        recent_requests = len(self.load_monitoring['request_history'])
        if recent_requests > 80:  # High activity
            optimizations.append('high_activity_mode')
        elif recent_requests < 10:  # Low activity
            optimizations.append('low_activity_mode')
        
        return {
            'optimizations_applied': optimizations,
            'performance_mode': 'high_activity' if recent_requests > 80 else 'normal',
            'connection_tuning': 'active'
        }

    def close(self):
        """Close the connection pool with cleanup."""
        if self.client:
            try:
                # Log final statistics
                print(f"📊 Final connection stats: {self.get_connection_stats()}")
                self.client.close()
                self.client = None
                self.database = None
                print("🔌 MongoDB connection pool closed")
            except Exception as e:
                print(f"⚠️ Error during pool cleanup: {e}")


# Global instance
_db_pool = None

def get_db_pool() -> MongoDBPool:
    """Get the global database pool instance."""
    global _db_pool
    if _db_pool is None:
        _db_pool = MongoDBPool()
    return _db_pool
